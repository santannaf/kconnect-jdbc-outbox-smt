package transform.outbox;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.everit.json.schema.ValidationException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

/**
 * Payloads no formato wire dos serializers do Confluent (magic byte 0, id do schema em 4 bytes e o
 * conteúdo). O payload só segue para o tópico depois de lido com o schema indicado pelo id, o mesmo
 * que o deserializer do consumidor vai usar.
 */
final class SchemaRegistryPayloads {

	private static final byte MAGIC_BYTE = 0x0;

	private static final int WIRE_HEADER_LENGTH = 1 + Integer.BYTES;

	private static final String VALUE_SUBJECT_SUFFIX = "-value";

	private static final int HTTP_NOT_FOUND = 404;

	private final SchemaRegistryClient client;

	private final boolean avroUseLatestVersion;

	private final boolean jsonFailInvalidSchema;

	SchemaRegistryPayloads(SchemaRegistryClient client, boolean avroUseLatestVersion, boolean jsonFailInvalidSchema) {
		this.client = client;
		this.avroUseLatestVersion = avroUseLatestVersion;
		this.jsonFailInvalidSchema = jsonFailInvalidSchema;
	}

	byte[] avro(String topic, byte[] payload) {
		final int writerId = schemaId(payload, PayloadFormat.AVRO);
		final AvroSchema writer = schema(writerId, AvroSchema.class, PayloadFormat.AVRO);
		if (!avroUseLatestVersion) {
			readAvro(payload, writerId, writer, writer);
			return payload;
		}
		final String subject = topic + VALUE_SUBJECT_SUFFIX;
		final SchemaMetadata latest = fetch(() -> client.getLatestSchemaMetadata(subject),
			"latest schema of subject " + subject);
		if (latest.getId() == writerId || isNewerThanLatest(subject, writer, latest)) {
			readAvro(payload, writerId, writer, writer);
			return payload;
		}
		final AvroSchema latestSchema = schema(latest.getId(), AvroSchema.class, PayloadFormat.AVRO);
		return writeAvro(latest.getId(), latestSchema, readAvro(payload, writerId, writer, latestSchema));
	}

	byte[] protobuf(byte[] payload) {
		final int schemaId = schemaId(payload, PayloadFormat.PROTOBUF);
		final ProtobufSchema schema = schema(schemaId, ProtobufSchema.class, PayloadFormat.PROTOBUF);
		final ByteBuffer message = ByteBuffer.wrap(payload, WIRE_HEADER_LENGTH, payload.length - WIRE_HEADER_LENGTH);
		try {
			final Descriptor descriptor = schema.toDescriptor(schema.toMessageName(MessageIndexes.readFrom(message)));
			DynamicMessage.parseFrom(descriptor, CodedInputStream.newInstance(message));
		} catch (IOException | RuntimeException e) {
			throw new DataException("protobuf payload is not a valid message of schema id " + schemaId, e);
		}
		return payload;
	}

	byte[] jsonSchema(byte[] payload) {
		final int schemaId = schemaId(payload, PayloadFormat.JSON_SCHEMA);
		final JsonSchema schema = schema(schemaId, JsonSchema.class, PayloadFormat.JSON_SCHEMA);
		final JsonNode json = TextPayloads.parseJson(payload, WIRE_HEADER_LENGTH, PayloadFormat.JSON_SCHEMA);
		if (jsonFailInvalidSchema) {
			try {
				schema.validate(json);
			} catch (JsonProcessingException | ValidationException e) {
				throw new DataException("json_schema payload does not match schema id " + schemaId, e);
			}
		}
		return payload;
	}

	// O latest vem de um cache com TTL e pode estar atrás da versão que a aplicação acabou de registrar:
	// converter para ele rebaixaria o evento e descartaria os campos novos.
	private boolean isNewerThanLatest(String subject, AvroSchema writer, SchemaMetadata latest) {
		try {
			return client.getVersion(subject, writer) > latest.getVersion();
		} catch (RestClientException e) {
			// Schema de quem gravou fora do subject do tópico (outra estratégia de nome de subject):
			// segue a regra geral e converte para o latest do tópico.
			if (e.getStatus() == HTTP_NOT_FOUND) {
				return false;
			}
			throw registryFailure(e, "version of schema " + writer.name() + " in subject " + subject);
		} catch (IOException e) {
			throw registryUnavailable(e, "version of schema " + writer.name() + " in subject " + subject);
		}
	}

	private static Object readAvro(byte[] payload, int writerId, AvroSchema writer, AvroSchema reader) {
		final BinaryDecoder decoder = DecoderFactory.get()
			.binaryDecoder(payload, WIRE_HEADER_LENGTH, payload.length - WIRE_HEADER_LENGTH, null);
		try {
			final Object datum = new GenericDatumReader<>(writer.rawSchema(), reader.rawSchema()).read(null, decoder);
			if (decoder.isEnd()) {
				return datum;
			}
		} catch (IOException | RuntimeException e) {
			throw new DataException("avro payload cannot be read with schema id " + writerId, e);
		}
		throw new DataException("avro payload has bytes left after reading it with schema id " + writerId
			+ ": it was not written with that schema");
	}

	private static byte[] writeAvro(int schemaId, AvroSchema schema, Object datum) {
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(MAGIC_BYTE);
		out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(schemaId).array());
		final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
		try {
			new GenericDatumWriter<>(schema.rawSchema()).write(datum, encoder);
			encoder.flush();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return out.toByteArray();
	}

	private static int schemaId(byte[] payload, PayloadFormat format) {
		if (payload.length < WIRE_HEADER_LENGTH || payload[0] != MAGIC_BYTE) {
			throw new DataException(format.configValue() + " payload is not in the Schema Registry wire format "
				+ "(magic byte 0 and a 4-byte schema id) written by the Confluent serializers");
		}
		return ByteBuffer.wrap(payload, 1, Integer.BYTES).getInt();
	}

	private <S extends ParsedSchema> S schema(int schemaId, Class<S> type, PayloadFormat format) {
		final ParsedSchema schema = fetch(() -> client.getSchemaById(schemaId), "schema id " + schemaId);
		if (!type.isInstance(schema)) {
			throw new DataException(String.format("Schema id %d is a %s schema, but the payload format is %s",
				schemaId, schema.schemaType(), format.configValue()));
		}
		return type.cast(schema);
	}

	private static <T> T fetch(RegistryCall<T> call, String description) {
		try {
			return call.execute();
		} catch (RestClientException e) {
			throw registryFailure(e, description);
		} catch (IOException e) {
			throw registryUnavailable(e, description);
		}
	}

	// Falhas transitórias viram RetriableException: com errors.retry.timeout no connector, o Kafka
	// Connect repete o registro em vez de parar a task.
	private static ConnectException registryFailure(RestClientException e, String description) {
		final String message = "Schema Registry could not return the " + description;
		if (e.getStatus() >= 500 || e.getStatus() == 408 || e.getStatus() == 429) {
			return new RetriableException(message, e);
		}
		return new DataException(message, e);
	}

	private static RetriableException registryUnavailable(IOException e, String description) {
		return new RetriableException("Schema Registry unavailable while reading the " + description, e);
	}

	@FunctionalInterface
	private interface RegistryCall<T> {

		T execute() throws IOException, RestClientException;
	}
}
