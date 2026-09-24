package transform.outbox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static transform.outbox.TestSchemas.ORDER_V1;
import static transform.outbox.TestSchemas.ORDER_V2;
import static transform.outbox.TestSchemas.avroEvent;
import static transform.outbox.TestSchemas.consumeAvro;
import static transform.outbox.TestSchemas.consumeJsonSchema;
import static transform.outbox.TestSchemas.consumeProtobuf;
import static transform.outbox.TestSchemas.field;
import static transform.outbox.TestSchemas.jsonSchemaEvent;
import static transform.outbox.TestSchemas.protobufEvent;

class SchemaRegistryPayloadsTest {

	private static final String TOPIC = "outbox.orders";

	private static final String SUBJECT = TOPIC + "-value";

	private final StaleLatestRegistry registry = new StaleLatestRegistry();

	@Test
	void keepsAvroPayloadWrittenWithTheLatestSchema() {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-1");

		assertArrayEquals(payload, payloads(true).avro(TOPIC, payload));
	}

	@Test
	void convertsAvroPayloadWrittenWithAnOlderSchemaToTheLatest() throws Exception {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-2");
		final int latestId = registry.register(SUBJECT, new AvroSchema(ORDER_V2));

		final byte[] published = payloads(true).avro(TOPIC, payload);

		assertEquals(latestId, schemaId(published));
		final GenericRecord event = (GenericRecord) consumeAvro(registry, TOPIC, published);
		assertEquals("order-2", event.get("orderId").toString());
		assertEquals("CREATED", event.get("status").toString());
		assertEquals("web", event.get("channel").toString());
	}

	@Test
	void neverDowngradesAvroPayloadWrittenWithASchemaNewerThanTheCachedLatest() throws Exception {
		registry.register(SUBJECT, new AvroSchema(ORDER_V1));
		registry.freezeLatest(SUBJECT);
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V2, "order-3");

		final byte[] published = payloads(true).avro(TOPIC, payload);

		assertArrayEquals(payload, published);
		assertEquals("app", ((GenericRecord) consumeAvro(registry, TOPIC, published)).get("channel").toString());
	}

	@Test
	void keepsTheWriterSchemaWhenLatestVersionIsDisabled() throws Exception {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-4");
		registry.register(SUBJECT, new AvroSchema(ORDER_V2));

		assertArrayEquals(payload, payloads(false).avro(TOPIC, payload));
	}

	@Test
	void rejectsAvroPayloadThatDoesNotMatchItsSchemaId() throws Exception {
		final int v1 = registry.register(SUBJECT, new AvroSchema(ORDER_V1));
		final byte[] writtenWithV2 = avroEvent(registry, "another.topic", ORDER_V2, "order-5");

		final DataException error = assertThrows(DataException.class,
			() -> payloads(false).avro(TOPIC, withSchemaId(writtenWithV2, v1)));

		assertTrue(error.getMessage().contains("bytes left"), error.getMessage());
	}

	@Test
	void rejectsTruncatedAvroPayload() {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-6");

		assertThrows(DataException.class, () -> payloads(true).avro(TOPIC, Arrays.copyOf(payload, payload.length - 3)));
	}

	@ParameterizedTest
	@ValueSource(strings = {"OrderCreated", "OrderCancelled"})
	void acceptsProtobufPayloadOfAnyMessageInTheSchema(String messageName) {
		final byte[] payload = protobufEvent(registry, TOPIC, messageName, "order-7");

		final byte[] published = payloads(true).protobuf(payload);

		assertArrayEquals(payload, published);
		final DynamicMessage event = consumeProtobuf(registry, TOPIC, published);
		assertEquals(messageName, event.getDescriptorForType().getName());
		assertEquals("order-7", field(event, 1));
	}

	@Test
	void rejectsTruncatedProtobufPayload() {
		final byte[] payload = protobufEvent(registry, TOPIC, "OrderCreated", "order-8");

		assertThrows(DataException.class, () -> payloads(true).protobuf(Arrays.copyOf(payload, payload.length - 3)));
	}

	@Test
	void rejectsProtobufPayloadPointingToAMessageOutsideTheSchema() {
		final byte[] payload = protobufEvent(registry, TOPIC, "OrderCreated", "order-9");
		// Índices de mensagem [5] (contagem e índice em varint zigzag) no lugar do [0] (byte único 0).
		final byte[] unknownMessage = ByteBuffer.allocate(payload.length + 1)
			.put(payload, 0, 5)
			.put(new byte[] {2, 10})
			.put(payload, 6, payload.length - 6)
			.array();

		assertThrows(DataException.class, () -> payloads(true).protobuf(unknownMessage));
	}

	@Test
	void acceptsJsonSchemaPayload() {
		final byte[] payload = jsonSchemaEvent(registry, TOPIC, "{\"orderId\": \"order-10\", \"status\": \"CREATED\"}");

		final byte[] published = payloads(true).jsonSchema(payload);

		assertArrayEquals(payload, published);
		assertTrue(consumeJsonSchema(registry, TOPIC, published).toString().contains("order-10"));
	}

	@Test
	void rejectsJsonSchemaPayloadWithMalformedJson() {
		final byte[] payload = jsonSchemaEvent(registry, TOPIC, "{\"orderId\": \"order-11\", \"status\": \"CREATED\"}");

		assertThrows(DataException.class, () -> payloads(true).jsonSchema(Arrays.copyOf(payload, payload.length - 1)));
	}

	@Test
	void validatesJsonSchemaPayloadAgainstItsSchemaOnlyWhenEnabled() {
		final byte[] missingStatus = jsonSchemaEvent(registry, TOPIC, "{\"orderId\": \"order-12\"}");

		assertArrayEquals(missingStatus, new SchemaRegistryPayloads(registry, true, false).jsonSchema(missingStatus));
		assertThrows(DataException.class, () -> new SchemaRegistryPayloads(registry, true, true).jsonSchema(missingStatus));
	}

	@ParameterizedTest
	@EnumSource(value = PayloadFormat.class, names = {"AVRO", "PROTOBUF", "JSON_SCHEMA"})
	void rejectsPayloadOutsideTheWireFormat(PayloadFormat format) {
		final byte[] json = "{\"orderId\": \"order-13\"}".getBytes(StandardCharsets.UTF_8);

		final DataException error = assertThrows(DataException.class, () -> publish(format, json));

		assertTrue(error.getMessage().contains("wire format"), error.getMessage());
	}

	@Test
	void rejectsPayloadWhoseSchemaIdBelongsToAnotherFormat() {
		final byte[] protobuf = protobufEvent(registry, TOPIC, "OrderCreated", "order-14");

		final DataException error = assertThrows(DataException.class, () -> payloads(true).avro(TOPIC, protobuf));

		assertTrue(error.getMessage().contains("PROTOBUF"), error.getMessage());
	}

	@Test
	void rejectsPayloadWithUnknownSchemaId() {
		final byte[] payload = withSchemaId(avroEvent(registry, TOPIC, ORDER_V1, "order-15"), 4242);

		assertThrows(DataException.class, () -> payloads(false).avro(TOPIC, payload));
	}

	@Test
	void schemaRegistryUnavailabilityIsRetriable() {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-16");
		final MockSchemaRegistryClient unavailable = new MockSchemaRegistryClient(TestSchemas.PROVIDERS) {
			@Override
			public ParsedSchema getSchemaById(int id) throws IOException {
				throw new IOException("Connection refused");
			}
		};

		assertThrows(RetriableException.class,
			() -> new SchemaRegistryPayloads(unavailable, true, false).avro(TOPIC, payload));
	}

	@ParameterizedTest
	@ValueSource(ints = {500, 503, 429, 408})
	void schemaRegistryServerErrorsAreRetriable(int status) {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-17");

		assertThrows(RetriableException.class,
			() -> new SchemaRegistryPayloads(failingWith(status), true, false).avro(TOPIC, payload));
	}

	@Test
	void schemaRegistryAuthorizationErrorsAreNotRetriable() {
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-18");

		assertThrows(DataException.class,
			() -> new SchemaRegistryPayloads(failingWith(401), true, false).avro(TOPIC, payload));
	}

	private SchemaRegistryPayloads payloads(boolean avroUseLatestVersion) {
		return new SchemaRegistryPayloads(registry, avroUseLatestVersion, false);
	}

	private byte[] publish(PayloadFormat format, byte[] payload) {
		return switch (format) {
			case AVRO -> payloads(true).avro(TOPIC, payload);
			case PROTOBUF -> payloads(true).protobuf(payload);
			case JSON_SCHEMA -> payloads(true).jsonSchema(payload);
			default -> throw new IllegalArgumentException(format.name());
		};
	}

	private static MockSchemaRegistryClient failingWith(int status) {
		return new MockSchemaRegistryClient(TestSchemas.PROVIDERS) {
			@Override
			public ParsedSchema getSchemaById(int id) throws RestClientException {
				throw new RestClientException("Schema Registry error", status, status * 100);
			}
		};
	}

	private static int schemaId(byte[] payload) {
		return ByteBuffer.wrap(payload, 1, Integer.BYTES).getInt();
	}

	private static byte[] withSchemaId(byte[] payload, int schemaId) {
		final byte[] copy = payload.clone();
		ByteBuffer.wrap(copy, 1, Integer.BYTES).putInt(schemaId);
		return copy;
	}

	/**
	 * Simula o cache de latest do cliente real parado numa versão anterior à que a aplicação acabou
	 * de registrar.
	 */
	private static final class StaleLatestRegistry extends MockSchemaRegistryClient {

		private final Map<String, SchemaMetadata> frozenLatest = new HashMap<>();

		StaleLatestRegistry() {
			super(TestSchemas.PROVIDERS);
		}

		void freezeLatest(String subject) throws IOException, RestClientException {
			frozenLatest.put(subject, super.getLatestSchemaMetadata(subject));
		}

		@Override
		public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
			final SchemaMetadata frozen = frozenLatest.get(subject);
			return frozen != null ? frozen : super.getLatestSchemaMetadata(subject);
		}
	}
}
