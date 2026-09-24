package transform.outbox;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import transform.TransformField;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Publica a linha da tabela de outbox como a mensagem que ela carrega: tópico, key, partição e headers
 * vêm de colunas, e o value é o payload gravado pela aplicação, validado no formato da linha (Avro pode
 * ser reescrito com o schema mais recente do tópico). O value sai como bytes (schema {@code BYTES}),
 * então o connector usa um converter de passagem ({@code ByteArrayConverter} ou o
 * {@code BinaryDataConverter} do Debezium) para qualquer formato.
 */
public class JdbcOutbox<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOutbox.class);

	private static final int SCHEMA_CACHE_CAPACITY = 1000;

	private String keyColumn;

	private ColumnEncoding keyEncoding;

	private String payloadColumn;

	private ColumnEncoding payloadEncoding;

	private PayloadFormat defaultFormat;

	private String formatColumn;

	private String topicColumn;

	private String routingTopic;

	private List<String> headerColumns;

	private String partitionColumn;

	private SchemaRegistryClient schemaRegistryClient;

	private SchemaRegistryPayloads schemaRegistryPayloads;

	@Override
	public String version() {
		return Optional.ofNullable(JdbcOutbox.class.getPackage().getImplementationVersion()).orElse("unknown");
	}

	@Override
	public void configure(Map<String, ?> configMap) {
		final JdbcOutboxFields fields = new JdbcOutboxFields(configMap);
		keyColumn = fields.getMessageKeyField();
		keyEncoding = fields.getKeyEncoding();
		payloadColumn = fields.getMessagePayloadField();
		payloadEncoding = fields.getPayloadEncoding();
		defaultFormat = fields.getPayloadFormat();
		formatColumn = fields.getPayloadFormatColumn();
		topicColumn = fields.getMessageTopicField();
		routingTopic = fields.getRoutingTopic();
		headerColumns = fields.getColumnsHeaders();
		partitionColumn = fields.getPartition();

		if (topicColumn == null && routingTopic == null) {
			throw new ConfigException(String.format("Either %s or %s must be filled",
				JdbcOutboxFields.FIELD_ROUTING_TOPIC.getName(), JdbcOutboxFields.FIELD_TOPIC_COLUMN.getName()));
		}

		final List<String> schemaRegistryUrls = fields.getSchemaRegistryUrls();
		if (schemaRegistryUrls.isEmpty()) {
			if (defaultFormat.usesSchemaRegistry()) {
				throw new ConfigException(String.format("%s=%s requires %s",
					JdbcOutboxFields.FIELD_PAYLOAD_FORMAT.getName(), defaultFormat.configValue(),
					JdbcOutboxFields.FIELD_SCHEMA_REGISTRY.getName()));
			}
		} else {
			final Map<String, Object> clientConfig = new HashMap<>(configMap);
			clientConfig.put(SchemaRegistryClientConfig.LATEST_CACHE_TTL_CONFIG,
				String.valueOf(TimeUnit.MINUTES.toSeconds(fields.getSchemaCacheTtlField())));
			schemaRegistryClient = SchemaRegistryClientFactory.newClient(schemaRegistryUrls, SCHEMA_CACHE_CAPACITY,
				List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
				clientConfig, Map.of());
			schemaRegistryPayloads = new SchemaRegistryPayloads(schemaRegistryClient, fields.getAvroUseLatestVersion(),
				fields.getJsonFailInvalidSchema());
		}

		LOGGER.info("Outbox configured: payload column {} ({}, format {}{}), key column {}, topic {}",
			payloadColumn, payloadEncoding.configValue(), defaultFormat.configValue(),
			formatColumn == null ? "" : " or column " + formatColumn, keyColumn,
			routingTopic == null ? "column " + topicColumn : routingTopic);
	}

	@Override
	public R apply(R recordToApply) {
		if (recordToApply.value() == null) {
			LOGGER.error("recordToApply null value, messageKey={}", recordToApply.key());
			return null;
		}

		final Struct row = requireStruct(recordToApply.value(), "Read Outbox Event");
		final String topic = topic(row);
		final PayloadFormat format = payloadFormat(row);
		final Object storedPayload = row.get(payloadColumn);
		final byte[] value = storedPayload == null ? null
			: recordValue(format, topic, payloadEncoding.decode(storedPayload, payloadColumn,
				JdbcOutboxFields.FIELD_PAYLOAD_ENCODE.getName()));

		final R newRecord = recordToApply.newRecord(topic, //
			partition(row), //
			Schema.STRING_SCHEMA, //
			key(row), //
			Schema.OPTIONAL_BYTES_SCHEMA, //
			value, //
			System.currentTimeMillis(), //
			null
		);
		applyHeaders(newRecord, row);

		LOGGER.debug("Outbox event routed to topic {}: key={}, format={}, {}", topic, newRecord.key(),
			format.configValue(), value == null ? "tombstone" : value.length + " bytes");
		return newRecord;
	}

	private byte[] recordValue(PayloadFormat format, String topic, byte[] payload) {
		return switch (format) {
			case AVRO -> schemaRegistry(format).avro(topic, payload);
			case PROTOBUF -> schemaRegistry(format).protobuf(payload);
			case JSON_SCHEMA -> schemaRegistry(format).jsonSchema(payload);
			case JSON -> TextPayloads.json(payload);
			case STRING -> TextPayloads.utf8(payload);
			case BYTES -> payload;
		};
	}

	private SchemaRegistryPayloads schemaRegistry(PayloadFormat format) {
		if (schemaRegistryPayloads == null) {
			throw new DataException(String.format("Payload format %s requires %s",
				format.configValue(), JdbcOutboxFields.FIELD_SCHEMA_REGISTRY.getName()));
		}
		return schemaRegistryPayloads;
	}

	private PayloadFormat payloadFormat(Struct row) {
		final Object value = formatColumn == null ? null : row.get(formatColumn);
		if (value == null || value.toString().isBlank()) {
			return defaultFormat;
		}
		return PayloadFormat.of(value.toString())
			.orElseThrow(() -> new DataException(String.format("Column %s has an unknown payload format '%s' (valid: %s)",
				formatColumn, value, PayloadFormat.VALID_VALUES)));
	}

	private String topic(Struct row) {
		if (routingTopic != null) {
			return routingTopic;
		}
		final Object topic = row.get(topicColumn);
		if (topic == null || topic.toString().isBlank()) {
			throw new DataException("Column " + topicColumn + " has no topic");
		}
		return topic.toString();
	}

	private String key(Struct row) {
		final Object key = row.get(keyColumn);

		// Quando a key é nula, temos problemas no kafka connector,
		// então, melhor aplicarmos um valor qualquer randômico.
		if (key == null) {
			return UUID.randomUUID().toString();
		}
		return new String(keyEncoding.decode(key, keyColumn, JdbcOutboxFields.FIELD_KEY_ENCODE.getName()),
			StandardCharsets.UTF_8);
	}

	private Integer partition(Struct row) {
		if (partitionColumn == null) {
			return null;
		}
		final Object partition = row.get(partitionColumn);
		if (partition == null) {
			return null;
		}
		if (!(partition instanceof Number number)) {
			throw new DataException("Column " + partitionColumn + " is not numeric: " + partition.getClass().getSimpleName());
		}
		return number.intValue();
	}

	private void applyHeaders(R newRecord, Struct row) {
		if (headerColumns == null || headerColumns.isEmpty()) {
			return;
		}

		for (final String headerKey : headerColumns) {
			final Field field = row.schema().field(headerKey);
			newRecord.headers().add(headerKey, new SchemaAndValue(field.schema(), row.get(headerKey)));
		}
	}

	@Override
	public ConfigDef config() {
		final ConfigDef config = new ConfigDef();
		for (TransformField field : JdbcOutboxFields.ALL_FIELDS) {
			field.define(config);
		}
		return config;
	}

	@Override
	public void close() {
		if (schemaRegistryClient == null) {
			return;
		}
		try {
			schemaRegistryClient.close();
		} catch (IOException e) {
			LOGGER.warn("Could not close the Schema Registry client", e);
		}
	}
}
