package transform.outbox;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import transform.TransformField;

final class JdbcOutboxFields {

	static final TransformField FIELD_SCHEMA_REGISTRY = new TransformField(1, //
			"outbox.schema", //
			"schema.registry.url", //
			"Schema Registry URLs", //
			"Comma-separated Schema Registry URLs. Required by the avro, protobuf and json_schema payload formats", //
			ConfigDef.Type.LIST, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_SCHEMA_CACHE_TTL = new TransformField(2, //
			"outbox.schema", //
			"schema.cache.ttl", //
			"schema cache ttl (in minutes)", //
			"How long, in minutes, the latest schema of each topic stays cached (avro.use.latest.version)", //
			ConfigDef.Type.INT, //
			ConfigDef.Importance.LOW, //
			ConfigDef.Width.SHORT, //
			60);

	static final TransformField FIELD_AVRO_USE_LATEST_VERSION = new TransformField(3, //
			"outbox.schema", //
			"avro.use.latest.version", //
			"publish Avro with the latest schema", //
			"Publish avro payloads with the latest schema of the <topic>-value subject, converting events written "
					+ "with older versions; events written with a newer version are never downgraded. "
					+ "false publishes each payload with the schema it was written with", //
			ConfigDef.Type.BOOLEAN, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.SHORT, //
			Boolean.TRUE);

	static final TransformField FIELD_JSON_FAIL_INVALID_SCHEMA = new TransformField(4, //
			"outbox.schema", //
			"json.fail.invalid.schema", //
			"reject payloads that do not match the JSON Schema", //
			"Reject json_schema payloads that do not match their JSON Schema (the Confluent serializers and "
					+ "deserializers do not validate by default)", //
			ConfigDef.Type.BOOLEAN, //
			ConfigDef.Importance.LOW, //
			ConfigDef.Width.SHORT, //
			Boolean.FALSE);

	static final TransformField FIELD_KEY_COLUMN = new TransformField(1, //
			"outbox.table", //
			"table.column.key", //
			"message key column", //
			"The column which contains the message key within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_KEY_ENCODE = new TransformField(2, //
			"outbox.table", //
			"table.column.key.encode", //
			"message key encode", //
			"How the key column is stored: string (default), base64 or byte_array", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			"string");

	static final TransformField FIELD_PAYLOAD_COLUMN = new TransformField(3, //
			"outbox.table", //
			"table.column.payload", //
			"message payload column", //
			"The column which contains the message payload within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_PAYLOAD_ENCODE = new TransformField(4, //
			"outbox.table", //
			"table.column.payload.encode", //
			"message payload encode", //
			"How the payload column is stored: base64 (text, default), byte_array (binary column) or string "
					+ "(text column published as UTF-8)", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.MEDIUM, //
			"base64");

	static final TransformField FIELD_PAYLOAD_FORMAT = new TransformField(5, //
			"outbox.table", //
			"payload.format", //
			"message payload format", //
			"Format of the payload, kept on the topic: avro, protobuf, json_schema (Schema Registry wire format), "
					+ "json, string or bytes (published without validation). Default is string", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.HIGH, //
			ConfigDef.Width.SHORT, //
			"string");

	static final TransformField FIELD_FORMAT_COLUMN = new TransformField(6, //
			"outbox.table", //
			"table.column.format", //
			"message payload format column", //
			"The column which contains the payload format of each row (same values as payload.format); "
					+ "rows with an empty value use payload.format", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_TOPIC_COLUMN = new TransformField(7, //
			"outbox.table", //
			"table.column.topic", //
			"message topic column", //
			"The column which contains the message topic within the outbox table", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_HEADERS_COLUMNS = new TransformField(8, //
			"outbox.table", //
			"table.column.headers", //
			"columns headers mapping", //
			"All columns that should be add to a event header", //
			ConfigDef.Type.LIST, //
			ConfigDef.Importance.LOW, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final TransformField FIELD_PARTITION_COLUMN = new TransformField(9, //
		"outbox.table", //
		"table.column.partition", //
		"columns partition mapping", //
		"The column which contains the partition number for topic within the outbox table", //
		ConfigDef.Type.STRING, //
		ConfigDef.Importance.LOW, //
		ConfigDef.Width.SHORT, //
		null);

	static final TransformField FIELD_ROUTING_TOPIC = new TransformField(1, //
			"outbox", //
			"routing.topic", //
			"routing topic", //
			"The routing topic for all messages (overrides the table column topic)", //
			ConfigDef.Type.STRING, //
			ConfigDef.Importance.MEDIUM, //
			ConfigDef.Width.MEDIUM, //
			null);

	static final List<TransformField> ALL_FIELDS = List.of(FIELD_SCHEMA_REGISTRY, //
			FIELD_SCHEMA_CACHE_TTL, //
			FIELD_AVRO_USE_LATEST_VERSION, //
			FIELD_JSON_FAIL_INVALID_SCHEMA, //
			FIELD_KEY_COLUMN, //
			FIELD_KEY_ENCODE, //
			FIELD_PAYLOAD_COLUMN, //
			FIELD_PAYLOAD_ENCODE, //
			FIELD_PAYLOAD_FORMAT, //
			FIELD_FORMAT_COLUMN, //
			FIELD_TOPIC_COLUMN, //
			FIELD_HEADERS_COLUMNS, //
			FIELD_PARTITION_COLUMN, //
			FIELD_ROUTING_TOPIC);

	private final Map<String, ?> transformConfigMap;

	JdbcOutboxFields(Map<String, ?> transformConfigMap) {
		this.transformConfigMap = transformConfigMap;
	}

	String getMessageKeyField() {
		return FIELD_KEY_COLUMN.getReqString(transformConfigMap);
	}

	ColumnEncoding getKeyEncoding() {
		return encoding(FIELD_KEY_ENCODE);
	}

	String getMessagePayloadField() {
		return FIELD_PAYLOAD_COLUMN.getReqString(transformConfigMap);
	}

	ColumnEncoding getPayloadEncoding() {
		return encoding(FIELD_PAYLOAD_ENCODE);
	}

	PayloadFormat getPayloadFormat() {
		final String value = FIELD_PAYLOAD_FORMAT.getReqString(transformConfigMap);
		return PayloadFormat.of(value)
			.orElseThrow(() -> new ConfigException(FIELD_PAYLOAD_FORMAT.getName(), value,
				"valid values: " + PayloadFormat.VALID_VALUES));
	}

	String getPayloadFormatColumn() {
		return FIELD_FORMAT_COLUMN.getString(transformConfigMap);
	}

	String getMessageTopicField() {
		return FIELD_TOPIC_COLUMN.getString(transformConfigMap);
	}

	List<String> getSchemaRegistryUrls() {
		final String urls = FIELD_SCHEMA_REGISTRY.getString(transformConfigMap);
		if (urls == null) {
			return List.of();
		}
		return Arrays.stream(urls.split(","))
			.map(String::trim)
			.filter(url -> !url.isEmpty())
			.toList();
	}

	int getSchemaCacheTtlField() {
		return FIELD_SCHEMA_CACHE_TTL.getReqInteger(transformConfigMap);
	}

	boolean getAvroUseLatestVersion() {
		return FIELD_AVRO_USE_LATEST_VERSION.getBoolean(transformConfigMap);
	}

	boolean getJsonFailInvalidSchema() {
		return FIELD_JSON_FAIL_INVALID_SCHEMA.getBoolean(transformConfigMap);
	}

	String getRoutingTopic() {
		return FIELD_ROUTING_TOPIC.getString(transformConfigMap);
	}

	List<String> getColumnsHeaders() {
		return FIELD_HEADERS_COLUMNS.getList(transformConfigMap);
	}

	String getPartition() {
		return FIELD_PARTITION_COLUMN.getString(transformConfigMap);
	}

	private ColumnEncoding encoding(TransformField field) {
		final String value = field.getReqString(transformConfigMap);
		return ColumnEncoding.of(value)
			.orElseThrow(() -> new ConfigException(field.getName(), value, "valid values: " + ColumnEncoding.VALID_VALUES));
	}
}
