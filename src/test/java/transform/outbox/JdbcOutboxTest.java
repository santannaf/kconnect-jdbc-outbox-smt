package transform.outbox;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static transform.outbox.TestSchemas.ORDER_V1;
import static transform.outbox.TestSchemas.ORDER_V2;
import static transform.outbox.TestSchemas.avroEvent;
import static transform.outbox.TestSchemas.consumeAvro;
import static transform.outbox.TestSchemas.jsonSchemaEvent;
import static transform.outbox.TestSchemas.protobufEvent;

class JdbcOutboxTest {

	private static final String TOPIC = "outbox.orders";

	private String registryScope;

	private SchemaRegistryClient registry;

	private final JdbcOutbox<SourceRecord> transform = new JdbcOutbox<>();

	@BeforeEach
	void setUp() {
		registryScope = "jdbc-outbox-" + UUID.randomUUID();
		registry = MockSchemaRegistry.getClientForScope(registryScope, TestSchemas.PROVIDERS);
	}

	@AfterEach
	void tearDown() {
		transform.close();
		MockSchemaRegistry.dropScope(registryScope);
	}

	@Test
	void publishesEachRowAsWrittenInTheFormatOfItsFormatColumn() {
		transform.configure(config(Map.of("table.column.format", "message_format",
			"table.column.payload.encode", "byte_array")));
		final Map<String, byte[]> payloadsByFormat = new LinkedHashMap<>();
		payloadsByFormat.put("avro", avroEvent(registry, "orders.avro", ORDER_V1, "order-1"));
		payloadsByFormat.put("protobuf", protobufEvent(registry, "orders.protobuf", "OrderCreated", "order-2"));
		payloadsByFormat.put("json_schema", jsonSchemaEvent(registry, "orders.json_schema",
			"{\"orderId\": \"order-3\", \"status\": \"CREATED\"}"));
		payloadsByFormat.put("json", "{\"orderId\": \"order-4\"}".getBytes(StandardCharsets.UTF_8));
		payloadsByFormat.put("string", "pedido order-5 confirmado".getBytes(StandardCharsets.UTF_8));
		payloadsByFormat.put("bytes", new byte[] {(byte) 0xCA, (byte) 0xFE});

		payloadsByFormat.forEach((format, payload) -> {
			final SourceRecord published = transform.apply(
				Row.outbox("orders." + format, payload).with("message_format", Schema.OPTIONAL_STRING_SCHEMA, format).record());

			assertEquals("orders." + format, published.topic());
			assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, published.valueSchema());
			assertArrayEquals(payload, (byte[]) published.value(), format);
		});
	}

	@Test
	void usesPayloadFormatWhenTheFormatColumnIsEmpty() {
		transform.configure(config(Map.of("payload.format", "json", "table.column.format", "message_format",
			"table.column.payload.encode", "string")));

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, "{\"orderId\": \"order-6\"}")
			.with("message_format", Schema.OPTIONAL_STRING_SCHEMA, null).record());
		assertArrayEquals("{\"orderId\": \"order-6\"}".getBytes(StandardCharsets.UTF_8), (byte[]) published.value());

		assertThrows(DataException.class, () -> transform.apply(Row.outbox(TOPIC, "not json")
			.with("message_format", Schema.OPTIONAL_STRING_SCHEMA, " ").record()));
	}

	@Test
	void rejectsUnknownFormatInTheFormatColumn() {
		transform.configure(config(Map.of("table.column.format", "message_format", "table.column.payload.encode", "string")));

		final DataException error = assertThrows(DataException.class, () -> transform.apply(
			Row.outbox(TOPIC, "{}").with("message_format", Schema.OPTIONAL_STRING_SCHEMA, "xml").record()));

		assertTrue(error.getMessage().contains("unknown payload format 'xml'"), error.getMessage());
	}

	@Test
	void convertsAvroToTheLatestTopicSchema() throws Exception {
		transform.configure(config(Map.of("payload.format", "avro")));
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-7");
		registry.register(TOPIC + "-value", new AvroSchema(ORDER_V2));

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, Base64.getEncoder().encodeToString(payload)).record());

		final GenericRecord event = (GenericRecord) consumeAvro(registry, TOPIC, (byte[]) published.value());
		assertEquals("order-7", event.get("orderId").toString());
		assertEquals("web", event.get("channel").toString());
	}

	@Test
	void keepsTheAvroWriterSchemaWhenLatestVersionIsDisabled() throws Exception {
		transform.configure(config(Map.of("payload.format", "avro", "avro.use.latest.version", "false",
			"table.column.payload.encode", "byte_array")));
		final byte[] payload = avroEvent(registry, TOPIC, ORDER_V1, "order-8");
		registry.register(TOPIC + "-value", new AvroSchema(ORDER_V2));

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, ByteBuffer.wrap(payload)).record());

		assertArrayEquals(payload, (byte[]) published.value());
	}

	@Test
	void publishesStringByDefaultWithoutSchemaRegistry() {
		final Map<String, Object> config = config(Map.of("table.column.payload.encode", "string"));
		config.remove("schema.registry.url");
		transform.configure(config);

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, "pedido order-16 criado").record());

		assertArrayEquals("pedido order-16 criado".getBytes(StandardCharsets.UTF_8), (byte[]) published.value());
		assertEquals("string", transform.config().configKeys().get("payload.format").defaultValue);
	}

	@Test
	void publishesTombstoneWhenPayloadIsNull() {
		transform.configure(config(Map.of("payload.format", "json")));

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, null).record());

		assertEquals(TOPIC, published.topic());
		assertEquals("order-42", published.key());
		assertNull(published.value());
	}

	@Test
	void routesToTheRoutingTopicInsteadOfTheTopicColumn() {
		transform.configure(config(Map.of("payload.format", "string", "table.column.payload.encode", "string",
			"routing.topic", "orders.routed")));

		assertEquals("orders.routed", transform.apply(Row.outbox(TOPIC, "order-9").record()).topic());
	}

	@Test
	void rejectsRowWithoutTopic() {
		transform.configure(config(Map.of("payload.format", "string", "table.column.payload.encode", "string")));

		assertThrows(DataException.class, () -> transform.apply(Row.outbox(null, "order-10").record()));
	}

	@Test
	void decodesKeyColumnAndGeneratesARandomKeyWhenItIsNull() {
		transform.configure(config(Map.of("payload.format", "string", "table.column.payload.encode", "string",
			"table.column.key.encode", "base64")));

		final SourceRecord encodedKey = transform.apply(Row.outbox(TOPIC, "order-11")
			.with("message_key", Schema.OPTIONAL_STRING_SCHEMA, Base64.getEncoder().encodeToString("order-11".getBytes())).record());
		final SourceRecord nullKey = transform.apply(Row.outbox(TOPIC, "order-12")
			.with("message_key", Schema.OPTIONAL_STRING_SCHEMA, null).record());

		assertEquals("order-11", encodedKey.key());
		assertEquals(Schema.STRING_SCHEMA, encodedKey.keySchema());
		assertNotNull(UUID.fromString((String) nullKey.key()));
	}

	@ParameterizedTest
	@MethodSource("partitionColumns")
	void usesNumericPartitionColumn(Schema schema, Object partition, Integer expected) {
		transform.configure(config(Map.of("payload.format", "string", "table.column.payload.encode", "string",
			"table.column.partition", "partition_number")));

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, "order-13")
			.with("partition_number", schema, partition).record());

		assertEquals(expected, published.kafkaPartition());
	}

	static List<Object[]> partitionColumns() {
		return List.of(new Object[] {Schema.OPTIONAL_INT32_SCHEMA, 2, 2},
			new Object[] {Decimal.builder(0).optional().build(), new BigDecimal("3"), 3},
			new Object[] {Schema.OPTIONAL_INT32_SCHEMA, null, null});
	}

	@Test
	void copiesConfiguredColumnsToHeadersWithTheirTypes() {
		transform.configure(config(Map.of("payload.format", "string", "table.column.payload.encode", "string",
			"table.column.headers", "event_type,created_at")));
		final Date createdAt = new Date(1_700_000_000_000L);

		final SourceRecord published = transform.apply(Row.outbox(TOPIC, "order-14")
			.with("event_type", Schema.STRING_SCHEMA, "OrderCreated")
			.with("created_at", Timestamp.SCHEMA, createdAt).record());

		assertEquals("OrderCreated", published.headers().lastWithName("event_type").value());
		final Header timestamp = published.headers().lastWithName("created_at");
		assertEquals(createdAt, timestamp.value());
		assertEquals(Timestamp.SCHEMA, timestamp.schema());
	}

	@ParameterizedTest
	@MethodSource("invalidConfigs")
	void rejectsInvalidConfig(Map<String, String> overrides) {
		final Map<String, Object> config = config(overrides);
		config.values().removeIf("<remove>"::equals);

		assertThrows(ConfigException.class, () -> transform.configure(config));
	}

	static List<Map<String, String>> invalidConfigs() {
		return List.of(Map.of("payload.format", "xml"),
			Map.of("table.column.payload.encode", "hex"),
			Map.of("table.column.key.encode", "hex"),
			Map.of("avro.use.latest.version", "yes"),
			Map.of("payload.format", "avro", "schema.registry.url", "<remove>"),
			Map.of("payload.format", "protobuf", "schema.registry.url", "<remove>"),
			Map.of("table.column.topic", "<remove>"));
	}

	@Test
	void rejectsSchemaRegistryFormatRowWhenNoSchemaRegistryIsConfigured() {
		final Map<String, Object> config = config(Map.of("payload.format", "json", "table.column.format", "message_format",
			"table.column.payload.encode", "byte_array"));
		config.remove("schema.registry.url");
		transform.configure(config);
		final byte[] avro = avroEvent(registry, TOPIC, ORDER_V1, "order-15");

		final DataException error = assertThrows(DataException.class, () -> transform.apply(
			Row.outbox(TOPIC, avro).with("message_format", Schema.OPTIONAL_STRING_SCHEMA, "avro").record()));

		assertTrue(error.getMessage().contains("schema.registry.url"), error.getMessage());
	}

	@Test
	void declaresEveryOptionInItsConfigDef() {
		assertEquals(JdbcOutboxFields.ALL_FIELDS.size(), transform.config().names().size());
		assertTrue(transform.config().names().containsAll(List.of("payload.format", "table.column.format",
			"avro.use.latest.version", "json.fail.invalid.schema", "routing.topic", "table.column.partition")));
	}

	private Map<String, Object> config(Map<String, String> overrides) {
		final Map<String, Object> config = new HashMap<>();
		config.put("schema.registry.url", "mock://" + registryScope);
		config.put("table.column.key", "message_key");
		config.put("table.column.payload", "message_payload");
		config.put("table.column.topic", "message_topic");
		config.putAll(overrides);
		return config;
	}

	private static final class Row {

		private final Map<String, Schema> schemas = new LinkedHashMap<>();

		private final Map<String, Object> values = new HashMap<>();

		static Row outbox(String topic, Object payload) {
			final Schema payloadSchema = payload instanceof String || payload == null
				? Schema.OPTIONAL_STRING_SCHEMA : Schema.OPTIONAL_BYTES_SCHEMA;
			return new Row().with("message_key", Schema.OPTIONAL_STRING_SCHEMA, "order-42")
				.with("message_topic", Schema.OPTIONAL_STRING_SCHEMA, topic)
				.with("message_payload", payloadSchema, payload);
		}

		Row with(String column, Schema schema, Object value) {
			schemas.put(column, schema);
			values.put(column, value);
			return this;
		}

		SourceRecord record() {
			final SchemaBuilder builder = SchemaBuilder.struct();
			schemas.forEach(builder::field);
			final Schema schema = builder.build();
			final Struct row = new Struct(schema);
			values.forEach(row::put);
			return new SourceRecord(Map.of(), Map.of(), "outbox_event", null, schema, row);
		}
	}
}
