package transform.outbox;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

/**
 * Grava os eventos como as aplicações (serializers do Confluent, que registram o schema em
 * {@code <tópico>-value}) e lê o que o SMT publica como os consumidores (deserializers do Confluent).
 */
final class TestSchemas {

	static final List<SchemaProvider> PROVIDERS = List.of(new AvroSchemaProvider(), new ProtobufSchemaProvider(),
		new JsonSchemaProvider());

	static final org.apache.avro.Schema ORDER_V1 = new org.apache.avro.Schema.Parser().parse("""
		{"type": "record", "name": "OrderCreated", "namespace": "com.example.orders", "fields": [
		  {"name": "orderId", "type": "string"},
		  {"name": "status", "type": "string"}
		]}""");

	static final org.apache.avro.Schema ORDER_V2 = new org.apache.avro.Schema.Parser().parse("""
		{"type": "record", "name": "OrderCreated", "namespace": "com.example.orders", "fields": [
		  {"name": "orderId", "type": "string"},
		  {"name": "status", "type": "string"},
		  {"name": "channel", "type": "string", "default": "web"}
		]}""");

	static final ProtobufSchema ORDER_PROTO = new ProtobufSchema("""
		syntax = "proto3";
		package com.example.orders;

		message OrderCreated {
		  string order_id = 1;
		  string status = 2;
		}

		message OrderCancelled {
		  string order_id = 1;
		  string reason = 2;
		}
		""");

	static final JsonSchema ORDER_JSON_SCHEMA = new JsonSchema("""
		{"$schema": "http://json-schema.org/draft-07/schema#", "title": "OrderCreated", "type": "object",
		 "properties": {"orderId": {"type": "string"}, "status": {"type": "string"}},
		 "required": ["orderId", "status"], "additionalProperties": false}""");

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private TestSchemas() {
	}

	static byte[] avroEvent(SchemaRegistryClient registry, String topic, org.apache.avro.Schema schema,
		String orderId) {
		final GenericData.Record event = new GenericData.Record(schema);
		event.put("orderId", orderId);
		event.put("status", "CREATED");
		if (schema.getField("channel") != null) {
			event.put("channel", "app");
		}
		try (KafkaAvroSerializer serializer = new KafkaAvroSerializer(registry, serdeConfig())) {
			return serializer.serialize(topic, event);
		}
	}

	static byte[] protobufEvent(SchemaRegistryClient registry, String topic, String messageName, String orderId) {
		final Descriptor descriptor = ORDER_PROTO.toDescriptor(messageName);
		final DynamicMessage event = DynamicMessage.newBuilder(descriptor)
			.setField(descriptor.findFieldByNumber(1), orderId)
			.setField(descriptor.findFieldByNumber(2), "field-2")
			.build();
		try (KafkaProtobufSerializer<DynamicMessage> serializer = new KafkaProtobufSerializer<>(registry,
			serdeConfig())) {
			return serializer.serialize(topic, event);
		}
	}

	static byte[] jsonSchemaEvent(SchemaRegistryClient registry, String topic, String json) {
		try (KafkaJsonSchemaSerializer<JsonNode> serializer = new KafkaJsonSchemaSerializer<>(registry,
			serdeConfig())) {
			return serializer.serialize(topic, JsonSchemaUtils.envelope(ORDER_JSON_SCHEMA, MAPPER.readTree(json)));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static Object consumeAvro(SchemaRegistryClient registry, String topic, byte[] value) {
		try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registry, serdeConfig())) {
			return deserializer.deserialize(topic, value);
		}
	}

	static DynamicMessage consumeProtobuf(SchemaRegistryClient registry, String topic, byte[] value) {
		try (KafkaProtobufDeserializer<DynamicMessage> deserializer = new KafkaProtobufDeserializer<>(registry,
			serdeConfig())) {
			return deserializer.deserialize(topic, value);
		}
	}

	static Object consumeJsonSchema(SchemaRegistryClient registry, String topic, byte[] value) {
		try (KafkaJsonSchemaDeserializer<Object> deserializer = new KafkaJsonSchemaDeserializer<>(registry,
			serdeConfig())) {
			return deserializer.deserialize(topic, value);
		}
	}

	static String field(DynamicMessage message, int number) {
		return (String) message.getField(message.getDescriptorForType().findFieldByNumber(number));
	}

	private static Map<String, Object> serdeConfig() {
		return Map.of("schema.registry.url", "mock://unused");
	}
}
