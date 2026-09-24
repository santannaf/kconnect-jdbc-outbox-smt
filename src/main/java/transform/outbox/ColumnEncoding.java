package transform.outbox;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.DataException;

enum ColumnEncoding {

	BASE64, BYTE_ARRAY, STRING;

	static final String VALID_VALUES = Arrays.stream(values())
		.map(ColumnEncoding::configValue)
		.collect(Collectors.joining(", "));

	static Optional<ColumnEncoding> of(String value) {
		return Arrays.stream(values())
			.filter(encoding -> encoding.configValue().equalsIgnoreCase(value.trim()))
			.findFirst();
	}

	String configValue() {
		return name().toLowerCase(Locale.ROOT);
	}

	byte[] decode(Object value, String column, String encodingConfig) {
		return switch (this) {
			case BASE64 -> base64(text(value, column, encodingConfig), column);
			case BYTE_ARRAY -> bytes(value, column, encodingConfig);
			case STRING -> text(value, column, encodingConfig).getBytes(StandardCharsets.UTF_8);
		};
	}

	private String text(Object value, String column, String encodingConfig) {
		if (value instanceof byte[] || value instanceof ByteBuffer) {
			throw new DataException(String.format("Column %s is binary, but %s=%s expects text; use %s",
				column, encodingConfig, configValue(), BYTE_ARRAY.configValue()));
		}
		return value.toString();
	}

	// O Kafka Connect aceita byte[] ou ByteBuffer em campos BYTES: o JDBC Source entrega byte[],
	// o Debezium entrega ByteBuffer.
	private static byte[] bytes(Object value, String column, String encodingConfig) {
		if (value instanceof byte[] bytes) {
			return bytes;
		}
		if (value instanceof ByteBuffer buffer) {
			final byte[] bytes = new byte[buffer.remaining()];
			buffer.duplicate().get(bytes);
			return bytes;
		}
		throw new DataException(String.format("Column %s is not binary (%s), but %s=%s; use %s or %s",
			column, value.getClass().getSimpleName(), encodingConfig, BYTE_ARRAY.configValue(),
			STRING.configValue(), BASE64.configValue()));
	}

	private static byte[] base64(String value, String column) {
		try {
			return Base64.getDecoder().decode(value);
		} catch (IllegalArgumentException e) {
			throw new DataException("Column " + column + " is not valid base64", e);
		}
	}
}
