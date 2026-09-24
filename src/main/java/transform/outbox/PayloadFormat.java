package transform.outbox;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

enum PayloadFormat {

	AVRO, PROTOBUF, JSON_SCHEMA, JSON, STRING, BYTES;

	static final String VALID_VALUES = Arrays.stream(values())
		.map(PayloadFormat::configValue)
		.collect(Collectors.joining(", "));

	static Optional<PayloadFormat> of(String value) {
		return Arrays.stream(values())
			.filter(format -> format.configValue().equalsIgnoreCase(value.trim()))
			.findFirst();
	}

	String configValue() {
		return name().toLowerCase(Locale.ROOT);
	}

	boolean usesSchemaRegistry() {
		return this == AVRO || this == PROTOBUF || this == JSON_SCHEMA;
	}
}
