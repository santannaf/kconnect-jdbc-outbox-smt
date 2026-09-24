package transform.outbox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

final class TextPayloads {

	private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

	private TextPayloads() {
	}

	static byte[] json(byte[] payload) {
		parseJson(payload, 0, PayloadFormat.JSON);
		return payload;
	}

	static JsonNode parseJson(byte[] payload, int offset, PayloadFormat format) {
		final JsonNode json;
		try {
			json = JSON.readTree(payload, offset, payload.length - offset);
		} catch (IOException e) {
			throw new DataException(format.configValue() + " payload is not valid JSON", e);
		}
		if (json == null || json.isMissingNode()) {
			throw new DataException(format.configValue() + " payload has no JSON document");
		}
		return json;
	}

	static byte[] utf8(byte[] payload) {
		try {
			StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(payload));
		} catch (CharacterCodingException e) {
			throw new DataException("string payload is not valid UTF-8", e);
		}
		return payload;
	}
}
