package transform.outbox;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TextPayloadsTest {

	@ParameterizedTest
	@ValueSource(strings = {"{\"orderId\": \"order-1\", \"itens\": [1, 2.50]}", "[]", "\"texto\"", "42", " {\"nome\": \"João\"} "})
	void acceptsAnyJsonDocument(String json) {
		final byte[] payload = json.getBytes(StandardCharsets.UTF_8);

		assertArrayEquals(payload, TextPayloads.json(payload));
	}

	@ParameterizedTest
	@ValueSource(strings = {"", "  ", "{\"orderId\": ", "{} {}", "{}x", "order-1", "{'orderId': 1}"})
	void rejectsAnythingButOneJsonDocument(String text) {
		assertThrows(DataException.class, () -> TextPayloads.json(text.getBytes(StandardCharsets.UTF_8)));
	}

	@Test
	void acceptsUtf8Text() {
		final byte[] payload = "pedido confirmado às 10h ✓".getBytes(StandardCharsets.UTF_8);

		assertArrayEquals(payload, TextPayloads.utf8(payload));
	}

	@Test
	void rejectsBytesThatAreNotUtf8() {
		assertThrows(DataException.class, () -> TextPayloads.utf8(new byte[] {(byte) 0xC3, (byte) 0x28}));
	}
}
