package transform.outbox;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnEncodingTest {

	private static final byte[] ORDER = "pedido-ção".getBytes(StandardCharsets.UTF_8);

	@Test
	void decodesBase64Text() {
		assertArrayEquals(ORDER, decode(ColumnEncoding.BASE64, Base64.getEncoder().encodeToString(ORDER)));
	}

	@Test
	void decodesBinaryColumnsAsJdbcSourceAndDebeziumDeliverThem() {
		final ByteBuffer slice = ByteBuffer.wrap(("xx" + "pedido-ção").getBytes(StandardCharsets.UTF_8), 2, ORDER.length).slice();

		assertArrayEquals(ORDER, decode(ColumnEncoding.BYTE_ARRAY, ORDER));
		assertArrayEquals(ORDER, decode(ColumnEncoding.BYTE_ARRAY, slice));
		assertEquals(ORDER.length, slice.remaining());
	}

	@Test
	void encodesTextAsUtf8() {
		assertArrayEquals(ORDER, decode(ColumnEncoding.STRING, "pedido-ção"));
		assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), decode(ColumnEncoding.STRING, 42L));
	}

	@Test
	void explainsTheEncodingThatMatchesTheColumnType() {
		final DataException binaryAsText = assertThrows(DataException.class, () -> decode(ColumnEncoding.STRING, ORDER));
		final DataException textAsBinary = assertThrows(DataException.class, () -> decode(ColumnEncoding.BYTE_ARRAY, "text"));

		assertTrue(binaryAsText.getMessage().contains("use byte_array"), binaryAsText.getMessage());
		assertTrue(textAsBinary.getMessage().contains("use string or base64"), textAsBinary.getMessage());
		assertThrows(DataException.class, () -> decode(ColumnEncoding.BASE64, "not base64!"));
	}

	@Test
	void parsesConfigValuesIgnoringCase() {
		assertEquals(Optional.of(ColumnEncoding.BYTE_ARRAY), ColumnEncoding.of(" Byte_Array "));
		assertEquals(Optional.empty(), ColumnEncoding.of("hex"));
	}

	private static byte[] decode(ColumnEncoding encoding, Object value) {
		return encoding.decode(value, "message_payload", "table.column.payload.encode");
	}
}
