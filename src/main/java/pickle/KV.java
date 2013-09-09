package pickle;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Michael Lieberman
 */
public final class KV {
	
	private KV() {
		
	}
	
	public static enum Type {DATA, META};

	public static <T> byte[] toBytes(Type type, T obj) {
		byte[] pickled = Pickler.pickle(obj);
		ByteBuffer buffer = ByteBuffer.allocate(1 + pickled.length);
		buffer.put((byte) type.ordinal());
		buffer.put(pickled);
		return buffer.array();
	}

	public static <T> T fromBytes(byte[] bytes) {
		// Skip the type byte at the beginning.
		return Pickler.unpickle(Arrays.copyOfRange(bytes, 1, bytes.length));
	}
	
}
