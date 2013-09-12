package pickle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Michael Lieberman
 */
public class Pickler {

	private static final Kryo KRYO = new Kryo();

	public static <T> byte[] pickle(T o) {
		return pickle(o, false);
	}

	public static <T> byte[] pickle(T o, boolean compressed) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output output = new Output(compressed ? new GZIPOutputStream(baos) : baos);
			KRYO.writeClassAndObject(output, o);
			output.close();
			return baos.toByteArray();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T unpickle(byte[] data) {
		return unpickle(data, false);
	}

	@SuppressWarnings("unchecked")
	public static <T> T unpickle(byte[] data, boolean compressed) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			Input input = new Input(compressed ? new GZIPInputStream(bais) : bais);
			T obj = (T) KRYO.readClassAndObject(input);
			input.close();
			return obj;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> String pickleToString(T o) {
		return pickleToString(o, false);
	}

	public static <T> String pickleToString(T o, boolean compressed) {
		return new String(Base64.encodeBase64(pickle(o), compressed));
	}

	public static <T> T unpickleFromString(String data) {
		return unpickleFromString(data, false);
	}

	public static <T> T unpickleFromString(String data, boolean compressed) {
		return unpickle(Base64.decodeBase64(data), compressed);
	}

}
