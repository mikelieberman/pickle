package pickle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Michael Lieberman
 */
public class Pickler {

	private static final Kryo KRYO = new Kryo();

	public static <T> byte[] pickle(T o) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		KRYO.writeClassAndObject(output, o);
		output.close();
		return baos.toByteArray();
	}

	@SuppressWarnings("unchecked")
	public static <T> T unpickle(byte[] data) {
		Input input = new Input(new ByteArrayInputStream(data));
		T obj = (T) KRYO.readClassAndObject(input);
		input.close();
		return obj;
	}

}
