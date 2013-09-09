package pickle;

import java.util.Map;

/**
 * @author Michael Lieberman
 */
public interface PickleMap<K, V> extends Map<K, V> {

	/**
	 * Flushes any in-memory keys/values to the backing store.
	 */
	public void flush();

	/**
	 * Flushes any in-memory data and closes
	 * the backing store connection.
	 */
	public void close();
	
}
