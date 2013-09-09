package pickle;

import java.util.NavigableMap;

/**
 * @author Michael Lieberman
 */
public interface NavigablePickleMap<K, V> extends NavigableMap<K, V> {

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
