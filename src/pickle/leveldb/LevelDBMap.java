package pickle.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import pickle.KV;
import pickle.PickleMap;
import pickle.KV.Type;

/**
 * @author Michael Lieberman
 */
public class LevelDBMap<K, V> extends AbstractMap<K, V> implements PickleMap<K, V> {

	protected static final String SIZESTR = "size";
	protected static final byte[] SIZE = KV.toBytes(Type.META, SIZESTR);

	protected File dbDir;
	protected DB db;

	public LevelDBMap(String dbDir) throws IOException {
		this(dbDir, false);
	}

	public LevelDBMap(String dbDir, boolean clear) throws IOException {
		this.dbDir = new File(dbDir);
		reopen(clear);
	}

	protected void reopen(boolean clear) throws IOException {
		close();

		if (clear && dbDir.exists()) {
			FileUtils.deleteDirectory(dbDir);
		}

		Options options = new Options();
		options.createIfMissing(true);
		options.cacheSize(1000000L);
		options.writeBufferSize(1000000);
		this.db = JniDBFactory.factory.open(dbDir, options);

		// Initialize size.
		if (db.get(SIZE) == null) {
			setSize(0);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public V get(Object key) {
		byte[] val = db.get(KV.toBytes(Type.DATA, key));
		return val != null ? KV.<V>fromBytes(val) : null;
	}

	@Override
	public V put(K key, V value) {
		return put(Type.DATA, key, value);
	}

	public V put(Type keyType, K key, V value) {
		try {
			V old = get(key);

			WriteBatch batch = db.createWriteBatch();
			batch.put(KV.toBytes(keyType, key), KV.toBytes(Type.DATA, value));

			// Update size.
			if (old == null) {
				setSize(getSize() + 1, batch);
			}

			db.write(batch);
			batch.close();

			return old;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			V old = get(key);

			if (old != null) {
				WriteBatch batch = db.createWriteBatch();

				delete(key, batch);
				setSize(getSize() - 1, batch);

				db.write(batch);
				batch.close();
			}

			return old;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		try {
			reopen(true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush() {
		// Nothing to do.
	}

	@Override
	public void close() {
		try {
			if (db != null) {
				db.close();
				db = null;
			}

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}


	protected int getSize() {
		return KV.fromBytes(db.get(SIZE));
	}

	protected void setSize(int size) {
		setSize(size, null);
	}

	protected void setSize(int size, WriteBatch batch) {
		byte[] val = KV.toBytes(Type.DATA, size);

		if (batch != null) {
			batch.put(SIZE, val);
		}
		else {
			db.put(SIZE, val);
		}
	}

	protected void deleteKey(Object key) {
		delete(key, null);
	}

	protected void delete(Object key, WriteBatch batch) {
		byte[] bytes = KV.toBytes(Type.DATA, key);
		if (batch != null) {
			batch.delete(bytes);
		}
		else {
			db.delete(bytes);
		}
	}


	protected class EntrySet extends AbstractSet<Map.Entry<K, V>> {

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		@Override
		public int size() {
			return getSize();
		}

	}


	protected class EntryIterator implements Iterator<Map.Entry<K, V>> {

		protected DBIterator iterator;
		protected Map.Entry<K, V> curEntry;

		public EntryIterator() {
			iterator = db.iterator();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Map.Entry<K, V> next() {
			curEntry = new EntryWrapper(iterator.next());
			return curEntry;
		}

		@Override
		public void remove() {
			deleteKey(curEntry.getKey());
		}

	}

	protected class EntryWrapper implements Map.Entry<K, V> {

		protected K key;
		protected V value;

		public EntryWrapper(Map.Entry<byte[], byte[]> entry) {
			this.key = KV.fromBytes(entry.getKey());
			this.value = KV.fromBytes(entry.getValue());
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V old = put(key, value);
			this.value = value;
			return old;
		}

	}

}
