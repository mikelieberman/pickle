package pickle.cdb;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import pickle.KV;
import pickle.PickleMap;
import pickle.KV.Type;

import com.strangegizmo.cdb.Cdb;
import com.strangegizmo.cdb.CdbElement;
import com.strangegizmo.cdb.CdbMake;

/**
 * @author Michael Lieberman
 */
public class CdbMap<K, V> extends AbstractMap<K, V> implements PickleMap<K, V> {

	public static enum Mode {CREATE, READ};

	protected Cdb cdb;
	protected CdbMake make;
	protected Mode mode;
	protected String file;

	public CdbMap(String file) throws IOException {
		this(file, Mode.READ);
	}

	public CdbMap(String file, Mode mode) throws IOException {
		this.file = file;
		this.mode = mode;

		if (mode == Mode.CREATE) {
			make = new CdbMake();
			make.start(file);
		}
		else {
			cdb = new Cdb(file);
		}
	}

	@Override
	public void flush() {
		// Nothing to do
	}

	@Override
	public void close() {
		try {
			if (mode == Mode.CREATE) {
				make.finish();
			}
			else {
				cdb.close();
			}

		} catch (IOException e) {
			error(e);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public V get(Object key) {
		if (mode != Mode.READ) {
			modeError();
		}

		byte[] val = cdb.find(KV.toBytes(Type.DATA, key));
		return val != null ? KV.<V>fromBytes(val) : null;
	}

	public Iterable<V> getAll(Object key) {
		Collection<V> values = new ArrayList<V>();

		byte[] k = KV.toBytes(Type.DATA, key);
		cdb.findstart(k);

		byte[] v;
		while ((v = cdb.findnext(k)) != null) {
			values.add(KV.<V>fromBytes(v));
		}

		return values;
	}

	@Override
	public V put(K key, V value) {
		if (mode != Mode.CREATE) {
			modeError();
		}

		try {
			make.add(KV.toBytes(Type.DATA, key),
					KV.toBytes(Type.DATA, value));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// Not really correct but can't do much about it.
		return null;
	}

	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}
	
	public int sizeWithDups() {
		return getSize(true);
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		if (mode != Mode.READ) {
			modeError();
		}

		return new EntrySet();
	}
	
	protected int getSize(boolean withDupKeys) {
		Iterator<Map.Entry<K, V>> i = entrySet().iterator();
		
		K prev = null;
		
		int size = 0;
		while (i.hasNext()) {
			Map.Entry<K, V> cur = i.next();
			
			if (cur.getKey().equals(prev)) {
				continue;
			}
			
			size++;
			
			prev = cur.getKey();
		}
		
		return size;
	}

	protected void error(Exception e) {
		throw new RuntimeException(e);
	}

	protected void modeError() {
		throw new RuntimeException("Incorrect mode! "+mode);
	}

	protected class EntrySet extends AbstractSet<Map.Entry<K, V>> {

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		@Override
		public int size() {
			return getSize(false);
		}

	}

	protected class EntryIterator implements Iterator<Map.Entry<K, V>> {

		protected Enumeration<CdbElement> enumeration;

		public EntryIterator() {
			try {
				enumeration = Cdb.elements(file);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean hasNext() {
			return enumeration.hasMoreElements();
		}

		@Override
		public Map.Entry<K, V> next() {
			return new CdbEntry(enumeration.nextElement());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	protected class CdbEntry implements Map.Entry<K, V> {

		protected K key;
		protected V value;

		public CdbEntry(CdbElement element) {
			this.key = KV.fromBytes(element.getKey());
			this.value = KV.fromBytes(element.getData());
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
			throw new UnsupportedOperationException();
		}

	}

}
