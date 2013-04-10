package org.mikelieberman.pickle.jdbm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

public class JDBMMap<K, V> implements NavigableMap<K, V> {

	private static final String NAME = JDBMMap.class.getSimpleName();
	private static final long DEFCACHESIZE = 10000000L;

	private RecordManager manager;
	private BTree tree;
	private Comparator<? super K> comparator;

	public JDBMMap(String file, Comparator<? super K> comparator) throws IOException {
		this(file, comparator, false);
	}

	public JDBMMap(String file, Comparator<? super K> comparator, boolean clear) throws IOException {
		this(file, comparator, clear, DEFCACHESIZE);
	}

	public JDBMMap(String file, Comparator<? super K> comparator,
			boolean clear, long cacheSize) throws IOException {
		Properties props = new Properties();
		props.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS, "true");
		props.setProperty(RecordManagerOptions.CACHE_SIZE, Long.toString(cacheSize));

		manager = RecordManagerFactory.createRecordManager(file);

		long id = manager.getNamedObject(NAME);
		if (clear && id != 0) {
			manager.delete(id);
			id = 0;
		}

		if (id != 0) {
			tree = BTree.load(manager, id);
		}
		else {
			tree = BTree.createInstance(manager, comparator);
			manager.setNamedObject(NAME, tree.getRecid());
		}

		this.comparator = comparator;
	}

	public void flush() throws IOException {
		manager.commit();
	}

	public void close() throws IOException {
		flush();
		manager.close();
	}

	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}

	@Override
	public K firstKey() {
		try {
			Tuple tuple = new Tuple();
			tree.browse().getNext(tuple);
			return (K) tuple.getKey();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public K lastKey() {
		try {
			Tuple tuple = new Tuple();
			tree.browse(null).getPrevious(tuple);
			return (K) tuple.getKey();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<K> keySet() {
		try {
			Set<K> keys = new TreeSet<K>(comparator);
			Tuple tuple = new Tuple();
			for (TupleBrowser browser = tree.browse(); browser.getNext(tuple); ) {
				keys.add((K) tuple.getKey());
			}
			// TODO Auto-generated method stub
			return keys;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Collection<V> values() {
		List<V> vals = new ArrayList<V>();

		for (K key : keySet()) {
			vals.add(get(key));
		}

		return vals;
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		return tree.size();
	}

	@Override
	public boolean isEmpty() {
		return tree.size() > 0;
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			return tree.find(key) != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsValue(Object value) {
		try {
			Tuple tuple = new Tuple();

			for (TupleBrowser browser = tree.browse(); browser.getNext(tuple); ) {
				if (tuple.getValue().equals(value)) {
					return true;
				}
			}

			return false;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(Object key) {
		try {
			return (V) tree.find(key);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V put(K key, V value) {
		try {
			return (V) tree.insert(key, value, true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			return (V) tree.remove(key);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void clear() {
		try {
			manager.delete(tree.getRecid());
			tree = BTree.createInstance(manager, comparator);
			manager.setNamedObject(NAME, tree.getRecid());

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map.Entry<K, V> lowerEntry(K key) {
		try {
			Tuple tuple = new Tuple();
			boolean found = tree.browse(key).getPrevious(tuple);
			return found ? new EntryWrapper(this, tuple) : null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public K lowerKey(K key) {
		Map.Entry<K, V> entry = lowerEntry(key);
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Map.Entry<K, V> floorEntry(K key) {
		try {
			Tuple tuple = new Tuple();
			TupleBrowser browser = tree.browse(key);
			boolean found = browser.getNext(tuple);
			if (found && tuple.getKey().equals(key)) {
				return new EntryWrapper(this, tuple);
			}
			else {
				found = browser.getPrevious(tuple);
				return found ? new EntryWrapper(this, tuple) : null;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public K floorKey(K key) {
		Map.Entry<K, V> entry = floorEntry(key);
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Map.Entry<K, V> ceilingEntry(K key) {
		try {
			Tuple tuple = tree.findGreaterOrEqual(key);
			return tuple != null ? new EntryWrapper(this, tuple) : null;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public K ceilingKey(K key) {
		Map.Entry<K, V> entry = ceilingEntry(key);
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Map.Entry<K, V> higherEntry(K key) {
		try {
			TupleBrowser browser = tree.browse(key);
			Tuple tuple = new Tuple();

			boolean found = browser.getNext(tuple);

			if (!found) {
				return null;
			}

			else if (!tuple.getKey().equals(key)) {
				return new EntryWrapper(this, tuple);
			}

			// Go past this one cause we want the next.
			else {
				found = browser.getNext(tuple);
				return found ? new EntryWrapper(this, tuple) : null;
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public K higherKey(K key) {
		Map.Entry<K, V> entry = higherEntry(key);
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Map.Entry<K, V> firstEntry() {
		try {
			Tuple tuple = new Tuple();
			boolean found = tree.browse().getNext(tuple);
			return found ? new EntryWrapper(this, tuple) : null;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map.Entry<K, V> lastEntry() {
		try {
			Tuple tuple = new Tuple();
			boolean found = tree.browse(null).getPrevious(tuple);
			return found ? new EntryWrapper(this, tuple) : null;

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map.Entry<K, V> pollFirstEntry() {
		Map.Entry<K, V> entry = firstEntry();

		if (entry != null) {
			remove(entry.getKey());
		}

		return entry;
	}

	@Override
	public Map.Entry<K, V> pollLastEntry() {
		Map.Entry<K, V> entry = lastEntry();

		if (entry != null) {
			remove(entry.getKey());
		}

		return entry;
	}

	@Override
	public NavigableMap<K, V> descendingMap() {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
			boolean toInclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		throw new UnsupportedOperationException();
	}


	protected class EntryWrapper implements Map.Entry<K, V> {

		private Map<K, V> parent;
		private K key;
		private V val;

		public EntryWrapper(Map<K, V> parent, Tuple t) {
			this.parent = parent;

			if (t != null) {
				this.key = (K) t.getKey();
				this.val = (V) t.getValue();
			}
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return val;
		}

		@Override
		public V setValue(V value) {
			V old = val;
			val = value;
			parent.put(key, value);
			return old;
		}

	}

}
