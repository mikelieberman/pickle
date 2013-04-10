package org.mikelieberman.pickle.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class AccumuloMap<K, V> implements NavigableMap<K, V> {

	private static final Text METAROW = new Text("!METADATA");
	private static final Text ROWCOUNTCF = new Text("rowcount");
	private static final Text EMPTY = new Text();

	private Connector connector;
	private String tableName;
	private Scanner scanner;
	private BatchWriter writer;
	private boolean autoflush;
	private Comparator<K> comparator;

	public AccumuloMap(Connector connector, String tablename, boolean clear, boolean autoflush, Comparator<K> comparator)
					throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
		this.connector = connector;
		this.tableName = tablename;
		this.autoflush = autoflush;
		this.comparator = comparator;

		TableOperations ops = connector.tableOperations();

		if (clear && ops.exists(tableName)) {
			ops.delete(tableName);
		}

		boolean created = false;

		if (!ops.exists(tableName)) {
			ops.create(tableName);
			created = true;
		}
		
		initScannerAndWriter();

		if (created) {
			initMetadata();
		}
	}
	
	private void initScannerAndWriter() throws TableNotFoundException {
		scanner = connector.createScanner(tableName, Constants.NO_AUTHS);

		writer = connector.createBatchWriter(tableName, 1000000L, 300000, 2);
		if (autoflush) {
			writer = new FlushedBatchWriter(writer);
		}
	}

	private void initMetadata() throws MutationsRejectedException, IOException {
		// Maybe initialize size of map here.
	}

	public void flush() throws MutationsRejectedException {
		writer.flush();
	}

	public void close() throws MutationsRejectedException {
		writer.close();
	}

	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public K firstKey() {
		try {
			Map.Entry<Key, Value> entry = Utils.getFirstEntry(scanner);
			return (K) (entry != null ? Utils.decode(entry.getKey()) : null);

		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<K> keySet() {
		Set<K> keys = new TreeSet<K>(comparator);

		for (Map.Entry<K, V> entry : entrySet()) {
			keys.add(entry.getKey());
		}

		return keys;
	}

	@Override
	public K lastKey() {
		Map.Entry<K, V> entry = lastEntry();
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		try {
			connector.tableOperations().delete(tableName);
			connector.tableOperations().create(tableName);

			initScannerAndWriter();
			
		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		} catch (AccumuloSecurityException e) {
			throw new RuntimeException(e);
		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		} catch (TableExistsException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return getVal(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V get(Object key) {
		return getVal(key);
	}

	@Override
	public boolean isEmpty() {
		return size() != 0;
	}

	@Override
	public V put(K key, V value) {
		try {
			V old = getVal(key);

			Mutation m = new Mutation(Utils.toText(key));
			m.put(EMPTY, EMPTY, Utils.toValue(value));
			writer.addMutation(m);

			return old;

		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (MutationsRejectedException e) {
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
	public V remove(Object key) {
		try {
			V old = getVal(key);

			if (old != null) {
				Mutation m = new Mutation(Utils.toText(key));
				m.putDelete((Text) null, (Text) null);
				writer.addMutation(m);
			}

			return old;

		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int size() {
		int size = 0;

		Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
		while (i.hasNext()) {
			size++;
		}

		return size;
	}

	private V getVal(Object key) {
		try {
			V val = null;

			scanner.setRange(Utils.toRange(key));
			Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
			if (i.hasNext()) {
				Map.Entry<Key, Value> entry = i.next();
				val = (V) Utils.decode(entry.getValue());
			}

			return val;

		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
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

	@Override
	public Map.Entry<K, V> lowerEntry(K key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public K lowerKey(K key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map.Entry<K, V> floorEntry(K key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public K floorKey(K key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map.Entry<K, V> ceilingEntry(K key) {
		try {
			Range range = new Range(Utils.toText(key), true, null, true);
			scanner.setRange(range);
			Map.Entry<Key, Value> entry = Utils.getFirstEntry(scanner);
			return entry != null ? new EntryWrapper(this, entry) : null;

		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
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
			Range range = new Range(Utils.toText(key), false, null, true);
			scanner.setRange(range);
			Map.Entry<Key, Value> entry = Utils.getFirstEntry(scanner);
			return entry != null ? new EntryWrapper(this, entry) : null;

		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
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
			Map.Entry<Key, Value> entry = Utils.getFirstEntry(scanner);
			return entry != null ? new EntryWrapper(this, entry) : null;

		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map.Entry<K, V> lastEntry() {
		try {
			Text key = connector.tableOperations().getMaxRow(tableName, Constants.NO_AUTHS,
					null, true, null, true);
			scanner.setRange(Utils.toRange(key));
			return new EntryWrapper(this, Utils.getFirstEntry(scanner));

		} catch (TableNotFoundException e) {
			throw new RuntimeException(e);
		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		} catch (AccumuloSecurityException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map.Entry<K, V> pollFirstEntry() {
		Map.Entry<K, V> first = firstEntry();
		remove(first.getKey());
		return first;
	}

	@Override
	public Map.Entry<K, V> pollLastEntry() {
		Map.Entry<K, V> last = lastEntry();
		remove(last.getKey());
		return last;
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


	private class EntryWrapper implements Map.Entry<K, V> {

		private AccumuloMap<K, V> parent;
		private K key;
		private V val;

		public EntryWrapper(AccumuloMap<K, V> parent, Map.Entry<Key, Value> entry) throws IOException, ClassNotFoundException {
			this(parent, (K) Utils.decode(entry.getKey()),
					(V) Utils.decode(entry.getValue()));
		}

		public EntryWrapper(AccumuloMap<K, V> parent, K key, V val) {
			this.parent = parent;
			this.key = key;
			this.val = val;
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
			parent.put(key, value);
			this.val = value;
			return val;
		}

	}

}
