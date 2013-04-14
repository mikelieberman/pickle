package pickle.accumulo;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;

import pickle.PickleMap;
import pickle.Pickler;


public class AccumuloMap<K, V> extends AbstractMap<K, V> implements PickleMap<K, V> {

	protected static final Text EMPTY = new Text();

	protected static final Text METAROW = new Text("!METADATA");
	protected static final Text COUNTCF = new Text("count");

	protected Connector conn;
	protected String table;
	protected boolean autoflush;
	protected Scanner scanner;
	protected BatchWriter writer;

	public AccumuloMap(Connector conn, String table) throws AccumuloException {
		this(conn, table, true);
	}

	public AccumuloMap(Connector conn, String table, boolean autoflush) throws AccumuloException {
		this(conn, table, autoflush, false);
	}

	public AccumuloMap(Connector conn, String table,
			boolean autoflush, boolean create) throws AccumuloException {
		try {
			this.conn = conn;
			this.table = table;
			this.autoflush = autoflush;

			if (create) {
				recreateTable();
			}

			initScannerAndWriter();

			if (create) {
				resetCount();
			}

		} catch (AccumuloSecurityException e) {
			throw new AccumuloException(e);
		} catch (TableExistsException e) {
			throw new AccumuloException(e);
		} catch (TableNotFoundException e) {
			throw new AccumuloException(e);
		}
	}

	protected void recreateTable() throws AccumuloException,
	AccumuloSecurityException, TableNotFoundException, TableExistsException {
		TableOperations ops = conn.tableOperations();

		if (ops.exists(table)) {
			ops.delete(table);
		}

		ops.create(table);

		// Attach the counting iterator to keep a row count.
		IteratorSetting settings = new IteratorSetting(10, SummingCombiner.class);

		IteratorSetting.Column count = new IteratorSetting.Column(COUNTCF);
		Combiner.setColumns(settings, Arrays.asList(new IteratorSetting.Column[]{count}));
		LongCombiner.setEncodingType(settings, LongCombiner.StringEncoder.class);

		ops.attachIterator(table, settings);
	}

	protected void initScannerAndWriter() throws TableNotFoundException {
		this.scanner = conn.createScanner(table, Constants.NO_AUTHS);

		this.writer = conn.createBatchWriter(table, 1000000L, 10L, 2);
		if (autoflush) {
			this.writer = new FlushedBatchWriter(writer);
		}
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public V get(Object key) {
		scanner.setRange(new Range(toRowId(key)));
		Map.Entry<Key, Value> entry = firstEntry(scanner);
		return entry != null ? Pickler.<V>unpickle(entry.getValue().get()) : null;
	}

	@Override
	public V put(K key, V value) {
		try {
			V old = get(key);

			Mutation m = new Mutation(toRowId(key));
			m.put(EMPTY, EMPTY, toValue(value));
			writer.addMutation(m);

			if (old == null) {
				incrementCount();
			}

			return old;

		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			V old = get(key);

			if (old != null) {
				deleteKey(key);
				decrementCount();
			}

			return old;

		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		}
	}


	protected Map.Entry<Key, Value> firstEntry(Scanner scanner) {
		Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
		return i.hasNext() ? i.next() : null;
	}

	protected static Text toRowId(Object key) {
		return new Text(Pickler.pickle(key));
	}

	protected static Value toValue(Object value) {
		return new Value(Pickler.pickle(value));
	}

	protected void deleteKey(Object key) {
		try {
			Mutation m = new Mutation(toRowId(key));
			m.putDelete(EMPTY, EMPTY);
			writer.addMutation(m);

		} catch (AccumuloException e) {
			throw new RuntimeException(e);
		}
	}

	protected void updateCount(long incr, boolean delete) throws AccumuloException {
		Mutation m = new Mutation(METAROW);

		if (delete) {
			m.putDelete(COUNTCF, EMPTY);
		}

		m.put(COUNTCF, EMPTY, new Value(Long.toString(incr).getBytes()));

		writer.addMutation(m);
	}

	protected void incrementCount() throws AccumuloException {
		updateCount(+1, false);
	}

	protected void decrementCount() throws AccumuloException {
		updateCount(-1, false);
	}

	protected void resetCount() throws AccumuloException {
		updateCount(0, true);
	}
	
	
	protected class EntrySet extends AbstractSet<Map.Entry<K, V>> {

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		@Override
		public int size() {
			scanner.setRange(new Range(METAROW));
			scanner.fetchColumnFamily(COUNTCF);
			Map.Entry<Key, Value> entry = firstEntry(scanner);
			scanner.clearColumns();

			return (int) Long.parseLong(new String(entry.getValue().get()));
		}

		@Override
		public void clear() {
			try {
				recreateTable();
				initScannerAndWriter();
				resetCount();

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

	}

	protected class EntryIterator implements Iterator<Map.Entry<K, V>> {

		protected Iterator<Map.Entry<Key, Value>> entries;
		protected Map.Entry<K, V> curEntry;

		public EntryIterator() {
			scanner.setRange(new Range());
			entries = scanner.iterator();
		}

		@Override
		public boolean hasNext() {
			return entries.hasNext();
		}

		@Override
		public Map.Entry<K, V> next() {
			curEntry = new EntryWrapper(entries.next());
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

		public EntryWrapper(Map.Entry<Key, Value> entry) {
			this.key = Pickler.unpickle(entry.getKey().getRow().getBytes());
			this.value = Pickler.unpickle(entry.getValue().get());
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

	@Override
	public void flush() {
		try {
			writer.flush();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException();
		}
	}

	@Override
	public void close() {
		try {
			writer.close();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException();
		}
	}

}
