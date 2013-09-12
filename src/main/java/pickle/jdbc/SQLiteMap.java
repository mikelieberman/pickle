package pickle.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import pickle.PickleMap;
import pickle.Pickler;

/**
 * @author Michael Lieberman
 */
public class SQLiteMap<K, V> extends AbstractMap<K, V> implements PickleMap<K, V> {

	private static final String KEY_COLUMN = "pickle_key";
	private static final String VALUE_COLUMN = "pickle_value";

	private static final String KEY_INDEX = "pickle_keyIndex";
	private static final String VALUE_INDEX = "pickle_valueIndex";

	private String tableName;

	private Connection conn;
	private PreparedStatement stmtCount;
	private PreparedStatement stmtEntries;
	private PreparedStatement stmtSelectKey;
	private PreparedStatement stmtSelectValue;
	private PreparedStatement stmtInsertKey;
	private PreparedStatement stmtDeleteKey;

	public SQLiteMap(String dbFile, String tableName) {
		this(dbFile, tableName, true);
	}

	public SQLiteMap(String dbFile, String tableName, boolean autoCommit) {
		try {
			Class.forName("org.sqlite.JDBC");
			this.conn = DriverManager.getConnection("jdbc:sqlite:"+dbFile);
			this.conn.setAutoCommit(autoCommit);

			this.tableName = tableName;

			createTableIfNotExists();

			prepareStatements();

		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void createTableIfNotExists() throws SQLException {
		System.err.println("CREATE");
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS "+tableName+" ("+KEY_COLUMN+" TEXT, "+VALUE_COLUMN+" TEXT)");
		conn.createStatement().executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS "+KEY_INDEX+" ON "+tableName+" ("+KEY_COLUMN+")");
		conn.createStatement().executeUpdate("CREATE INDEX IF NOT EXISTS "+VALUE_INDEX+" ON "+tableName+" ("+VALUE_COLUMN+")");
	}

	private void dropTable() throws SQLException {
		conn.createStatement().executeUpdate("DROP TABLE IF EXISTS "+tableName);
	}

	private void prepareStatements() throws SQLException {
		stmtCount = conn.prepareStatement("SELECT COUNT(*) FROM "+tableName);
		stmtEntries = conn.prepareStatement("SELECT "+KEY_COLUMN+", "+VALUE_COLUMN+" FROM "+tableName);
		stmtSelectKey = conn.prepareStatement("SELECT "+VALUE_COLUMN+" FROM "+tableName+" WHERE "+KEY_COLUMN+" = ?");
		stmtSelectValue = conn.prepareStatement("SELECT "+KEY_COLUMN+" FROM "+tableName+" WHERE "+VALUE_COLUMN+" = ?");
		stmtInsertKey = conn.prepareStatement("INSERT INTO "+tableName+" ("+KEY_COLUMN+", "+VALUE_COLUMN+") VALUES (?, ?)");
		stmtDeleteKey = conn.prepareStatement("DELETE FROM "+tableName+" WHERE "+KEY_COLUMN+" = ?");
	}


	@Override
	public boolean containsValue(Object value) {
		try {
			stmtSelectValue.setString(1, Pickler.pickleToString(value));
			return stmtSelectValue.executeQuery().next();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			stmtSelectKey.setString(1, Pickler.pickleToString(key));
			return stmtSelectKey.executeQuery().next();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(Object key) {
		try {
			stmtSelectKey.setString(1, Pickler.pickleToString(key));
			ResultSet rsGet = stmtSelectKey.executeQuery();
			return rsGet.next() ? Pickler.<V>unpickleFromString(rsGet.getString(VALUE_COLUMN)) : null;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V put(K key, V value) {
		try {
			V val = remove(key);
			stmtInsertKey.setString(1, Pickler.pickleToString(key));
			stmtInsertKey.setString(2, Pickler.pickleToString(value));
			stmtInsertKey.executeUpdate();
			return val;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			V value = get(key);
			stmtDeleteKey.setString(1, Pickler.pickleToString(key));
			stmtDeleteKey.executeUpdate();
			return value;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		try {
			dropTable();
			createTableIfNotExists();
			prepareStatements();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void commit() {
		try {
			if (!conn.getAutoCommit()) {
				conn.commit();
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void flush() {
		commit();
	}

	@Override
	public void close() {
		try {
			conn.close();
			conn = null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}


	class EntrySet extends AbstractSet<Map.Entry<K, V>> {

		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		@Override
		public int size() {
			try {
				ResultSet rsCount = stmtCount.executeQuery();
				rsCount.next();
				int count = rsCount.getInt(1);
				return count;

			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

	}

	class EntryIterator implements Iterator<Map.Entry<K, V>> {

		private ResultSet rsEntry;

		private boolean hasNext;
		private K nextKey;
		private V nextValue;

		private K removeKey;

		public EntryIterator() {
			try {
				rsEntry = stmtEntries.executeQuery();

				hasNext = rsEntry.next();
				if (hasNext) {
					nextKey = Pickler.unpickleFromString(rsEntry.getString(KEY_COLUMN));
					nextValue = Pickler.unpickleFromString(rsEntry.getString(VALUE_COLUMN));
				}

			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public Map.Entry<K, V> next() {
			if (hasNext) {
				Map.Entry<K, V> entry = new EntryWrapper(nextKey, nextValue);
				removeKey = nextKey;

				try {
					hasNext = rsEntry.next();
					if (hasNext) {
						nextKey = Pickler.unpickleFromString(rsEntry.getString(KEY_COLUMN));
						nextValue = Pickler.unpickleFromString(rsEntry.getString(VALUE_COLUMN));
					}

				} catch (SQLException e) {
					throw new RuntimeException(e);
				}

				return entry;
			}
			else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			if (null != removeKey) {
				SQLiteMap.this.remove(removeKey);
				removeKey = null;
			}
			else {
				throw new IllegalStateException();
			}
		}

	}

	protected class EntryWrapper implements Map.Entry<K, V> {

		protected K key;
		protected V value;

		public EntryWrapper(K key, V value) {
			this.key = key;
			this.value = value;
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
			this.value = value;
			return put(key, value);
		}

	}

}
