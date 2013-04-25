package pickle.mapdb;

import java.io.File;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import pickle.PickleMap;

/**
 * @author Michael Lieberman
 */
public class MapDBMap<K, V> implements PickleMap<K, V> {

	protected DB db;
	protected Map<K, V> map;

	public MapDBMap(String dir) {
		this(dir, "map");
	}

	public MapDBMap(String dir, String mapName) {
		this(dir, mapName, false);
	}

	public MapDBMap(String dir, String mapName, boolean clear) {
		db = DBMaker.newFileDB(new File(dir)).writeAheadLogDisable().make();
		map = db.getHashMap(mapName);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		return map.put(key, value);
	}

	@Override
	public V remove(Object key) {
		return map.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public Set<K> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<V> values() {
		return map.values();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	@Override
	public void flush() {
		db.commit();
	}

	@Override
	public void close() {
		db.commit();
		db.compact();
		db.close();
	}

}
