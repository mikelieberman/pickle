package pickle.mapdb;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import pickle.NavigablePickleMap;

/**
 * @author Michael Lieberman
 */
public class MapDBMap<K, V> implements NavigablePickleMap<K, V> {

	protected DB db;
	protected NavigableMap<K, V> map;

	public MapDBMap(String dir) {
		this(dir, "map");
	}

	public MapDBMap(String dir, Comparator<K> comparator) {
		this(dir, "map", comparator);
	}

	public MapDBMap(String dir, String mapName) {
		this(dir, mapName, null);
	}

	public MapDBMap(String dir, String mapName, Comparator<K> comparator) {
		db = DBMaker.newFileDB(new File(dir)).writeAheadLogDisable().make();
		try {
			map = db.createTreeMap(mapName, 32, false, false, null, null, comparator);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			// Map already exists so just get it.
			map = db.getTreeMap(mapName);
		}
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

	@Override
	public Map.Entry<K, V> lowerEntry(K key) {
		return map.lowerEntry(key);
	}

	@Override
	public K lowerKey(K key) {
		return map.lowerKey(key);
	}

	@Override
	public Map.Entry<K, V> floorEntry(K key) {
		return map.floorEntry(key);
	}

	@Override
	public K floorKey(K key) {
		return map.floorKey(key);
	}

	@Override
	public Map.Entry<K, V> ceilingEntry(K key) {
		return map.ceilingEntry(key);
	}

	@Override
	public K ceilingKey(K key) {
		return map.ceilingKey(key);
	}

	@Override
	public Map.Entry<K, V> higherEntry(K key) {
		return map.higherEntry(key);
	}

	@Override
	public K higherKey(K key) {
		return map.higherKey(key);
	}

	@Override
	public Map.Entry<K, V> firstEntry() {
		return map.firstEntry();
	}

	@Override
	public Map.Entry<K, V> lastEntry() {
		return map.lastEntry();
	}

	@Override
	public Map.Entry<K, V> pollFirstEntry() {
		return map.pollFirstEntry();
	}

	@Override
	public Map.Entry<K, V> pollLastEntry() {
		return map.pollLastEntry();
	}

	@Override
	public NavigableMap<K, V> descendingMap() {
		return map.descendingMap();
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return map.navigableKeySet();
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return map.descendingKeySet();
	}

	@Override
	public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,
			boolean toInclusive) {
		return map.subMap(fromKey, fromInclusive, toKey, toInclusive);
	}

	@Override
	public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		return map.headMap(toKey, inclusive);
	}

	@Override
	public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		return map.tailMap(fromKey, inclusive);
	}

	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		return map.subMap(fromKey, toKey);
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		return map.headMap(toKey);
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		return map.tailMap(fromKey);
	}

	@Override
	public Comparator<? super K> comparator() {
		return map.comparator();
	}

	@Override
	public K firstKey() {
		return map.firstKey();
	}

	@Override
	public K lastKey() {
		return map.lastKey();
	}

}
