package org.mikelieberman.pickle.jdbm;
/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2013 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/


import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;


public class JDBMSet<T> implements NavigableSet<T> {
	
	private static final Byte NULL = 0;

	private JDBMMap<T, Byte> map;

	public JDBMSet(String file, Comparator<? super T> comparator) throws IOException {
		map = new JDBMMap<T, Byte>(file, comparator);
	}

	public JDBMSet(String file, Comparator<? super T> comparator, boolean clear) throws IOException {
		map = new JDBMMap<T, Byte>(file, comparator, clear);
	}

	public JDBMSet(String file, Comparator<? super T> comparator,
			boolean clear, long cacheSize) throws IOException {
		map = new JDBMMap<T, Byte>(file, comparator, clear, cacheSize);
	}
	
	public void flush() throws IOException {
		map.flush();
	}
	
	public void close() throws IOException {
		map.close();
	}

	@Override
	public Comparator<? super T> comparator() {
		return map.comparator();
	}

	@Override
	public T first() {
		return map.firstKey();
	}

	@Override
	public T last() {
		return map.lastKey();
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
	public boolean contains(Object o) {
		return map.containsKey(o);
	}

	@Override
	public Object[] toArray() {
		return map.keySet().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return map.keySet().toArray(a);
	}

	@Override
	public boolean add(T e) {
		if (map.containsKey(e)) {
			return false;
		}
		else {
			map.put(e, NULL);
			return true;
		}
	}

	@Override
	public boolean remove(Object o) {
		if (map.containsKey(o)) {
			map.remove(o);
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c) {
			if (!map.containsKey(o)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		boolean changed = false;

		for (T e : c) {
			if (!map.containsKey(e)) {
				map.put(e, NULL);
				changed = true;
			}
		}

		return changed;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean changed = false;

		for (T key : map.keySet()) {
			if (!c.contains(key)) {
				map.remove(key);
				changed = true;
			}
		}

		return changed;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed = false;
		
		for (Object o : c) {
			if (map.containsKey(o)) {
				map.remove(o);
				changed = true;
			}
		}
		
		return changed;
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public T lower(T e) {
		return map.lowerKey(e);
	}

	@Override
	public T floor(T e) {
		return map.floorKey(e);
	}

	@Override
	public T ceiling(T e) {
		return map.ceilingKey(e);
	}

	@Override
	public T higher(T e) {
		return map.higherKey(e);
	}

	@Override
	public T pollFirst() {
		Map.Entry<T, Byte> entry = map.pollFirstEntry();
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public T pollLast() {
		Map.Entry<T, Byte> entry = map.pollLastEntry();
		return entry != null ? entry.getKey() : null;
	}

	@Override
	public Iterator<T> iterator() {
		return map.keySet().iterator();
	}

	@Override
	public NavigableSet<T> descendingSet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> descendingIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableSet<T> subSet(T fromElement, boolean fromInclusive,
			T toElement, boolean toInclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableSet<T> headSet(T toElement, boolean inclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public NavigableSet<T> tailSet(T fromElement, boolean inclusive) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedSet<T> subSet(T fromElement, T toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedSet<T> headSet(T toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedSet<T> tailSet(T fromElement) {
		throw new UnsupportedOperationException();
	}
	
}
