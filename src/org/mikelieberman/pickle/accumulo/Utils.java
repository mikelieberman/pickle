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

package org.mikelieberman.pickle.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class Utils {

	public static Map.Entry<Key, Value> getFirstEntry(Scanner scanner) {
		Iterator<Map.Entry<Key, Value>> i = scanner.iterator();
		return i.hasNext() ? i.next() : null;
	}
	
	public static <T> Text toText(T obj) throws IOException {
		return new Text(encode(obj));
	}
	
	public static <T> Value toValue(T obj) throws IOException {
		return new Value(encode(obj));
	}
	
	public static <T> Range toRange(T obj) throws IOException {
		return new Range(new Text(encode(obj)));
	}

	public static byte[] encode(Object obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		return baos.toByteArray();
	}
	
	public static Object decode(Key key) throws IOException, ClassNotFoundException {
		return decode(key.getRowData().getBackingArray());
	}
	
	public static Object decode(Text text) throws IOException, ClassNotFoundException {
		return decode(text.getBytes());
	}
	
	public static Object decode(Value value) throws IOException, ClassNotFoundException {
		return decode(value.get());
	}

	public static Object decode(byte[] data) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
		Object obj = ois.readObject();
		ois.close();
		return obj;
	}

}
