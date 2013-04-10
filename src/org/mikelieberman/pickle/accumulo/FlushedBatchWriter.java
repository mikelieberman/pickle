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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class FlushedBatchWriter implements BatchWriter {

	private BatchWriter writer;
	
	public FlushedBatchWriter(BatchWriter writer) {
		this.writer = writer;
	}
	
	@Override
	public void addMutation(Mutation m) throws MutationsRejectedException {
		writer.addMutation(m);
		flush();
	}

	@Override
	public void addMutations(Iterable<Mutation> iterable)
			throws MutationsRejectedException {
		writer.addMutations(iterable);
		flush();
	}

	@Override
	public void flush() throws MutationsRejectedException {
		writer.flush();
	}

	@Override
	public void close() throws MutationsRejectedException {
		writer.close();
	}

}
