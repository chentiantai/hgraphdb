package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import io.hgraphdb.readers.EdgeReader;
import io.hgraphdb.readers.ElementReader;
import io.hgraphdb.readers.VertexReader;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Mapper that hands over rows from data table to the index table.
 */
public class HBaseIndexImportDirectMapper extends HBaseIndexDirectMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexImportDirectMapper.class);

    private ElementReader<?> reader;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        reader = getIndex().type() == ElementType.EDGE ? new EdgeReader(getGraph()) : new VertexReader(getGraph());
    }

    @Override
    protected Iterator<? extends Mutation> constructMutations(Result result)
            throws IOException, InterruptedException {
        return HBaseIndexImportMapper.constructMutations(getGraph(), getIndex(), reader, result);
    }
}
