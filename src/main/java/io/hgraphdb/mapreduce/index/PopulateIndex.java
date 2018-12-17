package io.hgraphdb.mapreduce.index;

import io.hgraphdb.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An MR job to populate an index.
 */
public class PopulateIndex extends IndexTool {

    private static final Logger LOG = LoggerFactory.getLogger(PopulateIndex.class);

    @Override
    protected void setup(final HBaseGraph graph, final IndexMetadata index) {
        graph.updateIndex(index.key(), IndexMetadata.State.BUILDING);
    }

    @Override
    protected void cleanup(final HBaseGraph graph, final IndexMetadata index) {
        graph.updateIndex(index.key(), IndexMetadata.State.ACTIVE);
    }

    @Override
    protected Class<? extends Mapper> getDirectMapperClass() {
        return HBaseIndexImportDirectMapper.class;
    }

    @Override
    protected Class<? extends Reducer> getDirectReducerClass() {
        return HBaseIndexImportDirectReducer.class;
    }

    @Override
    protected Class<? extends Mapper> getBulkMapperClass() {
        return HBaseIndexImportMapper.class;
    }

    @Override
    protected TableName getInputTableName(final HBaseGraph graph, final IndexMetadata index) {
        return HBaseGraphUtils.getTableName(
                graph.configuration(), index.type() == ElementType.EDGE ? Constants.EDGES : Constants.VERTICES);
    }

    @Override
    protected TableName getOutputTableName(final HBaseGraph graph, final IndexMetadata index) {
        return HBaseGraphUtils.getTableName(
                graph.configuration(), index.type() == ElementType.EDGE ? Constants.EDGE_INDICES : Constants.VERTEX_INDICES);
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PopulateIndex(), args);
        System.exit(result);
    }
}
