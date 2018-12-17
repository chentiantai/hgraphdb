package io.hgraphdb.giraph.examples;

import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseEdge;
import io.hgraphdb.HBaseVertex;
import io.hgraphdb.giraph.HBaseEdgeOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateEdgeOutputFormat extends HBaseEdgeOutputFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateEdgeOutputFormat.class);

    @Override
    public HBaseEdgeWriter createEdgeWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TestEdgeWriter(context);
    }

    /*
     For each edge, write back to the configured table using
     the edge id as the row key bytes.
     */
    public static class TestEdgeWriter extends HBaseEdgeWriter {

        public TestEdgeWriter(TaskAttemptContext context) {
            super(context);
        }

        @Override
        public void writeEdge(HBaseBulkLoader writer, HBaseVertex outVertex, HBaseEdge edge, Writable value) {
            outVertex.addEdge("e2", edge.inVertex());
        }
    }
}

