package io.hgraphdb;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class Constants {

    /**
     * Table names
     */
    public static final String EDGES = "edges";
    public static final String EDGE_INDICES = "edgeIndices";
    public static final String VERTICES = "vertices";
    public static final String VERTEX_INDICES = "vertexIndices";
    public static final String INDEX_METADATA = "indexMetadata";
    public static final String LABEL_METADATA = "labelMetadata";
    public static final String LABEL_CONNECTIONS = "labelConnections";

    /**
     * Default column family
     */
    public static final String DEFAULT_FAMILY = "f";
    public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);

    /**
     * Internal keys
     */
    public static final String LABEL = Graph.Hidden.hide("l");
    public static final String FROM = Graph.Hidden.hide("f");
    public static final String TO = Graph.Hidden.hide("t");
    public static final String CREATED_AT = Graph.Hidden.hide("c");
    public static final String UPDATED_AT = Graph.Hidden.hide("u");
    public static final String UNIQUE = Graph.Hidden.hide("q");
    public static final String ELEMENT_ID = Graph.Hidden.hide("i");
    public static final String EDGE_ID = Graph.Hidden.hide("e");
    public static final String VERTEX_ID = Graph.Hidden.hide("v");
    public static final String INDEX_STATE = Graph.Hidden.hide("x");

    public static final byte[] LABEL_BYTES = Bytes.toBytes(LABEL);
    public static final byte[] FROM_BYTES = Bytes.toBytes(FROM);
    public static final byte[] TO_BYTES = Bytes.toBytes(TO);
    public static final byte[] CREATED_AT_BYTES = Bytes.toBytes(CREATED_AT);
    public static final byte[] UPDATED_AT_BYTES = Bytes.toBytes(UPDATED_AT);
    public static final byte[] UNIQUE_BYTES = Bytes.toBytes(UNIQUE);
    public static final byte[] ELEMENT_ID_BYTES = Bytes.toBytes(ELEMENT_ID);
    public static final byte[] EDGE_ID_BYTES = Bytes.toBytes(EDGE_ID);
    public static final byte[] VERTEX_ID_BYTES = Bytes.toBytes(VERTEX_ID);
    public static final byte[] INDEX_STATE_BYTES = Bytes.toBytes(INDEX_STATE);

    /**
     * Map-Reduce
     */
    public static final String EDGE_INPUT_TABLE             = "hbase.mapreduce.edgetable";
    public static final String VERTEX_INPUT_TABLE           = "hbase.mapreduce.vertextable";
    public static final String MAPREDUCE_INDEX_TYPE         = "hbase.mapreduce.index.type";
    public static final String MAPREDUCE_INDEX_LABEL        = "hbase.mapreduce.index.label";
    public static final String MAPREDUCE_INDEX_PROPERTY_KEY = "hbase.mapreduce.index.key";
    public static final String MAPREDUCE_INDEX_SKIP_WAL     = "hbase.mapreduce.index.skipwal";
}
