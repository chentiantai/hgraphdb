package io.hgraphdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.time.LocalDate;
import java.util.Iterator;

import static org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HBaseSchemaTest extends HBaseGraphTest {

    @Override
    protected HBaseGraphConfiguration generateGraphConfig(String graphName) {
        HBaseGraphConfiguration config = super.generateGraphConfig(graphName);
        config.setElementCacheMaxSize(0);
        config.setRelationshipCacheMaxSize(0);
        config.setLazyLoading(true);
        config.setStaleIndexExpiryMs(0);
        config.setUseSchema(true);
        return config;
    }

    @Test
    public void testVertexLabel() {
        assertEquals(0, count(graph.vertices()));

        try {
            graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
            fail("Vertex should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        graph.createLabel(ElementType.VERTEX, "b", ValueType.LONG, "key2", ValueType.LONG);

        graph.createIndex(ElementType.VERTEX, "b", "key2");

        try {
            graph.addVertex(T.id, id(10), T.label, "a", "key1", 11);
            fail("Vertex should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addVertex(T.id, 10L, T.label, "b", "key1", 11);
            fail("Vertex should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addVertex(T.id, 10L, T.label, "b", "key2", 11);
            fail("Vertex should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        graph.addVertex(T.id, 10L, T.label, "b", "key2", 11L);

        Iterator<Vertex> it = graph.verticesByLabel("b", "key2", 11L);
        assertEquals(1, count(it));
    }

    @Test
    public void testVertexLabelAddProperty() {
        assertEquals(0, count(graph.vertices()));

        graph.createLabel(ElementType.VERTEX, "b", ValueType.LONG, "key2", ValueType.LONG);

        Vertex v1 = graph.addVertex(T.id, 10L, T.label, "b", "key2", 11L);

        Iterator<Vertex> it = graph.verticesByLabel("b", "key2", 11L);
        assertEquals(1, count(it));

        try {
            v1.property("key3", "hi");
        } catch (HBaseGraphNotValidException ignored) {
        }

        graph.updateLabel(ElementType.VERTEX, "b", "key3", ValueType.STRING);
        v1.property("key3", "hi");

        it = graph.verticesByLabel("b", "key2", 11L);
        assertEquals(1, count(it));
    }

    @Test
    public void testEdgeLabelAddProperty() {
        assertEquals(0, count(graph.vertices()));

        graph.createLabel(ElementType.VERTEX, "a", ValueType.STRING, "key0", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "b", ValueType.STRING, "key1", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "c", ValueType.STRING, "key2", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "d", ValueType.STRING, "key3", ValueType.INT);

        Vertex v1 = graph.addVertex(T.id, id(10), T.label, "a", "key0", 10);
        Vertex v2 = graph.addVertex(T.id, id(11), T.label, "b", "key1", 11);
        Vertex v3 = graph.addVertex(T.id, id(12), T.label, "c", "key2", 12);
        Vertex v4 = graph.addVertex(T.id, id(13), T.label, "d", "key3", 13);

        graph.createLabel(ElementType.EDGE, "knows", ValueType.STRING, "since", ValueType.DATE);
        graph.connectLabels("a", "knows", "b");

        Edge e = graph.addEdge(v1, v2, "knows", "since", LocalDate.now());

        Iterator<Edge> it = v1.edges(Direction.OUT, "knows");
        assertEquals(1, count(it));

        try {
            e.property("key3", "hi");
        } catch (HBaseGraphNotValidException ignored) {
        }

        graph.updateLabel(ElementType.EDGE, "knows", "key3", ValueType.STRING);
        e.property("key3", "hi");

        it = v1.edges(Direction.OUT, "knows");
        assertEquals(1, count(it));
    }

    @Test
    public void testEdgeLabel() {
        assertEquals(0, count(graph.vertices()));

        graph.createLabel(ElementType.VERTEX, "a", ValueType.STRING, "key0", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "b", ValueType.STRING, "key1", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "c", ValueType.STRING, "key2", ValueType.INT);
        graph.createLabel(ElementType.VERTEX, "d", ValueType.STRING, "key3", ValueType.INT);

        Vertex v1 = graph.addVertex(T.id, id(10), T.label, "a", "key0", 10);
        Vertex v2 = graph.addVertex(T.id, id(11), T.label, "b", "key1", 11);
        Vertex v3 = graph.addVertex(T.id, id(12), T.label, "c", "key2", 12);
        Vertex v4 = graph.addVertex(T.id, id(13), T.label, "d", "key3", 13);

        graph.createLabel(ElementType.EDGE, "knows", ValueType.STRING, "since", ValueType.DATE);
        graph.connectLabels("a", "knows", "b");

        graph.createIndex(ElementType.EDGE, "knows", "since");

        try {
            graph.addEdge(v3, v4, "foo", "blah", 11);
            fail("Edge should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addEdge(v1, v4, "foo", "blah", 11);
            fail("Edge should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addEdge(v1, v2, "foo", "blah", 11);
            fail("Edge should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addEdge(v1, v2, "knows", "blah", 11);
            fail("Edge should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        try {
            graph.addEdge(v1, v2, "knows", "since", 11);
            fail("Edge should be invalid");
        } catch (HBaseGraphNotValidException ignored) {
        }

        graph.addEdge(v1, v2, "knows", "since", LocalDate.now());

        Iterator<Edge> it = v1.edges(Direction.OUT, "knows");
        assertEquals(1, count(it));
    }
}
