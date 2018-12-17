// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.hgraphdb;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
public class GraphOfTheGodsFactory {

    public static void load(final HBaseGraph graph) {
        load(graph, true);
    }


    public static void load(final HBaseGraph graph, boolean createSchema) {

        if (createSchema) {
            //create vertexLabel
            graph.createLabel(ElementType.VERTEX, "titan", ValueType.STRING, "name",
                ValueType.STRING, "age", ValueType.INT);
            graph.createLabel(ElementType.VERTEX, "location", ValueType.STRING, "name",
                ValueType.STRING);
            graph.createLabel(ElementType.VERTEX, "god", ValueType.STRING, "name",
                ValueType.STRING, "age", ValueType.INT);
            graph.createLabel(ElementType.VERTEX, "demigod", ValueType.STRING, "name",
                ValueType.STRING, "age", ValueType.INT);
            graph.createLabel(ElementType.VERTEX, "human", ValueType.STRING, "name",
                ValueType.STRING, "age", ValueType.INT);
            graph.createLabel(ElementType.VERTEX, "monster", ValueType.STRING, "name",
                ValueType.STRING, "age", ValueType.INT);
            //create idx
            graph.createIndex(ElementType.VERTEX, "titan", "name",true);
            graph.createIndex(ElementType.VERTEX, "location", "name", true);
            graph.createIndex(ElementType.VERTEX, "god", "name", true);
            graph.createIndex(ElementType.VERTEX, "demigod", "name", true);
            graph.createIndex(ElementType.VERTEX, "human", "name", true);
            graph.createIndex(ElementType.VERTEX, "monster", "name", true);


            //create edgeLabel
            graph.createLabel(ElementType.EDGE, "father", ValueType.STRING);
            graph.createLabel(ElementType.EDGE, "mother", ValueType.STRING);
            graph.createLabel(ElementType.EDGE, "battled", ValueType.STRING, "time", ValueType.INT,
                "place", ValueType.STRING);
            graph.createLabel(ElementType.EDGE, "lives", ValueType.STRING, "reason", ValueType.STRING);
            graph.createLabel(ElementType.EDGE, "pet", ValueType.STRING);
            graph.createLabel(ElementType.EDGE, "brother", ValueType.STRING);

            graph.connectLabels("god", "father", "titan");
            graph.connectLabels("demigod", "father", "god");
            graph.connectLabels("demigod","mother","human");

            graph.connectLabels("demigod","battled","monster");
            graph.connectLabels("god","lives","location");
            graph.connectLabels("monster","lives","location");

            graph.connectLabels("god","pet","monster");

            graph.connectLabels("god","brother","god");

            //create idx
            graph.createIndex(ElementType.EDGE, "battled", "time");
        }

        // vertices data
        Vertex saturn = graph.addVertex(T.label, "titan", T.id, "saturn","name", "saturn", "age", 10000);
        Vertex sky = graph.addVertex(T.label, "location", T.id, "sky","name", "sky");
        Vertex sea = graph.addVertex(T.label, "location", T.id, "sea", "name", "sea");
        Vertex jupiter = graph.addVertex(T.label, "god", T.id, "jupiter", "name", "jupiter", "age", 5000);
        Vertex neptune = graph.addVertex(T.label, "god", T.id, "neptune", "name", "neptune", "age", 4500);
        Vertex hercules = graph.addVertex(T.label, "demigod", T.id, "hercules", "name", "hercules", "age", 30);
        Vertex alcmene = graph.addVertex(T.label, "human", T.id, "alcmene", "name", "alcmene", "age", 45);
        Vertex pluto = graph.addVertex(T.label, "god", T.id, "pluto", "name", "pluto", "age", 4000);
        Vertex nemean = graph.addVertex(T.label, "monster", T.id, "nemean", "name", "nemean");
        Vertex hydra = graph.addVertex(T.label, "monster", T.id, "hydra", "name", "hydra");
        Vertex cerberus = graph.addVertex(T.label, "monster", T.id, "cerberus", "name", "cerberus");
        Vertex tartarus = graph.addVertex(T.label, "location", T.id, "tartarus", "name", "tartarus");

        // edges data
        jupiter.addEdge("father", saturn);
        jupiter.addEdge("lives", sky, "reason", "loves fresh breezes");
        jupiter.addEdge("brother", neptune);
        jupiter.addEdge("brother", pluto);

        neptune.addEdge("lives", sea).property("reason", "loves waves");
        neptune.addEdge("brother", jupiter);
        neptune.addEdge("brother", pluto);

        hercules.addEdge("father", jupiter);
        hercules.addEdge("mother", alcmene);
        hercules.addEdge("battled", nemean, "time", 1, "place", "nemean's home");
        hercules.addEdge("battled", hydra, "time", 2, "place", "hydra's home");
        hercules.addEdge("battled", cerberus, "time", 12, "place", "cerberus's home");

        pluto.addEdge("brother", jupiter);
        pluto.addEdge("brother", neptune);
        pluto.addEdge("lives", tartarus, "reason", "no fear of death");
        pluto.addEdge("pet", cerberus);

        cerberus.addEdge("lives", tartarus);

    }

    public static void main(String args[]) {
        if (null == args || 2 != args.length) {
            System.err.println("Usage: GraphOfTheGodsFactory <zk-url> <create-schema>");
            System.exit(1);
        }

        Configuration cfg = new HBaseGraphConfiguration()
            .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
            .setGraphNamespace("mygraph")
                .setUseSchema(true)
            .setCreateTables(true)
            .set("hbase.zookeeper.quorum", args[0]);
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(cfg);
        boolean createSchema = Boolean.parseBoolean(args[1]);
        try {
            if (createSchema) {
                System.out.println("start to truncate table");
                graph.clear();
                graph.close();
                graph = (HBaseGraph) GraphFactory.open(cfg);
                load(graph);
            } else {
                load(graph, false);
            }

        }finally {
            graph.close();
        }

    }
}
