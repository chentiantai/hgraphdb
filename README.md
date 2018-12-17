# 本机安装类库
```
git clone git@github.com:chentiantai/hgraphdb.git
mvn install -DskipTests 
```
# 在您的应用中依赖jar

```
 <dependency>
            <groupId>io.hgraphdb</groupId>
            <artifactId>hgraphdb</artifactId>
            <version>2.0.1</version>
            <exclusions>
                <exclusion>
                    <groupId>com.google.cloud.bigtable</groupId>
                    <artifactId>bigtable-hbase-1.x-shaded</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
```
# 初始化
要初始化HGraphDB，请创建一个HBaseGraphConfiguration实例，然后使用静态工厂方法创建HBaseGraph实例。

```
Configuration cfg = new HBaseGraphConfiguration()
           .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace("hgraph")
                .setCreateTables(true)
                .setRegionCount(16)
                .set("hbase.zookeeper.quorum", args[0])
                .setUseSchema(true)
                .set("zookeeper.znode.parent", "/hbase");
			
				HBaseGraph graph = (HBaseGraph)GraphFactory.open(cfg);
				//TODO: use graph
				
				//close graph when finishing using graph
				graph.close();

```
如您所见，HBase特定的配置参数可以直接传递。这些将在获取HBase连接时使用。


# Schema管理
通过调用启用schema管理HBaseGraphConfiguration.useSchema(true)。启用schema管理后，可以定义顶点和边标签的schema。
```
graph.createLabel(ElementType.VERTEX, "author", /* id */ ValueType.STRING, "age", ValueType.INT);
graph.createLabel(ElementType.VERTEX, "book", /* id */ ValueType.STRING, "publisher", ValueType.STRING);
graph.createLabel(ElementType.EDGE, "writes", /* id */ ValueType.STRING, "since", ValueType.DATE);   
Edge labels must be explicitly connected to vertex labels before edges are added to the graph.

graph.connectLabels("author", "writes", "book"); 
Additional properties can be added to labels at a later time; otherwise labels cannot be changed.

graph.updateLabel(ElementType.VERTEX, "author", "height", ValueType.DOUBLE);
```
然后就可以添加顶点和边了，生成的图形可以像任何其他TinkerPop图形实例一样使用。
```
Vertex v1 = graph.addVertex(T.id, 1L, T.label, "person", "name", "John");
Vertex v2 = graph.addVertex(T.id, 2L, T.label, "person", "name", "Sally");
v1.addEdge("knows", v2, T.id, "edge1", "since", LocalDate.now());
```
以上示例中需要注意的一些事项：
* HGraphDB接受用户提供的顶点和边缘ID。
* 以下类型可用于ID和属性值
	* boolean
	* String
	* numbers (byte, short, int, long, float, double)
	* java.math.BigDecimal
	* java.time.LocalDate
	* java.time.LocalTime
	* java.time.LocalDateTime
	* java.time.Duration
	* java.util.UUID
	* byte arrays
	* Enum instances
	* Kryo-serializable instances
	* Java-serializable instances
	* 

# 使用索引
如果已使用导入工具创建schema，无需创建索引，可直接使用。
HGraphDB支持两种类型的索引：
顶点可以通过标签和属性进行索引。
边可以通过标签和属性索引，特定于顶点。
索引创建如下：
```
graph.createIndex(ElementType.VERTEX, "person", "name");
...
graph.createIndex(ElementType.EDGE, "knows", "since");
```

应在载入相关数据之前运行上述命令。要是在导入数据后创建索引，请首先使用以下参数创建索引：
```
graph.createIndex(ElementType.VERTEX, "person", "name", false, /* populate */ true, /* async */ true);
```
然后使用hbase命令跑一个MR作业
```
hbase io.hgraphdb.mapreduce.index.PopulateIndex \
    -t vertex -l person -p name -op /tmp -ca gremlin.hbase.namespace=mygraph
```
创建索引并载入数据后，可以按如下方式使用：
```
// get persons named John
Iterator<Vertex> it = graph.verticesByLabel("person", "name", "John");
...
// get persons first known by John between 2007-01-01 (inclusive) and 2008-01-01 (exclusive)
Iterator<Edge> it = johnV.edges(Direction.OUT, "knows", "since", 
    LocalDate.parse("2007-01-01"), LocalDate.parse("2008-01-01"));
```
请注意，索引支持范围查询，其中范围的起点是包含的，范围的结尾是不包含的。

索引也可以指定为唯一索引。对于顶点索引，这意味着对于同一种label顶点或边，该属性唯一标识这个对象，排他。

```
graph.createIndex(ElementType.VERTEX, "person", "name", /* unique */ true);
```
要删除索引，请使用以下hbase命令调用MapReduce作业：
```
hbase io.hgraphdb.mapreduce.index.DropIndex \
    -t vertex -l person -p name -op / tmp -ca gremlin.hbase.namespace = mygraph
```

# 分页
定义索引后，可以对结果进行分页。HGraphDB支持顶点和边索引的keyset pagination分页。

```
// get first page of persons (note that null is passed as start key)
final int pageSize = 20;
Iterator<Vertex> it = graph.verticesWithLimit("person", "name", null, pageSize);
...
// get next page using start key of last person from previous page
it = graph.verticesWithLimit("person", "name", "John", pageSize + 1);
...
// get first page of persons most recently known by John
Iterator<Edge> it = johnV.edgesWithLimit(Direction.OUT, "knows", "since", 
    null, pageSize, /* reversed */ true);
```
