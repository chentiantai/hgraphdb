package io.hgraphdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mock.MockConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.DefaultCloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static io.hgraphdb.Constants.DEFAULT_FAMILY;
import static io.hgraphdb.HBaseGraphConfiguration.Keys.*;

public final class HBaseGraphUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseGraphUtils.class);

    private static final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public static Connection getConnection(HBaseGraphConfiguration config) {
        Connection conn = connections.get(config.getGraphNamespace());
        if (conn != null && !conn.isClosed()) return conn;
        Configuration hbaseConfig = config.toHBaseConfiguration();
        switch (config.getInstanceType()) {
            case MOCK:
                conn = MockConnectionFactory.createConnection(hbaseConfig);
                break;
            case BIGTABLE:
            case DISTRIBUTED:
                try {
                    UserGroupInformation ugi = null;
                    if ("kerberos".equals(hbaseConfig.get(HBASE_SECURITY_AUTHENTICATION))) {
                        String principal = hbaseConfig.get(HBASE_CLIENT_KERBEROS_PRINCIPAL);
                        String keytab = hbaseConfig.get(HBASE_CLIENT_KEYTAB_FILE);
                        if (principal != null && keytab != null) {
                            UserGroupInformation.setConfiguration(hbaseConfig);
                            UserGroupInformation.loginUserFromKeytab(principal, keytab);
                            ugi = UserGroupInformation.getLoginUser();
                        }
                    }
                    if (ugi != null) {
                        conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                            @Override
                            public Connection run() throws Exception {
                                return ConnectionFactory.createConnection(hbaseConfig);
                            }
                        });
                    } else {
                        conn = ConnectionFactory.createConnection(hbaseConfig);
                    }
                    break;
                } catch (Exception e) {
                    throw new HBaseGraphException(e);
                }
        }
        connections.put(config.getGraphNamespace(), conn);
        return conn;
    }

    public static void closeConnections() {
        for (Connection conn : connections.values()) {
            try {
                conn.close();
            } catch (IOException e) {
                LOGGER.warn("Could not close connection");
            }
        }
        connections.clear();
    }

    public static TableName getTableName(HBaseGraphConfiguration config, String name) {
        String ns = config.getGraphNamespace();
        String tablePrefix = config.getGraphTablePrefix();
        if (!tablePrefix.isEmpty()) {
            name = tablePrefix + "_" + name;
        }
        return config.getInstanceType() == HBaseGraphConfiguration.InstanceType.BIGTABLE
                ? TableName.valueOf(name)
                : TableName.valueOf(ns, name);
    }

    public static void createTables(HBaseGraphConfiguration config, Connection conn) {
        if (config.getInstanceType() == HBaseGraphConfiguration.InstanceType.MOCK) return;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (config.getInstanceType() != HBaseGraphConfiguration.InstanceType.BIGTABLE) {
                createNamespace(config, admin);
            }
            createTables(config, admin);
        } catch (Exception e) {
            throw new HBaseGraphException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static void createNamespace(HBaseGraphConfiguration config, Admin admin) throws IOException {
        String name = config.getGraphNamespace();
        try {
            NamespaceDescriptor ns = admin.getNamespaceDescriptor(name);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(name).build());
        }
    }

    private static void createTables(HBaseGraphConfiguration config, Admin admin) throws IOException {
        createTable(config, admin, Constants.EDGES, config.getEdgeTableTTL());
        createTable(config, admin, Constants.EDGE_INDICES, config.getEdgeTableTTL());
        createTable(config, admin, Constants.VERTICES, config.getVertexTableTTL());
        createTable(config, admin, Constants.VERTEX_INDICES, config.getVertexTableTTL());
        createTable(config, admin, Constants.INDEX_METADATA, HConstants.FOREVER);
        if (config.getUseSchema()) {
            createTable(config, admin, Constants.LABEL_METADATA, HConstants.FOREVER);
            createTable(config, admin, Constants.LABEL_CONNECTIONS, HConstants.FOREVER);
        }
    }

    private static void createTable(HBaseGraphConfiguration config, Admin admin, String name, int ttl) throws IOException {
        TableName tableName = getTableName(config, name);
        if (admin.tableExists(tableName)) return;
        try {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.setDurability(config.getTableAsyncWAL() ? Durability.ASYNC_WAL : Durability.USE_DEFAULT);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY)
                    .setCompressionType(Compression.Algorithm.valueOf(config.getCompressionAlgorithm().toUpperCase()))
                    .setBloomFilterType(BloomType.ROW)
                    .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                    .setMaxVersions(1)
                    .setMinVersions(0)
                    .setBlocksize(32768)
                    .setBlockCacheEnabled(true)
                    .setTimeToLive(ttl);
            tableDescriptor.addFamily(columnDescriptor);
            int regionCount = config.getRegionCount();
            if (regionCount <= 1) {
                admin.createTable(tableDescriptor);
            } else {
                admin.createTable(tableDescriptor, getStartKey(regionCount), getEndKey(regionCount), regionCount);
            }
        } catch (IOException e) {
            LOGGER.error("Could not create table " + tableName, e);
            if (e instanceof  TableExistsException) {//ignore
                return;
            }
            throw e;
        }
    }

    public static void dropTables(HBaseGraphConfiguration config, Connection conn) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            dropTables(config, admin);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static void truncateTables(HBaseGraphConfiguration config, Connection conn) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            truncateTables(config, admin);
        } catch (IOException e) {
            throw new HBaseGraphException(e);
        } finally {
            try {
                if (admin != null) admin.close();
            } catch (IOException ignored) {
            }
        }
    }
    private static void dropTables(HBaseGraphConfiguration config, Admin admin) throws IOException {
        dropTable(config, admin, Constants.EDGES);
        dropTable(config, admin, Constants.EDGE_INDICES);
        dropTable(config, admin, Constants.VERTICES);
        dropTable(config, admin, Constants.VERTEX_INDICES);
        dropTable(config, admin, Constants.INDEX_METADATA);
//        if (config.getUseSchema()) {
            dropTable(config, admin, Constants.LABEL_METADATA);
            dropTable(config, admin, Constants.LABEL_CONNECTIONS);
//        }
        admin.deleteNamespace(config.getGraphNamespace());
    }

    private static void truncateTables(HBaseGraphConfiguration config, Admin admin) throws IOException {
        truncateTable(config, admin, Constants.EDGES);
        truncateTable(config, admin, Constants.EDGE_INDICES);
        truncateTable(config, admin, Constants.VERTICES);
        truncateTable(config, admin, Constants.VERTEX_INDICES);
        truncateTable(config, admin, Constants.INDEX_METADATA);
        truncateTable(config, admin, Constants.LABEL_METADATA);
        truncateTable(config, admin, Constants.LABEL_CONNECTIONS);

    }

    private static void dropTable(HBaseGraphConfiguration config, Admin admin, String name) throws IOException {
        TableName tableName = getTableName(config, name);
        if (!admin.tableExists(tableName)) return;
        if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
    }

    private static void truncateTable(HBaseGraphConfiguration config, Admin admin, String name) throws IOException {
        TableName tableName = getTableName(config, name);
        if (!admin.tableExists(tableName)) return;
        if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
        }
        admin.truncateTable(tableName, true);
    }

    private static byte[] getStartKey(int regionCount) {
        byte[] ret = {(byte)((Byte.MAX_VALUE-Byte.MIN_VALUE+1) / regionCount)};
        return ret;
    }

    private static byte[] getEndKey(int regionCount) {
        byte[] ret = {(byte)((Byte.MAX_VALUE-Byte.MIN_VALUE+1) / regionCount * (regionCount - 1))};
        return ret;
    }

    public static byte[] incrementBytes(final byte[] value) {
        byte[] newValue = Arrays.copyOf(value, value.length);
        for (int i = 0; i < newValue.length; i++) {
            int val = newValue[newValue.length - i - 1] & 0x0ff;
            int total = val + 1;
            boolean carry = false;
            if (total > 255) {
                carry = true;
                total %= 256;
            }
            newValue[newValue.length - i - 1] = (byte) total;
            if (!carry) return newValue;
        }
        return newValue;
    }

    public static Object generateIdIfNeeded(Object id) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        } else if (id instanceof Long) {
            // noop
        } else if (id instanceof Number) {
            id = ((Number) id).longValue();
        }
        return id;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(keyStr, value);
            props.put(keyStr, value);
        }
        return props;
    }

    public static Map<String, ValueType> propertyKeysAndTypesToMap(Object... keyTypes) {
        Map<String, ValueType> props = new HashMap<>();
        for (int i = 0; i < keyTypes.length; i = i + 2) {
            Object key = keyTypes[i];
            if (key.equals(T.id) || key.equals(T.label)) continue;
            String keyStr = key.toString();
            Object type = keyTypes[i + 1];
            ValueType valueType;
            if (type instanceof ValueType) {
                valueType = (ValueType) type;
            } else {
                valueType = ValueType.valueOf(type.toString().toUpperCase());
            }
            props.put(keyStr, valueType);
        }
        return props;
    }

    @SuppressWarnings("unchecked")
    public static <E> Iterator<E> mapWithCloseAtEnd(ResultScanner scanner, final Function<Result, E> function) {
        Iterator<E> iterator = CloseableIteratorUtils.flatMap(
                CloseableIteratorUtils.concat(scanner.iterator(), IteratorUtils.of(Result.EMPTY_RESULT)),
                result -> {
                    if (result == Result.EMPTY_RESULT) {
                        scanner.close();
                        return Collections.emptyIterator();
                    }
                    return IteratorUtils.of(function.apply(result));
                });
        return new DefaultCloseableIterator<E>(iterator) {
            @Override
            public void close() {
                scanner.close();
            }
        };
    }
}
