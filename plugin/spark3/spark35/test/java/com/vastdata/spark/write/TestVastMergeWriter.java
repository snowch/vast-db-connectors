/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.google.common.collect.ImmutableMap;
import com.vastdata.client.QueryDataExtraParams;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.rowid.RowIdListSchemaFactory;
import com.vastdata.client.tx.VastTraceToken;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.VastTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.InternalRow$;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.collection.mutable.Builder;
import spark.sql.catalog.ndb.BoundBucketFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_FIELD_NAME;
import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.spark.SparkTestUtils.getTestConfig;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_DEC128_ROW_ID_FIELD;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_INT64_ROW_ID_FIELD;

/**
 * Drives a VastWriteFactory in MERGE mode with a mocked VastClient and checks
 * which RPC every kind of row ends up in, with which Arrow schema; the chunk
 * hand-off to the background writer, which MERGE shares with plain INSERT, is
 * checked in both modes.
 */
@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestVastMergeWriter
{
    private static final String SCHEMA_NAME = "buck/schem";
    private static final String TABLE_NAME = "tab";
    private static final String TABLE_PATH = "/" + SCHEMA_NAME + "/" + TABLE_NAME;
    private static final List<String> DATA_COLUMNS = List.of("k", "v");
    private static final int CHUNK_SIZE = 2;

    private static final class Rpc
    {
        final String kind;
        final String path;
        final List<String> fields;
        final List<ArrowType> types;
        final int rows;
        final List<Object> firstColumn;
        // the values of the partition key column k, empty when the request does not carry it
        final List<Object> keys;
        // the partition ids of the 128-bit row ids in the first column, empty otherwise
        final List<Object> partitions;

        Rpc(String kind, String path, org.apache.arrow.vector.types.pojo.Schema schema,
                int rows, List<Object> firstColumn, List<Object> keys, List<Object> partitions)
        {
            this.kind = kind;
            this.path = path;
            this.fields = schema.getFields().stream().map(Field::getName).collect(
                    Collectors.toList());
            this.types = schema.getFields().stream().map(Field::getType).collect(
                    Collectors.toList());
            this.rows = rows;
            this.firstColumn = firstColumn;
            this.keys = keys;
            this.partitions = partitions;
        }

        @Override
        public String toString()
        {
            return kind + path + fields + types + "x" + rows + firstColumn + " k=" + keys + " p=" + partitions;
        }
    }

    private VastClient client;
    private VastTransaction tx;
    private List<Rpc> rpcs;
    private AtomicInteger rollbacks;

    // 128-bit row ids carry the partition id in their high 64 bits and the row in the low ones
    private static Decimal rowId(long partition, long row)
    {
        return Decimal.apply(new BigDecimal(BigInteger
                .valueOf(partition)
                .shiftLeft(64)
                .add(BigInteger.valueOf(row))));
    }

    private static List<Object> rowIdPartitions(VectorSchemaRoot root)
    {
        List<Object> partitions = new ArrayList<>();
        if (root.getVector(0) instanceof DecimalVector) {
            DecimalVector vector = (DecimalVector) root.getVector(0);
            for (int i = 0; i < root.getRowCount(); i++) {
                partitions.add(vector.getObject(i).toBigInteger().shiftRight(64).longValue());
            }
        }
        return partitions;
    }

    // firstColumnValues() reads the low 64 bits of a 128-bit row id: the row within its partition
    private static List<Object> firstColumnValues(VectorSchemaRoot root)
    {
        FieldVector vector = root.getVector(0);
        List<Object> values = new ArrayList<>(root.getRowCount());
        for (int i = 0; i < root.getRowCount(); i++) {
            if (vector instanceof DecimalVector) {
                values.add(((DecimalVector) vector).getObject(i).longValue());
            }
            else if (vector instanceof UInt8Vector) {
                values.add(((UInt8Vector) vector).get(i));
            }
            else {
                values.add(vector.getObject(i));
            }
        }
        return values;
    }

    private static List<Object> keyValues(VectorSchemaRoot root)
    {
        List<Object> values = new ArrayList<>();
        for (FieldVector vector : root.getFieldVectors()) {
            if (vector.getName().equals("k")) {
                for (int i = 0; i < root.getRowCount(); i++) {
                    values.add(vector.getObject(i));
                }
            }
        }
        return values;
    }

    private static VectorSchemaRoot rowIds(int count, BufferAllocator allocator,
            RowIDStrategyType type)
    {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                RowIdListSchemaFactory.get(type), allocator);
        FieldVector vector = root.getVector(0);
        if (vector instanceof DecimalVector) {
            DecimalVector decimals = (DecimalVector) vector;
            decimals.allocateNew(count);
            for (int i = 0; i < count; i++) {
                decimals.set(i, BigDecimal.valueOf(1000 + i));
            }
        }
        else {
            UInt8Vector longs = (UInt8Vector) vector;
            longs.allocateNew(count);
            for (int i = 0; i < count; i++) {
                longs.set(i, 1000 + i);
            }
        }
        vector.setValueCount(count);
        root.setRowCount(count);
        return root;
    }

    private static InternalRow row(Object... values)
    {
        Builder<Object, scala.collection.immutable.List<Object>> builder = List$.MODULE$.newBuilder();
        for (Object value : values) {
            builder.$plus$eq(value);
        }
        return InternalRow$.MODULE$.apply(builder.result());
    }

    @BeforeMethod
    public void setup()
    {
        rpcs = Collections.synchronizedList(new ArrayList<>());
        rollbacks = new AtomicInteger();
        tx = mock(VastTransaction.class);
        when(tx.getId()).thenReturn(1L);
        when(tx.generateTraceToken(any())).thenReturn(
                new VastTraceToken(Optional.empty(), 1L, 1));
        client = mock(VastClient.class);
    }

    private void stubClient(RowIDStrategyType rowIdType)
            throws Exception
    {
        doAnswer(invocation -> {
            VectorSchemaRoot root = invocation.getArgument(3);
            rpcs.add(new Rpc("delete",
                    "/" + invocation.getArgument(1) + "/" + invocation.getArgument(2),
                    root.getSchema(), root.getRowCount(), firstColumnValues(root),
                    keyValues(root), rowIdPartitions(root)));
            return null;
        }).when(client).deleteRows(any(), anyString(), anyString(),
                any(VectorSchemaRoot.class), any(URI.class), any(),
                any(QueryDataExtraParams.class), nullable(String.class));
        doAnswer(invocation -> {
            VectorSchemaRoot root = invocation.getArgument(3);
            rpcs.add(new Rpc("update",
                    "/" + invocation.getArgument(1) + "/" + invocation.getArgument(2),
                    root.getSchema(), root.getRowCount(), firstColumnValues(root),
                    keyValues(root), rowIdPartitions(root)));
            return null;
        }).when(client).updateRows(any(), anyString(), anyString(),
                any(VectorSchemaRoot.class), any(URI.class), any(),
                any(QueryDataExtraParams.class), nullable(String.class));
        doAnswer(invocation -> {
            byte[] body = invocation.getArgument(3);
            int rows = recordIpc("insert", invocation.getArgument(2), body);
            return rowIds(rows, invocation.getArgument(7), rowIdType);
        }).when(client).insertRows(any(), any(URI.class), anyString(),
                any(byte[].class), anyBoolean(), any(QueryDataExtraParams.class),
                nullable(String.class), any(BufferAllocator.class));
        doAnswer(invocation -> {
            byte[] body = invocation.getArgument(2);
            recordIpc("insert-update", invocation.getArgument(1), body);
            return null;
        }).when(client).updateRows(any(), anyString(), any(byte[].class),
                any(URI.class), any(QueryDataExtraParams.class),
                nullable(String.class));
        doAnswer(invocation -> {
            rollbacks.incrementAndGet();
            return null;
        }).when(client).rollbackTransaction(any(), nullable(String.class));
    }

    // a slow client keeps every chunk after the first one in flight, unserialized, while the
    // foreground commits
    private void slowInserts(long millis, RowIDStrategyType rowIdType)
            throws Exception
    {
        doAnswer(invocation -> {
            Thread.sleep(millis);
            byte[] body = invocation.getArgument(3);
            int rows = recordIpc("insert", invocation.getArgument(2), body);
            return rowIds(rows, invocation.getArgument(7), rowIdType);
        }).when(client).insertRows(any(), any(URI.class), anyString(),
                any(byte[].class), anyBoolean(), any(QueryDataExtraParams.class),
                nullable(String.class), any(BufferAllocator.class));
    }

    private int recordIpc(String kind, String path, byte[] body)
            throws IOException
    {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(
                        new ByteArrayInputStream(body), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int rows = 0;
            List<Object> keys = new ArrayList<>();
            while (reader.loadNextBatch()) {
                rows += root.getRowCount();
                keys.addAll(keyValues(root));
            }
            rpcs.add(new Rpc(kind, path, root.getSchema(), rows, List.of(), keys, List.of()));
            return rows;
        }
    }

    private VastWriteFactory mergeFactory(boolean sorted)
    {
        VastTable table = table(mergeSchema(sorted ? SPARK_DEC128_ROW_ID_FIELD : SPARK_INT64_ROW_ID_FIELD),
                new Transform[0],
                sorted ? ImmutableMap.of(SORTED_BY_PROPERTY, "k") : Map.of());
        table.getTableMD().setForMerge();
        return factory(table);
    }

    // a partitioned table carries the wide row id, like a sorted one
    private VastWriteFactory partitionedMergeFactory(Transform partitioning)
    {
        VastTable table = table(mergeSchema(SPARK_DEC128_ROW_ID_FIELD),
                new Transform[] {partitioning}, Map.of());
        table.getTableMD().setForMerge();
        return factory(table);
    }

    // a plain INSERT writer: no row id in the rows and, as for every table the catalog builds,
    // an empty (not null) partitioning, so the rows still go through the partition queues
    private VastWriteFactory insertFactory()
    {
        StructType schema = new StructType(new StructField[] {
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("v", DataTypes.StringType, true)});
        return factory(table(schema, new Transform[0], Map.of()));
    }

    // a plain DELETE writer on a partitioned table
    private VastWriteFactory partitionedDeleteFactory()
    {
        VastTable table = table(mergeSchema(SPARK_DEC128_ROW_ID_FIELD),
                new Transform[] {Expressions.identity("k")}, Map.of());
        table.getTableMD().setForDelete();
        return factory(table);
    }

    // MERGE reads the row id with the row: field 0 of the table schema
    private static StructType mergeSchema(StructField rowIdField)
    {
        return new StructType(new StructField[] {rowIdField,
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("v", DataTypes.StringType, true)});
    }

    private VastTable table(StructType schema, Transform[] partitioning,
            Map<String, String> properties)
    {
        return new VastTable(null, SCHEMA_NAME, TABLE_NAME, "handle", schema, partitioning,
                () -> client, false, Optional.empty(), properties);
    }

    private VastWriteFactory factory(VastTable table)
    {
        VastConfig config = getTestConfig()
                .setMaxRowsPerDelete(CHUNK_SIZE)
                .setMaxRowsPerUpdate(CHUNK_SIZE)
                .setMaxRowsPerInsert(CHUNK_SIZE);
        return new VastWriteFactory(tx, config, table,
                List.of(URI.create("http://localhost:1234")),
                (scala.collection.immutable.Map<String, String>) Map$.MODULE$.<String, String>empty(),
                vastConfig -> client);
    }

    private static Object id(boolean sorted, long value)
    {
        return sorted ? Decimal.apply(value) : (Object) value;
    }

    private List<Rpc> rpcs(String kind)
    {
        return rpcs.stream().filter(r -> r.kind.equals(kind)).collect(
                Collectors.toList());
    }

    private static int totalRows(List<Rpc> rpcs)
    {
        return rpcs.stream().mapToInt(r -> r.rows).sum();
    }

    // the partition keys of every inserted chunk, as the by-column inserter sent them (in the
    // insert request or in its follow-up update); each chunk and the list of chunks are sorted,
    // the order the background writer takes is not part of the contract
    private List<String> insertedKeyChunks()
    {
        return chunks(rpcs
                .stream()
                .filter(r -> (r.kind.equals("insert") || r.kind.equals("insert-update")) && !r.keys.isEmpty())
                .map(r -> r.keys)
                .collect(Collectors.toList()));
    }

    // one entry per delete or update request: the partition ids of its row ids, then the rows
    // in request order; sorted, the order of the requests is not part of the contract
    private List<String> rowIdRequests(String kind)
    {
        return rpcs(kind)
                .stream()
                .map(r -> new TreeSet<>(r.partitions) + ":" + r.firstColumn)
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<String> chunks(List<? extends List<?>> keyChunks)
    {
        return keyChunks
                .stream()
                .map(chunk -> chunk.stream().map(String::valueOf).sorted().collect(
                        Collectors.joining(",", "[", "]")))
                .sorted()
                .collect(Collectors.toList());
    }

    private void assertInterleavedMerge(boolean sorted, ArrowType rowIdType)
            throws Exception
    {
        stubClient(sorted ?
                RowIDStrategyType.DECIMAL_128 :
                RowIDStrategyType.UNSIGNED_INT64);
        DeltaWriter<InternalRow> writer = mergeFactory(sorted).createWriter(0, 7L);

        writer.update(null, row(id(sorted, 3)), row(id(sorted, 3), 30, UTF8String.fromString("c")));
        writer.insert(row(null, 20, UTF8String.fromString("b")));
        writer.delete(null, row(id(sorted, 4)));
        writer.update(null, row(id(sorted, 1)), row(id(sorted, 1), 10, UTF8String.fromString("a")));
        writer.delete(null, row(id(sorted, 2)));
        writer.insert(row(null, 40, UTF8String.fromString("d")));
        writer.insert(row(null, 50, UTF8String.fromString("e")));
        writer.delete(null, row(id(sorted, 6)));
        writer.update(null, row(id(sorted, 5)), row(id(sorted, 5), 60, UTF8String.fromString("f")));
        WriterCommitMessage message = writer.commit();
        writer.close();

        assertNotNull(message);
        assertEquals(rollbacks.get(), 0);

        List<Rpc> deletes = rpcs("delete");
        assertEquals(totalRows(deletes), 3, "deletes: " + deletes);
        assertEquals(deletes.size(), 2, "one full chunk and the remainder: " + deletes);
        for (Rpc delete : deletes) {
            assertEquals(delete.path, TABLE_PATH);
            assertEquals(delete.fields, List.of(ROW_ID_FIELD_NAME), "delete: " + delete);
            assertEquals(delete.types, List.of(rowIdType), "delete: " + delete);
        }
        // row ids are sent in ascending order within a chunk (priority queue)
        assertEquals(deletes.get(0).firstColumn, List.of(2L, 4L));
        assertEquals(deletes.get(1).firstColumn, List.of(6L));

        List<Rpc> updates = rpcs("update");
        assertEquals(totalRows(updates), 3, "updates: " + updates);
        assertEquals(updates.size(), 2, "updates: " + updates);
        for (Rpc update : updates) {
            assertEquals(update.path, TABLE_PATH);
            assertEquals(update.fields, List.of(ROW_ID_FIELD_NAME, "k", "v"), "update: " + update);
            assertEquals(update.types.get(0), rowIdType, "update: " + update);
        }
        assertEquals(updates.get(0).firstColumn, List.of(1L, 3L));
        assertEquals(updates.get(1).firstColumn, List.of(5L));

        // inserted rows never carry the (null) row id slot Spark projects
        List<Rpc> inserts = rpcs("insert");
        assertEquals(totalRows(inserts), 3, "inserts: " + inserts);
        assertTrue(inserts.size() >= 2, "inserts: " + inserts);
        for (Rpc insert : inserts) {
            assertEquals(insert.path, TABLE_PATH);
            assertTrue(DATA_COLUMNS.containsAll(insert.fields), "insert: " + insert);
            assertTrue(!insert.fields.isEmpty(), "insert: " + insert);
        }
        // columns the by-column inserter sends as a follow-up update are keyed by
        // the row ids the insert returned, and only ever carry data columns
        for (Rpc followUp : rpcs("insert-update")) {
            assertEquals(followUp.path, TABLE_PATH);
            assertEquals(followUp.fields.get(0), ROW_ID_FIELD_NAME, "follow up: " + followUp);
            assertTrue(DATA_COLUMNS.containsAll(followUp.fields.subList(1, followUp.fields.size())),
                    "follow up: " + followUp);
        }
    }

    @Test
    public void testInterleavedDeleteUpdateInsertInt64RowId()
            throws Exception
    {
        assertInterleavedMerge(false, new ArrowType.Int(64, false));
    }

    @Test
    public void testInterleavedDeleteUpdateInsertDec128RowId()
            throws Exception
    {
        assertInterleavedMerge(true, new ArrowType.Decimal(38, 0, 128));
    }

    @Test
    public void testOnlyUsedContextsAreCreated()
            throws Exception
    {
        stubClient(RowIDStrategyType.UNSIGNED_INT64);
        DeltaWriter<InternalRow> writer = mergeFactory(false).createWriter(1, 8L);
        writer.delete(null, row(9L));
        writer.commit();
        writer.close();
        assertEquals(rpcs("delete").size(), 1);
        assertEquals(rpcs("update").size(), 0);
        assertEquals(rpcs("insert").size(), 0);
        assertEquals(rollbacks.get(), 0);

        // an empty task commits without creating any context
        DeltaWriter<InternalRow> emptyWriter = mergeFactory(false).createWriter(2, 9L);
        assertNotNull(emptyWriter.commit());
        emptyWriter.close();
        assertEquals(rpcs.size(), 1);
    }

    @Test
    public void testUpdateWithChangedRowIdIsRefused()
            throws Exception
    {
        stubClient(RowIDStrategyType.UNSIGNED_INT64);
        DeltaWriter<InternalRow> writer = mergeFactory(false).createWriter(0, 7L);
        try {
            writer.update(null, row(1L), row(2L, 10, UTF8String.fromString("a")));
            fail("expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("can not be changed"), e.getMessage());
        }
        finally {
            writer.abort();
            writer.close();
        }
        assertEquals(rollbacks.get(), 0);

        stubClient(RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> sortedWriter = mergeFactory(true).createWriter(0, 7L);
        try {
            sortedWriter.update(null, row(Decimal.apply(1L)),
                    row(Decimal.apply(2L), 10, UTF8String.fromString("a")));
            fail("expected an IllegalStateException");
        }
        catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("can not be changed"), e.getMessage());
        }
        finally {
            sortedWriter.abort();
            sortedWriter.close();
        }
        assertEquals(rollbacks.get(), 0);
    }

    @Test
    public void testFailureInOneContextRollsBackOnce()
            throws Exception
    {
        stubClient(RowIDStrategyType.UNSIGNED_INT64);
        doAnswer(invocation -> {
            throw new RuntimeException("Simulated delete failure");
        }).when(client).deleteRows(any(), anyString(), anyString(),
                any(VectorSchemaRoot.class), any(URI.class), any(),
                any(QueryDataExtraParams.class), nullable(String.class));
        DeltaWriter<InternalRow> writer = mergeFactory(false).createWriter(0, 7L);
        // a second, healthy context
        writer.update(null, row(1L), row(1L, 10, UTF8String.fromString("a")));
        // a full chunk of deletes is handed to the background writer, which fails
        writer.delete(null, row(2L));
        writer.delete(null, row(3L));
        // As with the single mode writer, commit() flushes and waits for the
        // background writers; the failure then surfaces from close(), which is
        // what fails the Spark task
        writer.commit();
        assertEquals(rpcs("update").size(), 1, "healthy context flushed: " + rpcs);
        for (int i = 0; i < 2; i++) {
            try {
                writer.close();
                fail("close() is expected to re-throw the background failure");
            }
            catch (RuntimeException e) {
                assertTrue(e.getMessage().contains("Simulated delete failure"), e.getMessage());
            }
        }
        // every context and every close() funnel into the one shared rollback
        assertEquals(rollbacks.get(), 1);
    }

    // The insert context of a MERGE into a partitioned table chunks its rows per partition key,
    // computed over the data columns it receives (the row id slot is dropped first): k is column 0
    // of an inserted row here but column 1 of the table
    @Test
    public void testInsertsIntoPartitionedTableAreChunkedByPartitionKey()
            throws Exception
    {
        stubClient(RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> writer = partitionedMergeFactory(
                Expressions.identity("k")).createWriter(0, 7L);
        writer.insert(row(null, 1, UTF8String.fromString("a")));
        writer.insert(row(null, 2, UTF8String.fromString("b")));
        writer.update(null, row(Decimal.apply(3L)), row(Decimal.apply(3L), 2, UTF8String.fromString("c")));
        writer.insert(row(null, 1, UTF8String.fromString("d")));
        writer.insert(row(null, 2, UTF8String.fromString("e")));
        writer.delete(null, row(Decimal.apply(4L)));
        writer.insert(row(null, 1, UTF8String.fromString("f")));
        writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);

        // k=1 fills a chunk with the first and the third inserted row, k=2 with the second and
        // the fourth; the last k=1 row is flushed at commit
        assertEquals(insertedKeyChunks(),
                chunks(List.of(List.of(1, 1), List.of(2, 2), List.of(1))), "rpcs: " + rpcs);
        List<Rpc> inserts = rpcs("insert");
        assertEquals(totalRows(inserts), 5, "inserts: " + inserts);
        for (Rpc insert : inserts) {
            assertEquals(insert.path, TABLE_PATH);
            assertTrue(DATA_COLUMNS.containsAll(insert.fields), "insert: " + insert);
        }
        // updates and deletes are keyed by the row id and not grouped
        assertEquals(rpcs("update").size(), 1, "updates: " + rpcs("update"));
        assertEquals(rpcs("update").get(0).firstColumn, List.of(3L));
        assertEquals(rpcs("delete").size(), 1, "deletes: " + rpcs("delete"));
        assertEquals(rpcs("delete").get(0).firstColumn, List.of(4L));
    }

    // With a transform the rows are chunked by the transformed key: two keys of one bucket share
    // a chunk, a key of another bucket does not
    @Test
    public void testInsertsIntoBucketPartitionedTableAreChunkedByBucket()
            throws Exception
    {
        int buckets = 4;
        BoundBucketFunction.BucketInt bucket = new BoundBucketFunction.BucketInt(buckets,
                DataTypes.IntegerType);
        int a = 1;
        int b = 2;
        while (!bucket.produceResult(row(b)).equals(bucket.produceResult(row(a)))) {
            b++;
        }
        int c = 2;
        while (bucket.produceResult(row(c)).equals(bucket.produceResult(row(a)))) {
            c++;
        }
        stubClient(RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> writer = partitionedMergeFactory(
                Expressions.bucket(buckets, "k")).createWriter(0, 7L);
        writer.insert(row(null, a, UTF8String.fromString("a")));
        writer.insert(row(null, c, UTF8String.fromString("c")));
        writer.insert(row(null, b, UTF8String.fromString("b")));
        writer.insert(row(null, c, UTF8String.fromString("c")));
        writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);
        assertEquals(insertedKeyChunks(), chunks(List.of(List.of(a, b), List.of(c, c))),
                "a=" + a + " b=" + b + " c=" + c + " rpcs: " + rpcs);
        assertEquals(totalRows(rpcs("insert")), 4, "inserts: " + rpcs("insert"));
    }

    // A full chunk still in flight at commit belongs to the background writer. flushQueues()
    // used to close the Arrow root of every partition context it stole from, including a root
    // already handed over, and the chunk was then serialized from freed buffers as NULL rows
    @Test
    public void testChunkInFlightAtCommitIsNotClosedUnderTheBackgroundWriter()
            throws Exception
    {
        stubClient(RowIDStrategyType.DECIMAL_128);
        slowInserts(300, RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> writer = partitionedMergeFactory(
                Expressions.identity("k")).createWriter(0, 7L);
        // two full chunks and no remainder, committed right away: the second chunk is
        // serialized only after commit() ran
        writer.insert(row(null, 1, UTF8String.fromString("a")));
        writer.insert(row(null, 1, UTF8String.fromString("b")));
        writer.insert(row(null, 2, UTF8String.fromString("c")));
        writer.insert(row(null, 2, UTF8String.fromString("d")));
        writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);
        assertEquals(insertedKeyChunks(), chunks(List.of(List.of(1, 1), List.of(2, 2))),
                "rpcs: " + rpcs);
        assertEquals(totalRows(rpcs("insert")), 4, "inserts: " + rpcs("insert"));
    }

    // The same hand-off serves a plain INSERT, whose rows all share one partition context
    @Test
    public void testPlainInsertChunkInFlightAtCommitIsNotClosedUnderTheBackgroundWriter()
            throws Exception
    {
        stubClient(RowIDStrategyType.UNSIGNED_INT64);
        slowInserts(300, RowIDStrategyType.UNSIGNED_INT64);
        DeltaWriter<InternalRow> writer = insertFactory().createWriter(0, 7L);
        writer.write(row(1, UTF8String.fromString("a")));
        writer.write(row(1, UTF8String.fromString("b")));
        writer.write(row(2, UTF8String.fromString("c")));
        writer.write(row(2, UTF8String.fromString("d")));
        writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);
        assertEquals(insertedKeyChunks(), chunks(List.of(List.of(1, 1), List.of(2, 2))),
                "rpcs: " + rpcs);
        assertEquals(totalRows(rpcs("insert")), 4, "inserts: " + rpcs("insert"));
    }

    // On a partitioned table the deletes and updates of a merge are grouped by the partition
    // their row id names, sorted within it, and never share a request across partitions, at
    // commit included (the remainder of a partition is flushed on its own)
    @Test
    public void testDeletesAndUpdatesOnPartitionedTableGoOutPerPartition()
            throws Exception
    {
        stubClient(RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> writer = partitionedMergeFactory(
                Expressions.identity("k")).createWriter(0, 7L);
        writer.delete(null, row(rowId(1, 4)));
        writer.update(null, row(rowId(2, 6)), row(rowId(2, 6), 2, UTF8String.fromString("f")));
        writer.delete(null, row(rowId(2, 1)));
        writer.update(null, row(rowId(1, 8)), row(rowId(1, 8), 1, UTF8String.fromString("h")));
        writer.delete(null, row(rowId(1, 2)));
        writer.update(null, row(rowId(2, 5)), row(rowId(2, 5), 2, UTF8String.fromString("e")));
        writer.delete(null, row(rowId(2, 3)));
        writer.update(null, row(rowId(1, 7)), row(rowId(1, 7), 1, UTF8String.fromString("g")));
        writer.delete(null, row(rowId(1, 9)));
        writer.insert(row(null, 3, UTF8String.fromString("i")));
        WriterCommitMessage message = writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);
        assertNotNull(message);

        assertEquals(rowIdRequests("delete"),
                List.of("[1]:[2, 4]", "[1]:[9]", "[2]:[1, 3]"), "deletes: " + rpcs("delete"));
        assertEquals(rowIdRequests("update"),
                List.of("[1]:[7, 8]", "[2]:[5, 6]"), "updates: " + rpcs("update"));
        for (Rpc rpc : rpcs("delete")) {
            assertEquals(rpc.fields, List.of(ROW_ID_FIELD_NAME), "delete: " + rpc);
            assertEquals(rpc.types, List.of(new ArrowType.Decimal(38, 0, 128)), "delete: " + rpc);
        }
        for (Rpc rpc : rpcs("update")) {
            assertEquals(rpc.fields, List.of(ROW_ID_FIELD_NAME, "k", "v"), "update: " + rpc);
        }
        assertEquals(totalRows(rpcs("insert")), 1, "inserts: " + rpcs("insert"));
    }

    // The same grouping serves a plain DELETE on a partitioned table
    @Test
    public void testPlainDeleteOnPartitionedTableGoesOutPerPartition()
            throws Exception
    {
        stubClient(RowIDStrategyType.DECIMAL_128);
        DeltaWriter<InternalRow> writer = partitionedDeleteFactory().createWriter(0, 7L);
        writer.write(row(rowId(2, 1)));
        writer.write(row(rowId(1, 4)));
        writer.write(row(rowId(1, 2)));
        writer.write(row(rowId(2, 3)));
        writer.write(row(rowId(1, 9)));
        WriterCommitMessage message = writer.commit();
        writer.close();
        assertEquals(rollbacks.get(), 0);
        assertTrue(message.toString().contains("writtenRows=5"), "commit message: " + message);
        assertEquals(rowIdRequests("delete"),
                List.of("[1]:[2, 4]", "[1]:[9]", "[2]:[1, 3]"), "deletes: " + rpcs("delete"));
    }
}
