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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * which RPC every kind of row ends up in, with which Arrow schema.
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

        Rpc(String kind, String path, org.apache.arrow.vector.types.pojo.Schema schema,
                int rows, List<Object> firstColumn)
        {
            this.kind = kind;
            this.path = path;
            this.fields = schema.getFields().stream().map(Field::getName).collect(
                    Collectors.toList());
            this.types = schema.getFields().stream().map(Field::getType).collect(
                    Collectors.toList());
            this.rows = rows;
            this.firstColumn = firstColumn;
        }

        @Override
        public String toString()
        {
            return kind + path + fields + types + "x" + rows + firstColumn;
        }
    }

    private VastClient client;
    private VastTransaction tx;
    private List<Rpc> rpcs;
    private AtomicInteger rollbacks;

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
                    root.getSchema(), root.getRowCount(), firstColumnValues(root)));
            return null;
        }).when(client).deleteRows(any(), anyString(), anyString(),
                any(VectorSchemaRoot.class), any(URI.class), any(),
                any(QueryDataExtraParams.class), nullable(String.class));
        doAnswer(invocation -> {
            VectorSchemaRoot root = invocation.getArgument(3);
            rpcs.add(new Rpc("update",
                    "/" + invocation.getArgument(1) + "/" + invocation.getArgument(2),
                    root.getSchema(), root.getRowCount(), firstColumnValues(root)));
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

    private int recordIpc(String kind, String path, byte[] body)
            throws IOException
    {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(
                        new ByteArrayInputStream(body), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int rows = 0;
            while (reader.loadNextBatch()) {
                rows += root.getRowCount();
            }
            rpcs.add(new Rpc(kind, path, root.getSchema(), rows, List.of()));
            return rows;
        }
    }

    private VastWriteFactory mergeFactory(boolean sorted)
    {
        StructField rowIdField = sorted ?
                SPARK_DEC128_ROW_ID_FIELD :
                SPARK_INT64_ROW_ID_FIELD;
        StructType schema = new StructType(new StructField[] {rowIdField,
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("v", DataTypes.StringType, true)});
        Map<String, String> properties = sorted ?
                ImmutableMap.of(SORTED_BY_PROPERTY, "k") :
                Map.of();
        VastTable table = new VastTable(null, SCHEMA_NAME, TABLE_NAME, "handle",
                schema, new Transform[0], () -> client, false,
                Optional.empty(), properties);
        table.getTableMD().setForMerge();
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
}
