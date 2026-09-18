/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_DEC128_ROW_ID_FIELD;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_INT64_ROW_ID_FIELD;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestVastRowLevelOperationBuilder
{
    private static final StructType SCHEMA = new StructType(
            new StructField[] {SPARK_INT64_ROW_ID_FIELD,
                    createStructField("k", DataTypes.IntegerType, true),
                    createStructField("v", DataTypes.StringType, true)});

    private static VastTable table(Map<String, String> properties)
    {
        return new VastTable(null, "buck/schem", "tab", "handle", SCHEMA,
                new Transform[0], () -> null, false, Optional.empty(),
                properties);
    }

    private static RowLevelOperationInfo info(RowLevelOperation.Command command)
    {
        RowLevelOperationInfo info = mock(RowLevelOperationInfo.class);
        when(info.command()).thenReturn(command);
        when(info.options()).thenReturn(CaseInsensitiveStringMap.empty());
        return info;
    }

    @Test
    public void testBuildMerge()
    {
        VastTable table = table(Map.of());
        RowLevelOperation operation = new VastRowLevelOperationBuilder(table,
                info(RowLevelOperation.Command.MERGE)).build();
        assertTrue(operation instanceof RowLevelMerge);
        assertEquals(operation.command(), RowLevelOperation.Command.MERGE);
        assertTrue(table.getTableMD().isForMerge());
        assertFalse(table.getTableMD().isForDelete());
        assertFalse(table.getTableMD().isForUpdate());
        assertEquals(((RowLevelMerge) operation).rowId()[0].fieldNames()[0],
                SPARK_INT64_ROW_ID_FIELD.name());
    }

    @Test
    public void testBuildMergeSortedTableUsesDec128RowId()
    {
        VastTable table = table(ImmutableMap.of(SORTED_BY_PROPERTY, "k"));
        RowLevelOperation operation = new VastRowLevelOperationBuilder(table,
                info(RowLevelOperation.Command.MERGE)).build();
        assertTrue(operation instanceof RowLevelMerge);
        assertEquals(((RowLevelMerge) operation).rowId()[0].fieldNames()[0],
                SPARK_DEC128_ROW_ID_FIELD.name());
    }

    @Test
    public void testBuildDeleteUnchanged()
    {
        VastTable table = table(Map.of());
        RowLevelOperation operation = new VastRowLevelOperationBuilder(table,
                info(RowLevelOperation.Command.DELETE)).build();
        assertTrue(operation instanceof RowLevelDelete);
        assertEquals(operation.command(), RowLevelOperation.Command.DELETE);
        assertTrue(table.getTableMD().isForDelete());
        assertFalse(table.getTableMD().isForUpdate());
        assertFalse(table.getTableMD().isForMerge());
    }

    @Test
    public void testBuildUpdateUnchanged()
    {
        VastTable table = table(Map.of());
        RowLevelOperation operation = new VastRowLevelOperationBuilder(table,
                info(RowLevelOperation.Command.UPDATE)).build();
        assertTrue(operation instanceof RowLevelUpdate);
        assertEquals(operation.command(), RowLevelOperation.Command.UPDATE);
        assertTrue(table.getTableMD().isForUpdate());
        assertFalse(table.getTableMD().isForDelete());
        assertFalse(table.getTableMD().isForMerge());
    }
}
