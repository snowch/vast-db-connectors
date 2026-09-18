/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import java.util.Map;
import java.util.Optional;

import static ndb.SparkPlannerUtil.newAlias;
import static ndb.SparkPlannerUtil.newDataSourceV2Relation;
import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestNDBRCLSResolvedRelationAdaptorRule
{
    private static final String[] NAMESPACE = {"buck", "schem"};
    // The identifiers a VAST relation is looked up by: the RCLS suffix in every statement, the
    // row level operation suffix as well for the target of DELETE / UPDATE / MERGE. Spark aliases
    // a relation it resolves itself with the lookup identifier.
    private static final String LOOKUP_NAME = "tgt" + VAST_THROW_RCLS_ERROR;
    private static final String ROW_LEVEL_OP_LOOKUP_NAME = "tgt VAST_DB_ROW_LEVEL_OP" + VAST_THROW_RCLS_ERROR;
    private static final String DELETE_REFUSED = "Delete from table is not allowed by current VAST security policy rules";
    private static final String UPDATE_REFUSED = "Update table is not allowed by current VAST security policy rules";

    private final NDBRCLSResolvedRelationAdaptorRule rule = new NDBRCLSResolvedRelationAdaptorRule();

    @SafeVarargs
    private static <T> List<T> seq(T... items)
    {
        Builder<T, List<T>> builder = List$.MODULE$.newBuilder();
        for (T item : items) {
            builder.$plus$eq(item);
        }
        return builder.result();
    }

    private static Seq<String> qualifier()
    {
        return seq("ndb", "buck", "schem");
    }

    private static DataSourceV2Relation relation(String lookupName)
    {
        StructType schema = new StructType(new StructField[] {
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("v", DataTypes.StringType, true)});
        VastTable table = new VastTable(null, "buck/schem", "tgt", "handle", schema,
                new Transform[0], () -> null, false, Optional.empty(), Map.of());
        return newDataSourceV2Relation(table, null, NAMESPACE, lookupName);
    }

    private static SubqueryAlias alias(String name, LogicalPlan child)
    {
        return new SubqueryAlias(new AliasIdentifier(name, qualifier()), child);
    }

    private static Attribute output(LogicalPlan plan, String name)
    {
        Seq<Attribute> output = plan.output();
        for (int i = 0; i < output.size(); i++) {
            if (output.apply(i).name().equals(name)) {
                return output.apply(i);
            }
        }
        throw new IllegalArgumentException(name);
    }

    private static Expression keyEquals(DataSourceV2Relation relation, int value)
    {
        return new EqualTo(output(relation, "k"),
                new Literal(value, DataTypes.IntegerType));
    }

    // the shape NDBTablesResolutionRule gives a table with a row filter
    private static Filter rowFilter(DataSourceV2Relation relation)
    {
        return new Filter(new GreaterThan(output(relation, "k"),
                new Literal(10, DataTypes.IntegerType)), relation);
    }

    // the shape NDBTablesResolutionRule gives a table with a column mask
    private static Project columnMask(DataSourceV2Relation relation)
    {
        return new Project(TestNDBRCLSResolvedRelationAdaptorRule.<NamedExpression>seq(
                output(relation, "k"),
                newAlias(new Literal(null, DataTypes.StringType), "v")), relation);
    }

    private static UpdateTable update(LogicalPlan target, DataSourceV2Relation relation)
    {
        return new UpdateTable(target,
                seq(new Assignment(output(relation, "v"),
                        new Literal(null, DataTypes.StringType))),
                Option.apply(keyEquals(relation, 1)));
    }

    @Test
    public void testLookupAliasIsRenamedToThePlainTableName()
    {
        for (String lookupName : new String[] {LOOKUP_NAME, ROW_LEVEL_OP_LOOKUP_NAME}) {
            LogicalPlan result = rule.apply(alias(lookupName, relation(lookupName)));
            assertTrue(result instanceof SubqueryAlias, "result: " + result);
            SubqueryAlias adapted = (SubqueryAlias) result;
            assertEquals(adapted.alias(), "tgt", "alias for " + lookupName);
            assertEquals(adapted.identifier().qualifier(), qualifier());
            assertTrue(adapted.child() instanceof DataSourceV2Relation, "child: " + adapted.child());
            assertEquals(((DataSourceV2Relation) adapted.child()).identifier().get().name(), "tgt");
            assertEquals(output(adapted, "k").qualifier(), seq("ndb", "buck", "schem", "tgt"));
        }
        // a plain alias is left alone
        SubqueryAlias plain = alias("tgt", relation("tgt"));
        assertSame(rule.apply(plain), plain);
    }

    @Test
    public void testDeleteKeepsTheAliasOfItsTarget()
    {
        DataSourceV2Relation relation = relation("tgt");
        DeleteFromTable delete = new DeleteFromTable(alias("tgt", relation),
                keyEquals(relation, 1));
        assertSame(rule.apply(delete), delete);

        DataSourceV2Relation suffixed = relation(ROW_LEVEL_OP_LOOKUP_NAME);
        LogicalPlan result = rule.apply(new DeleteFromTable(
                alias(ROW_LEVEL_OP_LOOKUP_NAME, suffixed), keyEquals(suffixed, 1)));
        assertTrue(result instanceof DeleteFromTable, "result: " + result);
        LogicalPlan target = ((DeleteFromTable) result).table();
        assertTrue(target instanceof SubqueryAlias, "target: " + target);
        assertEquals(((SubqueryAlias) target).alias(), "tgt");
        assertTrue(((SubqueryAlias) target).child() instanceof DataSourceV2Relation,
                "target: " + target);
    }

    @Test
    public void testDeleteMergesTheRowFilterIntoItsConditionAndKeepsTheAlias()
    {
        DataSourceV2Relation relation = relation("tgt");
        Filter filter = rowFilter(relation);
        Expression condition = keyEquals(relation, 1);
        LogicalPlan result = rule.apply(new DeleteFromTable(alias("tgt", filter), condition));
        assertTrue(result instanceof DeleteFromTable, "result: " + result);
        DeleteFromTable delete = (DeleteFromTable) result;
        assertEquals(delete.condition(), new And(condition, filter.condition()));
        assertTrue(delete.table() instanceof SubqueryAlias, "target: " + delete.table());
        assertEquals(((SubqueryAlias) delete.table()).alias(), "tgt");
        assertSame(((SubqueryAlias) delete.table()).child(), relation);

        // a filter directly under the command is merged as before
        DeleteFromTable bare = (DeleteFromTable) rule.apply(
                new DeleteFromTable(filter, condition));
        assertEquals(bare.condition(), new And(condition, filter.condition()));
        assertSame(bare.table(), relation);
    }

    @Test
    public void testDeleteAndUpdateRefuseRowFiltersAndColumnMasksUnderTheAlias()
    {
        DataSourceV2Relation relation = relation("tgt");
        Expression condition = keyEquals(relation, 1);
        assertThatThrownBy(() -> rule.apply(
                new DeleteFromTable(alias("tgt", columnMask(relation)), condition)))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining(DELETE_REFUSED);
        assertThatThrownBy(() -> rule.apply(
                new DeleteFromTable(columnMask(relation), condition)))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining(DELETE_REFUSED);
        assertThatThrownBy(() -> rule.apply(update(alias("tgt", rowFilter(relation)), relation)))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining(UPDATE_REFUSED);
        assertThatThrownBy(() -> rule.apply(update(alias("tgt", columnMask(relation)), relation)))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining(UPDATE_REFUSED);
        UpdateTable plain = update(alias("tgt", relation), relation);
        assertSame(rule.apply(plain), plain);
    }
}
