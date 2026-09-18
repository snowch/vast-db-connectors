/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.google.common.collect.ImmutableMap;
import com.vastdata.client.error.VastRuntimeException;
import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.VastTable;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction;
import org.apache.spark.sql.catalyst.plans.logical.InsertAction;
import org.apache.spark.sql.catalyst.plans.logical.InsertStarAction;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction;
import org.apache.spark.sql.catalyst.plans.logical.UpdateStarAction;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.Builder;

import java.util.Map;
import java.util.Optional;

import static com.vastdata.client.schema.VastMetadataUtils.SORTED_BY_PROPERTY;
import static com.vastdata.spark.SparkPlannerUtil.getEmptyInternalRowSeq;
import static com.vastdata.spark.SparkPlannerUtil.getEmptyStringSeq;
import static ndb.SparkPlannerUtil.newDataSourceV2Relation;
import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_DEC128_ROW_ID_FIELD;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_INT64_ROW_ID_FIELD;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestNDBRowLevelResolutionRuleMerge
{
    private static final String[] NAMESPACE = {"buck", "schem"};
    private static final String SUFFIXED_TARGET_NAME = "tgt VAST_DB_ROW_LEVEL_OP" + VAST_THROW_RCLS_ERROR;

    private final NDBRowLevelResolutionRule rule = new NDBRowLevelResolutionRule();

    private static AttributeReference attr(String name, DataType type)
    {
        return new AttributeReference(name, type, true, Metadata.empty(),
                NamedExpression.newExprId(), getEmptyStringSeq());
    }

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

    private static VastTable table(StructField rowIdField, Transform[] partitioning,
            Map<String, String> properties)
    {
        StructType schema = new StructType(new StructField[] {rowIdField,
                createStructField("k", DataTypes.IntegerType, true),
                createStructField("v", DataTypes.StringType, true)});
        return new VastTable(null, "buck/schem", "tgt", "handle", schema,
                partitioning, () -> null, false, Optional.empty(), properties);
    }

    private static VastTable int64Table()
    {
        return table(SPARK_INT64_ROW_ID_FIELD, new Transform[0], Map.of());
    }

    private static VastTable dec128Table()
    {
        return table(SPARK_DEC128_ROW_ID_FIELD, new Transform[0],
                ImmutableMap.of(SORTED_BY_PROPERTY, "k"));
    }

    private static VastTable partitionedTable()
    {
        return table(SPARK_DEC128_ROW_ID_FIELD,
                new Transform[] {Expressions.identity("k")}, Map.of());
    }

    // Shape of a target resolved by Spark: SubqueryAlias(lookup identifier, relation)
    private static SubqueryAlias resolvedTarget(VastTable table)
    {
        DataSourceV2Relation relation = newDataSourceV2Relation(table, null,
                NAMESPACE, "tgt");
        return new SubqueryAlias(
                new AliasIdentifier(SUFFIXED_TARGET_NAME, qualifier()),
                relation);
    }

    private static LocalRelation source(Attribute... attrs)
    {
        return new LocalRelation(seq(attrs), getEmptyInternalRowSeq(), false);
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

    private static MergeIntoTable merge(LogicalPlan target, LogicalPlan source,
            Seq<MergeAction> matched, Seq<MergeAction> notMatched,
            Seq<MergeAction> notMatchedBySource)
    {
        Expression condition = new EqualTo(
                new UnresolvedAttribute(seq("t", "k")),
                new UnresolvedAttribute(seq("s", "k")));
        return new MergeIntoTable(target, source, condition, matched,
                notMatched, notMatchedBySource);
    }

    private static void assertRowIdPlaceholder(Assignment assignment,
            LogicalPlan target, String rowIdName)
    {
        Attribute rowId = output(target, rowIdName);
        assertTrue(assignment.key().semanticEquals(rowId), "key of " + assignment);
        assertTrue(assignment.value() instanceof Literal, "value of " + assignment);
        Literal value = (Literal) assignment.value();
        assertTrue(value.value() != null, "value of " + assignment);
        assertEquals(value.dataType(), rowId.dataType());
    }

    private static void assertStarAssignments(Seq<Assignment> assignments,
            LogicalPlan target, LogicalPlan source, int expectedCount)
    {
        assertEquals(assignments.size(), expectedCount, "assignments: " + assignments);
        for (String column : new String[] {"k", "v"}) {
            Assignment assignment = column.equals("k") ?
                    assignments.apply(0) :
                    assignments.apply(1);
            assertTrue(assignment.key().semanticEquals(output(target, column)),
                    "key of " + assignment);
            assertTrue(assignment.value().semanticEquals(output(source, column)),
                    "value of " + assignment);
        }
    }

    private void assertStarExpansion(VastTable table, String rowIdName)
    {
        SubqueryAlias target = resolvedTarget(table);
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType),
                attr("extra", DataTypes.StringType));
        Option<Expression> insertCondition = Option.apply(
                new EqualTo(new UnresolvedAttribute(seq("s", "extra")),
                        new Literal(null, DataTypes.StringType)));
        MergeIntoTable plan = merge(new NDBMergeTarget(target), source,
                seq(new UpdateStarAction(Option.empty())),
                seq(new InsertStarAction(insertCondition)), seq());

        LogicalPlan result = rule.apply(plan);

        assertTrue(result instanceof MergeIntoTable, "result: " + result);
        MergeIntoTable resolved = (MergeIntoTable) result;
        // marker removed, alias restored to the plain table name
        assertTrue(resolved.targetTable() instanceof SubqueryAlias);
        SubqueryAlias resolvedTarget = (SubqueryAlias) resolved.targetTable();
        assertEquals(resolvedTarget.alias(), "tgt");
        assertEquals(resolvedTarget.identifier().qualifier(), qualifier());
        assertSame(resolvedTarget.child(), target.child());
        assertEquals(output(resolvedTarget, rowIdName).name(), rowIdName);
        // stars expanded over the data columns only
        MergeAction matched = resolved.matchedActions().apply(0);
        assertTrue(matched instanceof UpdateAction, "matched: " + matched);
        assertTrue(((UpdateAction) matched).condition().isEmpty());
        // updates keep the row id, Spark aligns it to itself
        assertStarAssignments(((UpdateAction) matched).assignments(), target,
                source, 2);
        MergeAction notMatched = resolved.notMatchedActions().apply(0);
        assertTrue(notMatched instanceof InsertAction,
                "not matched: " + notMatched);
        assertEquals(((InsertAction) notMatched).condition(), insertCondition);
        // inserts get a placeholder for the row id, which the writer drops
        Seq<Assignment> insertAssignments = ((InsertAction) notMatched).assignments();
        assertStarAssignments(insertAssignments, target, source, 3);
        assertRowIdPlaceholder(insertAssignments.apply(2), target, rowIdName);
        assertTrue(resolved.notMatchedBySourceActions().isEmpty());
        assertSame(resolved.sourceTable(), source);
    }

    // Shape of a target resolved by the connector: NDBTablesResolutionRule aliases the relation
    // with its plain table name, there is nothing to clean up
    @Test
    public void testConnectorResolvedTargetKeepsItsAlias()
    {
        DataSourceV2Relation relation = newDataSourceV2Relation(int64Table(), null,
                NAMESPACE, "tgt");
        SubqueryAlias target = new SubqueryAlias(
                new AliasIdentifier("tgt", qualifier()), relation);
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        MergeIntoTable resolved = (MergeIntoTable) rule.apply(
                merge(new NDBMergeTarget(target), source,
                        seq(new UpdateStarAction(Option.empty())), seq(), seq()));
        assertSame(resolved.targetTable(), target);
        Attribute k = output(resolved.targetTable(), "k");
        assertEquals(k.qualifier().last(), "tgt", "qualifier of " + k);
        assertStarAssignments(((UpdateAction) resolved.matchedActions().apply(0)).assignments(),
                target, source, 2);
    }

    @Test
    public void testStarExpansionInt64RowId()
    {
        assertStarExpansion(int64Table(), SPARK_INT64_ROW_ID_FIELD.name());
    }

    @Test
    public void testStarExpansionDec128RowId()
    {
        assertStarExpansion(dec128Table(), SPARK_DEC128_ROW_ID_FIELD.name());
    }

    @Test
    public void testStarExpansionIsCaseInsensitiveByDefault()
    {
        SubqueryAlias target = resolvedTarget(int64Table());
        LocalRelation source = source(attr("K", DataTypes.IntegerType),
                attr("V", DataTypes.StringType));
        MergeIntoTable resolved = (MergeIntoTable) rule.apply(
                merge(new NDBMergeTarget(target), source,
                        seq(new UpdateStarAction(Option.empty())), seq(),
                        seq()));
        Seq<Assignment> assignments = ((UpdateAction) resolved
                .matchedActions()
                .apply(0)).assignments();
        assertEquals(assignments.size(), 2);
        assertTrue(assignments.apply(0).value().semanticEquals(
                output(source, "K")));
        assertTrue(assignments.apply(1).value().semanticEquals(
                output(source, "V")));
    }

    @Test
    public void testMissingSourceColumnIsReported()
    {
        SubqueryAlias target = resolvedTarget(int64Table());
        LocalRelation source = source(attr("k", DataTypes.IntegerType));
        MergeIntoTable plan = merge(new NDBMergeTarget(target), source, seq(),
                seq(new InsertStarAction(Option.empty())), seq());
        assertThatThrownBy(() -> rule.apply(plan))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining("target column v cannot be resolved in the source columns [k]");
    }

    @Test
    public void testExplicitActionsPassThroughAndMarkerIsRemoved()
    {
        SubqueryAlias target = resolvedTarget(int64Table());
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        UpdateAction update = new UpdateAction(Option.empty(),
                seq(new Assignment(new UnresolvedAttribute(seq("v")),
                        new UnresolvedAttribute(seq("s", "v")))));
        DeleteAction delete = new DeleteAction(Option.empty());
        InsertAction insert = new InsertAction(Option.empty(),
                seq(new Assignment(new UnresolvedAttribute(seq("k")),
                        new UnresolvedAttribute(seq("s", "k")))));
        MergeIntoTable resolved = (MergeIntoTable) rule.apply(
                merge(new NDBMergeTarget(target), source, seq(delete, update),
                        seq(insert), seq(delete)));
        assertTrue(resolved.targetTable() instanceof SubqueryAlias);
        assertSame(resolved.matchedActions().apply(0), delete);
        assertSame(resolved.matchedActions().apply(1), update);
        assertSame(resolved.notMatchedBySourceActions().apply(0), delete);
        // explicit inserts keep their assignments and get the row id placeholder
        InsertAction resolvedInsert = (InsertAction) resolved.notMatchedActions().apply(0);
        assertEquals(resolvedInsert.condition(), insert.condition());
        assertEquals(resolvedInsert.assignments().size(), 2);
        assertSame(resolvedInsert.assignments().apply(0), insert.assignments().apply(0));
        assertRowIdPlaceholder(resolvedInsert.assignments().apply(1), target,
                SPARK_INT64_ROW_ID_FIELD.name());
    }

    @Test
    public void testAssignmentToRowIdIsRefused()
    {
        SubqueryAlias target = resolvedTarget(int64Table());
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        UpdateAction update = new UpdateAction(Option.empty(), seq(
                new Assignment(new UnresolvedAttribute(
                        seq(SPARK_INT64_ROW_ID_FIELD.name())),
                        new Literal(5L, DataTypes.LongType))));
        assertThatThrownBy(() -> rule.apply(
                merge(new NDBMergeTarget(target), source, seq(update), seq(),
                        seq())))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining("Assigning a value to " + SPARK_INT64_ROW_ID_FIELD.name() + " is not allowed");
        // same with an already resolved key (e.g. INSERT (rowid, k, v))
        InsertAction insert = new InsertAction(Option.empty(), seq(
                new Assignment(output(target, SPARK_INT64_ROW_ID_FIELD.name()),
                        new Literal(5L, DataTypes.LongType))));
        assertThatThrownBy(() -> rule.apply(
                merge(new NDBMergeTarget(target), source, seq(), seq(insert),
                        seq())))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining("is not allowed");
    }

    @Test
    public void testInsertIntoPartitionedTableIsRefused()
    {
        SubqueryAlias target = resolvedTarget(partitionedTable());
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        assertThatThrownBy(() -> rule.apply(
                merge(new NDBMergeTarget(target), source,
                        seq(new UpdateStarAction(Option.empty())),
                        seq(new InsertStarAction(Option.empty())), seq())))
                .isInstanceOf(VastRuntimeException.class)
                .hasMessageContaining("not supported on partitioned table");
    }

    @Test
    public void testUpdateAndDeleteOnPartitionedTableAreAllowed()
    {
        SubqueryAlias target = resolvedTarget(partitionedTable());
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        MergeIntoTable resolved = (MergeIntoTable) rule.apply(
                merge(new NDBMergeTarget(target), source,
                        seq(new UpdateStarAction(Option.empty())), seq(),
                        seq(new DeleteAction(Option.empty()))));
        assertTrue(resolved.targetTable() instanceof SubqueryAlias);
        assertTrue(resolved.matchedActions().apply(0) instanceof UpdateAction);
    }

    @Test
    public void testWaitsForTargetAndSourceResolution()
    {
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        UnresolvedRelation unresolvedTarget = new UnresolvedRelation(
                seq("ndb", "buck", "schem", SUFFIXED_TARGET_NAME),
                CaseInsensitiveStringMap.empty(), false);
        MergeIntoTable plan = merge(new NDBMergeTarget(unresolvedTarget),
                source, seq(new UpdateStarAction(Option.empty())), seq(),
                seq());
        assertSame(rule.apply(plan), plan);

        UnresolvedRelation unresolvedSource = new UnresolvedRelation(
                seq("ndb", "buck", "schem", "src"),
                CaseInsensitiveStringMap.empty(), false);
        MergeIntoTable plan2 = merge(
                new NDBMergeTarget(resolvedTarget(int64Table())),
                unresolvedSource, seq(new UpdateStarAction(Option.empty())),
                seq(), seq());
        assertSame(rule.apply(plan2), plan2);
    }

    @Test
    public void testMergeWithoutMarkerIsLeftAlone()
    {
        SubqueryAlias target = resolvedTarget(int64Table());
        LocalRelation source = source(attr("k", DataTypes.IntegerType),
                attr("v", DataTypes.StringType));
        MergeIntoTable plan = merge(target, source,
                seq(new UpdateStarAction(Option.empty())), seq(), seq());
        assertSame(rule.apply(plan), plan);
    }
}
