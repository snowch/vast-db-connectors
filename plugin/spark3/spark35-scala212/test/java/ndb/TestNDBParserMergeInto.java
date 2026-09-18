/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.spark.CommonSparkTestUtils;
import com.vastdata.spark.SparkTestUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.InsertStarAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.catalyst.plans.logical.UpdateStarAction;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.SparkSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

import static ndb.view.NDBTablesResolutionRule.VAST_THROW_RCLS_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Listeners(CommonSparkTestUtils.TestListener.class)
public class TestNDBParserMergeInto
{
    private static final String ROW_LEVEL_OP_TARGET = "tgt VAST_DB_ROW_LEVEL_OP" + VAST_THROW_RCLS_ERROR;
    private static final String RCLS_SOURCE = "src" + VAST_THROW_RCLS_ERROR;

    private SparkSession session;
    private NDBParser parser;

    private static List<String> identifier(UnresolvedRelation relation)
    {
        Seq<String> parts = relation.multipartIdentifier();
        List<String> result = new ArrayList<>(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            result.add(parts.apply(i));
        }
        return result;
    }

    private static UnresolvedRelation relationOf(LogicalPlan plan)
    {
        if (plan instanceof SubqueryAlias) {
            return relationOf(((SubqueryAlias) plan).child());
        }
        assertTrue(plan instanceof UnresolvedRelation,
                "Expected an UnresolvedRelation but got: " + plan);
        return (UnresolvedRelation) plan;
    }

    private static MergeIntoTable mergeOf(LogicalPlan plan)
    {
        if (plan instanceof UnresolvedWith) {
            return mergeOf(((UnresolvedWith) plan).child());
        }
        assertTrue(plan instanceof MergeIntoTable,
                "Expected a MergeIntoTable but got: " + plan);
        return (MergeIntoTable) plan;
    }

    @BeforeClass
    public void setup()
    {
        session = SparkTestUtils.getSession(1234);
        parser = new NDBParser(session, new SparkSqlParser());
    }

    @AfterClass
    public void tearDown()
    {
        session.close();
    }

    @Test
    public void testAliasedTargetAndVastSource()
            throws ParseException
    {
        MergeIntoTable merge = mergeOf(parser.parsePlan(
                "MERGE INTO ndb.buck.schem.tgt AS t USING ndb.buck.schem.src AS s ON t.k = s.k " +
                        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"));
        assertTrue(merge.targetTable() instanceof NDBMergeTarget,
                "target: " + merge.targetTable());
        LogicalPlan target = ((NDBMergeTarget) merge.targetTable()).child();
        assertTrue(target instanceof SubqueryAlias);
        assertEquals(((SubqueryAlias) target).alias(), "t");
        assertEquals(identifier(relationOf(target)),
                List.of("ndb", "buck", "schem", ROW_LEVEL_OP_TARGET));
        // the source gets the regular (SELECT) security treatment only
        assertTrue(merge.sourceTable() instanceof SubqueryAlias);
        assertEquals(((SubqueryAlias) merge.sourceTable()).alias(), "s");
        assertEquals(identifier(relationOf(merge.sourceTable())),
                List.of("ndb", "buck", "schem", RCLS_SOURCE));
        // star actions are left for the resolution rule to expand
        assertTrue(merge.matchedActions().apply(0) instanceof UpdateStarAction);
        assertTrue(merge.notMatchedActions().apply(0) instanceof InsertStarAction);
    }

    @Test
    public void testUnaliasedTarget()
            throws ParseException
    {
        MergeIntoTable merge = mergeOf(parser.parsePlan(
                "MERGE INTO ndb.buck.schem.tgt USING ndb.buck.schem.src ON tgt.k = src.k " +
                        "WHEN MATCHED THEN DELETE"));
        assertTrue(merge.targetTable() instanceof NDBMergeTarget);
        LogicalPlan target = ((NDBMergeTarget) merge.targetTable()).child();
        assertTrue(target instanceof UnresolvedRelation);
        assertEquals(identifier((UnresolvedRelation) target),
                List.of("ndb", "buck", "schem", ROW_LEVEL_OP_TARGET));
        assertEquals(identifier(relationOf(merge.sourceTable())),
                List.of("ndb", "buck", "schem", RCLS_SOURCE));
    }

    @Test
    public void testSubquerySourceRelationsGetOnlySecurityTreatment()
            throws ParseException
    {
        MergeIntoTable merge = mergeOf(parser.parsePlan(
                "MERGE INTO ndb.buck.schem.tgt t USING (SELECT * FROM ndb.buck.schem.src) s ON t.k = s.k " +
                        "WHEN MATCHED THEN UPDATE SET v = s.v"));
        assertTrue(merge.targetTable() instanceof NDBMergeTarget);
        List<UnresolvedRelation> sourceRelations = new ArrayList<>();
        merge.sourceTable().foreach(p -> {
            if (p instanceof UnresolvedRelation) {
                sourceRelations.add((UnresolvedRelation) p);
            }
            return null;
        });
        assertEquals(sourceRelations.size(), 1);
        assertEquals(identifier(sourceRelations.get(0)),
                List.of("ndb", "buck", "schem", RCLS_SOURCE));
    }

    @Test
    public void testCteSourceIsNotSuffixed()
            throws ParseException
    {
        LogicalPlan plan = parser.parsePlan(
                "WITH s AS (SELECT * FROM ndb.buck.schem.src) " +
                        "MERGE INTO ndb.buck.schem.tgt t USING s ON t.k = s.k WHEN MATCHED THEN DELETE");
        assertTrue(plan instanceof UnresolvedWith, "plan: " + plan);
        MergeIntoTable merge = mergeOf(plan);
        assertTrue(merge.targetTable() instanceof NDBMergeTarget);
        assertEquals(identifier(relationOf(
                        ((NDBMergeTarget) merge.targetTable()).child())),
                List.of("ndb", "buck", "schem", ROW_LEVEL_OP_TARGET));
        // the CTE reference keeps its name, as for any other statement
        assertEquals(identifier(relationOf(merge.sourceTable())),
                List.of("s"));
    }

    @Test
    public void testInsertOnlyMergeIsLeftToTheRegularInsertPath()
            throws ParseException
    {
        MergeIntoTable merge = mergeOf(parser.parsePlan(
                "MERGE INTO ndb.buck.schem.tgt t USING ndb.buck.schem.src s ON t.k = s.k " +
                        "WHEN NOT MATCHED THEN INSERT *"));
        assertFalse(merge.targetTable() instanceof NDBMergeTarget);
        assertEquals(identifier(relationOf(merge.targetTable())),
                List.of("ndb", "buck", "schem", "tgt" + VAST_THROW_RCLS_ERROR));
        assertEquals(identifier(relationOf(merge.sourceTable())),
                List.of("ndb", "buck", "schem", RCLS_SOURCE));
    }

    @Test
    public void testNotMatchedBySourceNeedsTheRowLevelPath()
            throws ParseException
    {
        MergeIntoTable merge = mergeOf(parser.parsePlan(
                "MERGE INTO ndb.buck.schem.tgt t USING ndb.buck.schem.src s ON t.k = s.k " +
                        "WHEN NOT MATCHED BY SOURCE THEN DELETE"));
        assertTrue(merge.targetTable() instanceof NDBMergeTarget);
    }

    @Test
    public void testDeleteAndUpdateStillSuffixTheirTarget()
            throws ParseException
    {
        LogicalPlan delete = parser.parsePlan(
                "DELETE FROM ndb.buck.schem.tgt WHERE k = 1");
        assertTrue(delete instanceof DeleteFromTable);
        assertEquals(identifier(relationOf(((DeleteFromTable) delete).table())),
                List.of("ndb", "buck", "schem", ROW_LEVEL_OP_TARGET));
        LogicalPlan update = parser.parsePlan(
                "UPDATE ndb.buck.schem.tgt SET v = 'x' WHERE k = 1");
        assertTrue(update instanceof UpdateTable);
        assertEquals(identifier(relationOf(((UpdateTable) update).table())),
                List.of("ndb", "buck", "schem", ROW_LEVEL_OP_TARGET));
    }
}
