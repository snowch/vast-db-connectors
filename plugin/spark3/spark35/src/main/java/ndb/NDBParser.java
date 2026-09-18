/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import ndb.view.AlterNDBViewAsPlan;
import ndb.view.CreateNDBViewPlan;
import ndb.view.DropNDBViewPlan;
import ndb.view.RenameNDBViewPlan;
import ndb.view.ShowNDBViewsPlan;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.AlterViewAs;
import org.apache.spark.sql.catalyst.plans.logical.CreateView;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.DropView;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.RenameTable;
import org.apache.spark.sql.catalyst.plans.logical.ShowViews;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.immutable.Seq;

import java.util.HashSet;

import static java.lang.String.format;
import static ndb.SparkPlannerUtil.addVastResolutionSuffixes;
import static ndb.SparkPlannerUtil.removeVastResolutionSuffixes;

public class NDBParser
        implements ParserInterface
{
    private static final Logger LOG = LoggerFactory.getLogger(NDBParser.class);

    private final SparkSession session;
    private final ParserInterface parser;

    public NDBParser(SparkSession sparkSession, ParserInterface parserInterface)
    {
        session = sparkSession;
        parser = parserInterface;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText)
            throws ParseException
    {
        LogicalPlan original = parser.parsePlan(sqlText);
        if (original instanceof DeleteFromTable || original instanceof UpdateTable) {
            LOG.debug("NDBParser.parsePlan original LogicalPlan is {}: {}",
                    original.getClass(), original);
            Function1<LogicalPlan, LogicalPlan> func = p -> {
                if (p instanceof UnresolvedRelation) {
                    UnresolvedRelation unresolvedRel = (UnresolvedRelation) p;
                    return addVastResolutionSuffixes(unresolvedRel, true, true);
                }
                else {
                    return p;
                }
            };
            PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(
                    func);
            LogicalPlan transformed = original.transform(transformer);
            LOG.info("Transformed row level operation plan: {}", transformed);
            return transformed;
        }
        else if (original instanceof ShowViews) {
            LOG.debug(
                    "NDBParser.parsePlan original LogicalPlan is a ShowViews plan");
            return ShowNDBViewsPlan.instance((ShowViews) original);
        }
        else if (original instanceof CreateView) {
            LOG.debug(
                    "NDBParser.parsePlan original LogicalPlan is a CreateView plan, with child: {}",
                    ((CreateView) original).child());
            final String currentCatalog = session
                    .sessionState()
                    .catalogManager()
                    .currentCatalog()
                    .name();
            final String[] currentNamespace = session
                    .sessionState()
                    .catalogManager()
                    .currentNamespace();
            return CreateNDBViewPlan.instance((CreateView) original,
                    currentCatalog, currentNamespace);
        }
        else if (original instanceof DropView) {
            LOG.debug(
                    "NDBParser.parsePlan original LogicalPlan is a DropView plan");
            return DropNDBViewPlan.instance((DropView) original);
        }
        else if (original instanceof AlterViewAs) {
            LOG.debug(
                    "NDBParser.parsePlan original LogicalPlan is an AlterViewAs plan");
            return AlterNDBViewAsPlan.instance((AlterViewAs) original);
        }
        else if (original instanceof RenameTable && ((RenameTable) original).isView()) {
            LOG.debug(
                    "NDBParser.parsePlan original LogicalPlan is an RenameTable(isView=True) plan");
            return RenameNDBViewPlan.instance((RenameTable) original);
        }
        return original.transformUp(PartialFunction.fromFunction(
                withMergeTargetAdaptor(getSecurityWrapper(original))));
    }

    /**
     * Extends the security wrapper so that the target of a MERGE INTO plan is
     * resolved as a row level operation, like the target of DELETE/UPDATE.
     * The plan is transformed bottom-up, so by the time the MergeIntoTable
     * node is visited, every relation in it (target included) has already
     * been through the security wrapper. The target is re-suffixed here,
     * the source side is left with the regular security treatment.
     */
    private Function1<LogicalPlan, LogicalPlan> withMergeTargetAdaptor(
            Function1<LogicalPlan, LogicalPlan> securityWrapper)
    {
        return plan -> {
            if (plan instanceof MergeIntoTable) {
                return adaptMergeIntoTable((MergeIntoTable) plan);
            }
            return securityWrapper.apply(plan);
        };
    }

    private LogicalPlan adaptMergeIntoTable(MergeIntoTable merge)
    {
        if (merge.matchedActions().isEmpty() && merge
                .notMatchedBySourceActions()
                .isEmpty()) {
            // Spark rewrites a MERGE with only WHEN NOT MATCHED actions into a
            // plain AppendData and never asks for a row level operation, so the
            // target must keep its regular (non row level op) resolution
            LOG.debug(
                    "NDBParser.parsePlan leaving insert-only MergeIntoTable unchanged: {}",
                    merge);
            return merge;
        }
        LogicalPlan adaptedTarget = adaptMergeTargetRelation(
                merge.targetTable());
        if (adaptedTarget == null) {
            LOG.warn(
                    "NDBParser.parsePlan unexpected MergeIntoTable target, leaving plan unchanged: {}",
                    merge.targetTable());
            return merge;
        }
        MergeIntoTable adapted = merge.copy(new NDBMergeTarget(adaptedTarget),
                merge.sourceTable(), merge.mergeCondition(),
                merge.matchedActions(), merge.notMatchedActions(),
                merge.notMatchedBySourceActions());
        LOG.info("Transformed merge into table plan: {}", adapted);
        return adapted;
    }

    // Only the target relation itself is adapted, through its alias if any.
    // Returns null when the target is not a (possibly aliased) relation.
    private LogicalPlan adaptMergeTargetRelation(LogicalPlan target)
    {
        if (target instanceof UnresolvedRelation) {
            UnresolvedRelation withoutSuffixes = removeVastResolutionSuffixes(
                    (UnresolvedRelation) target);
            return addVastResolutionSuffixes(withoutSuffixes, true, true);
        }
        else if (target instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) target;
            LogicalPlan adaptedChild = adaptMergeTargetRelation(alias.child());
            return adaptedChild == null ?
                    null :
                    new SubqueryAlias(alias.identifier(), adaptedChild);
        }
        return null;
    }

    private Function1<LogicalPlan, LogicalPlan> getSecurityWrapper(
            LogicalPlan original)
    {
        HashSet<String> cteNames = new HashSet<>();
        if (original instanceof UnresolvedWith) {
            UnresolvedWith unresolvedWith = (UnresolvedWith) original;
            Seq<Tuple2<String, SubqueryAlias>> cteRelations = unresolvedWith.cteRelations();
            cteRelations.foreach(cte -> {
                cteNames.add(cte._1);
                return null;
            });
        }
        return plan -> {
            if (plan instanceof UnresolvedRelation) {
                UnresolvedRelation unresolvedRel = (UnresolvedRelation) plan;
                Seq<String> multipartIdentifier = unresolvedRel.multipartIdentifier();
                if (multipartIdentifier.size() > 1) {
                    String provider = multipartIdentifier.head();
                    try {
                        Class<?> aClass = DataSource.lookupDataSource(provider,
                                session.sessionState().conf());
                        if (!DefaultSource.class.isAssignableFrom(aClass)) {
                            LOG.warn(
                                    "Skipping vast parser wrapper for non-vast provider: {}",
                                    provider);
                            return plan;
                        }
                    }
                    catch (Throwable t) {
                        LOG.warn(
                                format("NDBParser.parsePlan could not find DataSource for provider: %s",
                                        provider), t);
                    }
                }
                StringBuilder sb = new StringBuilder();
                multipartIdentifier.foreach(
                        part -> sb.append(part).append("."));
                String identAsString = sb.substring(0, sb.length() - 1);
                if (cteNames.contains(identAsString)) {
                    LOG.warn(
                            "Skipping vast parser wrapper for CTE identifier: {}",
                            identAsString);
                    return plan;
                }
                else {
                    return addVastResolutionSuffixes(unresolvedRel, false,
                            true);
                }
            }
            return plan;
        };
    }

    @Override
    public Expression parseExpression(String sqlText)
            throws ParseException
    {
        return parser.parseExpression(sqlText);
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText)
            throws ParseException
    {
        return parser.parseMultipartIdentifier(sqlText);
    }

    @Override
    public LogicalPlan parseQuery(String sqlText)
            throws ParseException
    {
        return parser.parseQuery(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText)
            throws ParseException
    {
        return parser.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText)
            throws ParseException
    {
        return parser.parseDataType(sqlText);
    }
}
