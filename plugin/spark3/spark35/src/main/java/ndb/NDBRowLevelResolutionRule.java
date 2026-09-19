/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import com.vastdata.client.error.ErrorType;
import com.vastdata.client.error.VastRuntimeException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.AliasIdentifier;
import org.apache.spark.sql.catalyst.analysis.AssignmentUtils;
import org.apache.spark.sql.catalyst.analysis.FieldName;
import org.apache.spark.sql.catalyst.analysis.ResolvedFieldName;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.AddColumns;
import org.apache.spark.sql.catalyst.plans.logical.Assignment;
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable;
import org.apache.spark.sql.catalyst.plans.logical.DropColumns;
import org.apache.spark.sql.catalyst.plans.logical.InsertAction;
import org.apache.spark.sql.catalyst.plans.logical.InsertStarAction;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MergeAction;
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable;
import org.apache.spark.sql.catalyst.plans.logical.QualifiedColType;
import org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction;
import org.apache.spark.sql.catalyst.plans.logical.UpdateStarAction;
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable;
import org.apache.spark.sql.catalyst.plans.logical.WithCTE;
import org.apache.spark.sql.catalyst.util.CharVarcharUtils;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.PartialFunction;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Seq;
import scala.collection.mutable.Builder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static ndb.SparkPlannerUtil.nameSeqWithoutRCLSSuffix;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.isForRowLevelOp;
import static spark.sql.catalog.ndb.NDBRowLevelOperationIdentifier.trimTableNameFromRowLevelOpSuffix;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_DEC128_ROW_ID_FIELD;
import static spark.sql.catalog.ndb.TypeUtil.SPARK_INT64_ROW_ID_FIELD;

public class NDBRowLevelResolutionRule
        extends
        org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan>
{
    private static final Logger LOG = LoggerFactory.getLogger(
            NDBRowLevelResolutionRule.class);
    private static final ComplexRowIDPredicate rowIdPredicate = new ComplexRowIDPredicate();

    @Override
    public LogicalPlan apply(LogicalPlan plan)
    {
        if (plan instanceof UpdateTable) {
            UpdateTable u = (UpdateTable) plan;
            if (u.resolved() && u.rewritable() && !u.aligned()) {
                if (SparkSession.getActiveSession().get().conf().contains(
                        "spark.sql.storeAssignmentPolicy") && SparkSession
                        .getActiveSession()
                        .get()
                        .conf()
                        .get("spark.sql.storeAssignmentPolicy")
                        .equalsIgnoreCase("legacy")) {
                    throw new RuntimeException(
                            "LEGACY store assignment policy is disallowed in Spark data source V2. " + "Please set the configuration spark.sql.storeAssignmentPolicy to other values.");
                }
                Function1<LogicalPlan, LogicalPlan> func = lp -> {
                    if (lp instanceof DataSourceV2Relation) {
                        DataSourceV2Relation v2Relation = (DataSourceV2Relation) lp;
                        Seq<AttributeReference> newOutput = v2Relation
                                .output()
                                .map(CharVarcharUtils::cleanAttrMetadata)
                                .toSeq();
                        LOG.info(
                                "NDBResolutionRule UpdateTable: new output: {}",
                                newOutput);
                        return (LogicalPlan) v2Relation.copy(v2Relation.table(),
                                newOutput, v2Relation.catalog(),
                                v2Relation.identifier(), v2Relation.options());
                    }
                    else {
                        return lp;
                    }
                };
                PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(
                        func);
                LogicalPlan transformedTable = u.table().transform(transformer);
                Seq<Assignment> newAssignments = AssignmentUtils.alignUpdateAssignments(
                        transformedTable.output(), u.assignments());
                return u.copy(transformedTable, newAssignments, u.condition());
            }
        }
        else if (plan instanceof DeleteFromTable) {
            DeleteFromTable d = (DeleteFromTable) plan;
            Function1<LogicalPlan, LogicalPlan> func = lp -> {
                if (lp instanceof DataSourceV2Relation) {
                    DataSourceV2Relation v2Relation = (DataSourceV2Relation) lp;
                    Builder<AttributeReference, List<AttributeReference>> refsWithRowID = List.newBuilder();
                    final AttributeReference rowIdAttRef = rowIdPredicate.test(
                            v2Relation.table()) ?
                            new AttributeReference(
                                    SPARK_DEC128_ROW_ID_FIELD.name(),
                                    SPARK_DEC128_ROW_ID_FIELD.dataType(), false,
                                    Metadata.empty(), ExprId.apply(0),
                                    List.<String>newBuilder().result()) :
                            new AttributeReference(
                                    SPARK_INT64_ROW_ID_FIELD.name(),
                                    SPARK_INT64_ROW_ID_FIELD.dataType(), false,
                                    Metadata.empty(), ExprId.apply(0),
                                    List.<String>newBuilder().result());
                    v2Relation.output().foreach(refsWithRowID::$plus$eq);
                    refsWithRowID.$plus$eq(rowIdAttRef);
                    List<AttributeReference> newOutput = refsWithRowID.result();
                    LOG.info(
                            "NDBResolutionRule DeleteFromTable: new output: {}",
                            newOutput);
                    return (LogicalPlan) v2Relation.copy(v2Relation.table(),
                            newOutput, v2Relation.catalog(),
                            v2Relation.identifier(), v2Relation.options());
                }
                else {
                    return lp;
                }
            };
            PartialFunction<LogicalPlan, LogicalPlan> transformer = PartialFunction.fromFunction(
                    func);
            LogicalPlan transformedTable = d.table().transformUp(transformer);
            DeleteFromTable copy = d.copy(transformedTable, d.condition());
            LOG.debug("DeleteFromTable: {}", copy);
        }
        else if (plan instanceof MergeIntoTable || plan instanceof WithCTE) {
            // a MERGE with a CTE source is the child of a WithCTE node
            return plan.transformUp(PartialFunction.fromFunction(
                    this::resolveMergeIntoTableIfReady));
        }
        else if (plan instanceof DropColumns) {
            LOG.debug("Drop columns: {}", plan);
            DropColumns drop = (DropColumns) plan;
            Seq<FieldName> columns = drop.columnsToDrop();
            IntStream.range(0, columns.size()).forEach(i -> {
                FieldName fName = columns.apply(i);
                if (fName instanceof ResolvedFieldName) {
                    ResolvedFieldName resolvedFieldName = (ResolvedFieldName) fName;
                    String name = resolvedFieldName.field().name();
                    if (SPARK_INT64_ROW_ID_FIELD.name().equalsIgnoreCase(
                            name) || SPARK_DEC128_ROW_ID_FIELD
                            .name()
                            .equalsIgnoreCase(name)) {
                        throw new RuntimeException(
                                format("Dropping %s is not allowed", name));
                    }
                }
            });
        }
        else if (plan instanceof AddColumns) {
            LOG.debug("Add columns: {}", plan);
            AddColumns drop = (AddColumns) plan;
            Seq<QualifiedColType> columns = drop.columnsToAdd();
            IntStream.range(0, columns.size()).forEach(i -> {
                QualifiedColType fName = columns.apply(i);
                String name = fName.colName();
                if (SPARK_INT64_ROW_ID_FIELD.name().equalsIgnoreCase(
                        name) || SPARK_DEC128_ROW_ID_FIELD
                        .name()
                        .equalsIgnoreCase(name)) {
                    throw new RuntimeException(
                            format("Adding %s is not allowed", name));
                }
            });
        }
        else if (plan instanceof ReplaceColumns) {
            ReplaceColumns replaceColumns = (ReplaceColumns) plan;
            Seq<QualifiedColType> colsToAdd = replaceColumns.columnsToAdd();
            IntStream.range(0, colsToAdd.size()).forEach(i -> {
                String name = colsToAdd.apply(i).colName();
                if (SPARK_INT64_ROW_ID_FIELD.name().equalsIgnoreCase(
                        name) || SPARK_INT64_ROW_ID_FIELD
                        .name()
                        .equalsIgnoreCase(name)) {
                    throw new RuntimeException(
                            format("Adding %s is not allowed", name));
                }
            });
        }
        return plan;
    }

    private LogicalPlan resolveMergeIntoTableIfReady(LogicalPlan plan)
    {
        if (plan instanceof MergeIntoTable) {
            MergeIntoTable merge = (MergeIntoTable) plan;
            if (merge.targetTable() instanceof NDBMergeTarget) {
                LogicalPlan target = ((NDBMergeTarget) merge.targetTable()).child();
                if (target.resolved() && merge.sourceTable().resolved()) {
                    return resolveMergeIntoTable(merge, target);
                }
                LOG.debug("MergeIntoTable target or source not resolved yet: {}",
                        merge);
            }
        }
        return plan;
    }

    /**
     * Expands {@code UPDATE SET *} / {@code INSERT *} over the data columns of
     * the resolved target (the VAST row id is not a data column), refuses
     * assignments to the row id, and unwraps the {@link NDBMergeTarget} so
     * that Spark's own assignment alignment and MERGE rewrite take over.
     */
    private LogicalPlan resolveMergeIntoTable(MergeIntoTable merge,
            LogicalPlan target)
    {
        Seq<Attribute> targetOutput = target.output();
        java.util.List<Attribute> dataColumns = new ArrayList<>();
        java.util.List<Attribute> rowIdColumns = new ArrayList<>();
        IntStream.range(0, targetOutput.size()).forEachOrdered(i -> {
            Attribute attr = targetOutput.apply(i);
            if (isRowIdColumn(attr.name())) {
                rowIdColumns.add(attr);
            }
            else {
                dataColumns.add(attr);
            }
        });
        LogicalPlan source = merge.sourceTable();
        Seq<MergeAction> matchedActions = adaptMergeActions(
                merge.matchedActions(), dataColumns, rowIdColumns, source);
        Seq<MergeAction> notMatchedActions = adaptMergeActions(
                merge.notMatchedActions(), dataColumns, rowIdColumns, source);
        Seq<MergeAction> notMatchedBySourceActions = adaptMergeActions(
                merge.notMatchedBySourceActions(), dataColumns, rowIdColumns,
                source);
        LogicalPlan unwrappedTarget = withoutResolutionSuffixesInAliases(
                target);
        MergeIntoTable resolved = merge.copy(unwrappedTarget, source,
                merge.mergeCondition(), matchedActions, notMatchedActions,
                notMatchedBySourceActions);
        LOG.info("NDBResolutionRule MergeIntoTable: resolved plan: {}",
                resolved);
        return resolved;
    }

    private static boolean isRowIdColumn(String name)
    {
        return SPARK_INT64_ROW_ID_FIELD.name().equals(
                name) || SPARK_DEC128_ROW_ID_FIELD.name().equals(name);
    }

    private Seq<MergeAction> adaptMergeActions(Seq<MergeAction> actions,
            java.util.List<Attribute> dataColumns,
            java.util.List<Attribute> rowIdColumns, LogicalPlan source)
    {
        Builder<MergeAction, List<MergeAction>> builder = List$.MODULE$.newBuilder();
        IntStream.range(0, actions.size()).forEachOrdered(i -> {
            MergeAction action = actions.apply(i);
            if (action instanceof UpdateStarAction) {
                builder.$plus$eq(new UpdateAction(action.condition(),
                        starAssignments(dataColumns, source)));
            }
            else if (action instanceof InsertStarAction) {
                builder.$plus$eq(new InsertAction(action.condition(),
                        withRowIdPlaceholders(
                                starAssignments(dataColumns, source),
                                rowIdColumns)));
            }
            else if (action instanceof InsertAction) {
                InsertAction insert = (InsertAction) action;
                assertNoRowIdAssignment(insert.assignments());
                builder.$plus$eq(new InsertAction(insert.condition(),
                        withRowIdPlaceholders(insert.assignments(),
                                rowIdColumns)));
            }
            else {
                if (action instanceof UpdateAction) {
                    assertNoRowIdAssignment(
                            ((UpdateAction) action).assignments());
                }
                builder.$plus$eq(action);
            }
        });
        return builder.result();
    }

    // The row id is a non-nullable column of the target, and Spark's insert
    // assignment alignment insists on an assignment for every column (a null
    // literal would be wrapped in AssertNotNull and fail at run time).
    // Inserted rows never send the row id (VastMergeWriter drops the slot), so
    // any non-null value of the right type will do.
    private static Seq<Assignment> withRowIdPlaceholders(
            Seq<Assignment> assignments, java.util.List<Attribute> rowIdColumns)
    {
        Builder<Assignment, List<Assignment>> builder = List$.MODULE$.newBuilder();
        IntStream.range(0, assignments.size()).forEachOrdered(
                i -> builder.$plus$eq(assignments.apply(i)));
        for (Attribute rowId : rowIdColumns) {
            builder.$plus$eq(new Assignment(rowId, zero(rowId.dataType())));
        }
        return builder.result();
    }

    private static Literal zero(DataType dataType)
    {
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return new Literal(Decimal.apply(BigDecimal.ZERO,
                    decimalType.precision(), decimalType.scale()), dataType);
        }
        return new Literal(0L, dataType);
    }

    // Same expansion as Spark's ResolveReferences, minus the row id column:
    // every data column of the target is assigned the source column of the
    // same name, resolved against the source only
    private Seq<Assignment> starAssignments(
            java.util.List<Attribute> dataColumns, LogicalPlan source)
    {
        Function2<String, String, Object> resolver = conf().resolver();
        Builder<Assignment, List<Assignment>> builder = List$.MODULE$.newBuilder();
        for (Attribute targetColumn : dataColumns) {
            Builder<String, List<String>> nameBuilder = List$.MODULE$.newBuilder();
            nameBuilder.$plus$eq(targetColumn.name());
            Option<NamedExpression> sourceColumn = source.resolve(
                    nameBuilder.result(), resolver);
            if (sourceColumn.isEmpty()) {
                String sourceColumns = IntStream
                        .range(0, source.output().size())
                        .mapToObj(i -> source.output().apply(i).name())
                        .collect(Collectors.joining(", "));
                throw new VastRuntimeException(
                        format("MERGE INTO: target column %s cannot be resolved in the source columns [%s]. Use an explicit column list instead of *",
                                targetColumn.name(), sourceColumns), null,
                        ErrorType.USER);
            }
            builder.$plus$eq(new Assignment(targetColumn,
                    (Expression) sourceColumn.get()));
        }
        return builder.result();
    }

    private static void assertNoRowIdAssignment(Seq<Assignment> assignments)
    {
        IntStream.range(0, assignments.size()).forEachOrdered(i -> {
            Expression key = assignments.apply(i).key();
            String name = null;
            if (key instanceof UnresolvedAttribute) {
                name = ((UnresolvedAttribute) key).nameParts().last();
            }
            else if (key instanceof Attribute) {
                name = ((Attribute) key).name();
            }
            if (name != null && isRowIdColumn(name)) {
                throw new VastRuntimeException(
                        format("Assigning a value to %s is not allowed", name),
                        null, ErrorType.USER);
            }
        });
    }

    // Spark aliases a resolved table with the identifier it was looked up by,
    // which for a row level operation carries the VAST resolution suffixes.
    // Restore the plain table name so that `table.column` references in the
    // MERGE condition and actions resolve.
    private static LogicalPlan withoutResolutionSuffixesInAliases(
            LogicalPlan target)
    {
        if (target instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) target;
            LogicalPlan child = withoutResolutionSuffixesInAliases(
                    alias.child());
            AliasIdentifier identifier = alias.identifier();
            Builder<String, List<String>> nameBuilder = List$.MODULE$.newBuilder();
            nameBuilder.$plus$eq(identifier.name());
            String name = nameSeqWithoutRCLSSuffix(nameBuilder.result()).last();
            if (isForRowLevelOp(name)) {
                name = trimTableNameFromRowLevelOpSuffix(name);
            }
            if (!name.equals(identifier.name()) || child != alias.child()) {
                return new SubqueryAlias(
                        new AliasIdentifier(name, identifier.qualifier()),
                        child);
            }
        }
        return target;
    }
}
