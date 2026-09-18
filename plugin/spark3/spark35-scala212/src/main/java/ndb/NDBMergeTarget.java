/*
 *  Copyright (C) Vast Data Ltd.
 */

package ndb;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.IndexedSeq;
import scala.collection.immutable.List$;
import scala.collection.Seq;
import scala.collection.mutable.Builder;

/**
 * Wraps the target of a MERGE INTO statement between parsing and
 * {@link NDBRowLevelResolutionRule}.
 * <p>
 * The wrapped relation is resolved by Spark as usual, but this node always
 * reports itself as unresolved. Spark's analyzer therefore does not resolve
 * the MERGE actions (in particular it does not expand {@code UPDATE SET *} and
 * {@code INSERT *} over the target output, which contains the VAST row id)
 * until {@link NDBRowLevelResolutionRule} has expanded them itself and removed
 * this node.
 */
public class NDBMergeTarget
        extends LogicalPlan
{
    private final LogicalPlan child;

    public NDBMergeTarget(LogicalPlan child)
    {
        super();
        this.child = child;
    }

    public LogicalPlan child()
    {
        return child;
    }

    @Override
    public Seq<LogicalPlan> children()
    {
        Builder<LogicalPlan, scala.collection.immutable.List<LogicalPlan>> builder = List$.MODULE$.newBuilder();
        builder.$plus$eq(child);
        return builder.result();
    }

    @Override
    public LogicalPlan withNewChildrenInternal(
            IndexedSeq<LogicalPlan> newChildren)
    {
        return new NDBMergeTarget(newChildren.apply(0));
    }

    @Override
    public Seq<Attribute> output()
    {
        return child.output();
    }

    @Override
    public boolean resolved()
    {
        return false;
    }

    @Override
    public boolean canEqual(Object that)
    {
        return that instanceof NDBMergeTarget;
    }

    @Override
    public Object productElement(int n)
    {
        if (n == 0) {
            return child;
        }
        throw new IndexOutOfBoundsException(String.valueOf(n));
    }

    @Override
    public int productArity()
    {
        return 1;
    }
}
