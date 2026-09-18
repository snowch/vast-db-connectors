/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark;

import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;

public class RowLevelMerge
        extends VastDeltaOperation
{
    public RowLevelMerge(VastTable vastTable)
    {
        super(vastTable);
        vastTable.getTableMD().setForMerge();
    }

    @Override
    public Command command()
    {
        return MERGE;
    }
}
