/*
 *  Copyright (C) Vast Data Ltd.
 */

package com.vastdata.spark.write;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.vastdata.client.VastClient;
import com.vastdata.client.VastConfig;
import com.vastdata.client.error.VastUserException;
import com.vastdata.client.metrics.ByColumnInserterMetrics;
import com.vastdata.client.metrics.RecordBatchSplitterMetrics;
import com.vastdata.client.rowid.RowIDStrategyType;
import com.vastdata.client.tx.VastTransaction;
import com.vastdata.spark.VastArrowAllocator;
import com.vastdata.spark.VastTable;
import com.vastdata.spark.VastTableMetaData;
import com.vastdata.spark.write.bg.AwaitableCompletionListener;
import com.vastdata.spark.write.bg.CompletedWriteExecutionComponent;
import com.vastdata.spark.write.bg.FunctionalQ;
import com.vastdata.spark.write.bg.Status;
import com.vastdata.spark.write.bg.VastBGWriter;
import com.vastdata.spark.write.bg.VastBGWriterFactory;
import com.vastdata.spark.write.bg.VastWriteMode;
import ndb.ComplexRowIDPredicate;
import ndb.NDB;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.MutableProjection;
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import spark.sql.catalog.ndb.BoundBucketFunction;
import spark.sql.catalog.ndb.BoundTruncateFunction;
import spark.sql.catalog.ndb.DaysFunction;
import spark.sql.catalog.ndb.HoursFunction;
import spark.sql.catalog.ndb.MonthsFunction;
import spark.sql.catalog.ndb.TypeUtil;
import spark.sql.catalog.ndb.YearsFunction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vastdata.client.error.VastExceptionFactory.toRuntime;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_DEC128_FIELD;
import static com.vastdata.client.schema.ArrowSchemaUtils.ROW_ID_INT64_FIELD;
import static com.vastdata.spark.SparkArrowVectorUtil.ROW_ID_SIGNED_ADAPTOR;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_DEC128_ROW_ID_NONNULL;
import static com.vastdata.spark.SparkArrowVectorUtil.VASTDB_SPARK_INT64_ROW_ID_NONNULL;
import static java.lang.String.format;
import static ndb.NDBSparkSessionExtension.getSessionUser;
import static spark.sql.catalog.ndb.TypeUtil.VAST_ROW_ID_FIELD_SIGNED_FIELD;

public class VastWriteFactory
        implements DeltaWriterFactory
{
    static final Function<VastConfig, VastClient> VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT = vastConfig -> {
        try {
            return NDB.getVastClient(vastConfig);
        }
        catch (VastUserException e) {
            throw toRuntime(e);
        }
    };

    private static final Logger FACTORY_LOG = LoggerFactory.getLogger(
            VastWriteFactory.class);
    private static final Logger DATA_WRITER_LOG = LoggerFactory.getLogger(
            VastWriter.class);
    private static final ComplexRowIDPredicate rowIDPredicate = new ComplexRowIDPredicate();

    private final List<URI> endpoints;
    private final VastTransaction tx;
    private final VastConfig vastConfig;
    private final VastTableMetaData vastTableMetaData;
    private final String vastTraceTokenStr;
    private final Map<String, String> sessionConfig;
    private final boolean complexRowID;
    private final Set<String> nonUpdatableColumns;
    private final List<String> partitionColumns;
    private final List<String> transformNames;
    private final List<Integer> transformArgs;
    // set only by the package private constructor (tests); executors always use
    // VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT
    private transient Function<VastConfig, VastClient> vastClientSupplier;
    private transient RecordBatchSplitterMetrics splitterMetrics;
    private transient ByColumnInserterMetrics insertMetrics;
    private transient ExecutorService ioExecutor;
    private transient ExecutorService cpuExecutor;

    public VastWriteFactory(VastTransaction tx, VastConfig vastConfig,
            VastTable vastTable, List<URI> dataEndpoints,
            Map<String, String> sessionConfig)
    {
        this.tx = tx;
        this.vastConfig = vastConfig;
        this.vastTraceTokenStr = tx
                .generateTraceToken(Optional.empty())
                .toString();
        this.vastTableMetaData = vastTable.getTableMD();
        this.endpoints = dataEndpoints;
        this.sessionConfig = sessionConfig;
        this.complexRowID = rowIDPredicate.test(vastTable);
        if (vastTable.partitioning() != null && !vastTableMetaData.isForDelete() && !vastTableMetaData.isForUpdate() && !vastTableMetaData.forImportData) {
            this.partitionColumns = Arrays
                    .stream(vastTable.partitioning())
                    .map(t -> t.references()[0].fieldNames()[0])
                    .collect(Collectors.toList());
            this.transformNames = Arrays.stream(vastTable.partitioning()).map(
                    Transform::name).collect(Collectors.toList());
            this.transformArgs = Arrays
                    .stream(vastTable.partitioning())
                    .map(t -> t.children().length < 2 ?
                            null :
                            (Integer) ((Literal<?>) t.children()[0]).value())
                    .collect(Collectors.toList());
        }
        else {
            this.partitionColumns = null;
            this.transformNames = null;
            this.transformArgs = null;
        }
        this.nonUpdatableColumns = vastTable.getNonUpdatableColumns();
    }

    VastWriteFactory(VastTransaction tx, VastConfig vastConfig,
            VastTable vastTable, List<URI> dataEndpoints,
            Map<String, String> sessionConfig,
            Function<VastConfig, VastClient> vastClientSupplier)
    {
        this(tx, vastConfig, vastTable, dataEndpoints, sessionConfig);
        this.vastClientSupplier = vastClientSupplier;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId)
    {
        if (vastTableMetaData.isForMerge()) {
            VastMergeWriter mergeWriter = new VastMergeWriter(partitionId,
                    taskId);
            FACTORY_LOG.info(
                    "Created new merge writer: {} for partitionId={}, taskId={}",
                    mergeWriter.name(), partitionId, taskId);
            return mergeWriter;
        }
        VastWriter vastDataWriter = new VastWriter(partitionId, taskId,
                singleWriteMode(), rollbackTransaction());
        FACTORY_LOG.info("Created new writer: {} for partitionId={}, taskId={}",
                vastDataWriter.name(), partitionId, taskId);
        return vastDataWriter;
    }

    private VastWriteMode singleWriteMode()
    {
        if (vastTableMetaData.forImportData) {
            return VastWriteMode.IMPORT;
        }
        else if (vastTableMetaData.isForDelete()) {
            return VastWriteMode.DELETE;
        }
        else if (vastTableMetaData.isForUpdate()) {
            return VastWriteMode.UPDATE;
        }
        return VastWriteMode.INSERT;
    }

    private Function<VastConfig, VastClient> clientSupplier()
    {
        return vastClientSupplier != null ?
                vastClientSupplier :
                VAST_CLIENT_SUPPLIER_FROM_SPARK_CONTEXT;
    }

    private Callable<Void> rollbackTransaction()
    {
        return () -> {
            VastClient vastClient = clientSupplier().apply(vastConfig);
            vastClient.rollbackTransaction(tx, null);
            return null;
        };
    }

    // the partition id of a 128-bit row id, its high 64 bits (the Int128 layout the
    // predicate serializer sends the server)
    private static Long rowIdPartition(InternalRow row)
    {
        return row
                .getDecimal(0, 38, 0)
                .toJavaBigDecimal()
                .toBigInteger()
                .shiftRight(64)
                .longValue();
    }

    private static Callable<Void> atMostOnce(Callable<Void> action)
    {
        AtomicBoolean done = new AtomicBoolean(false);
        return () -> done.compareAndSet(false, true) ? action.call() : null;
    }

    // For MERGE the table schema carries the row id at field 0 (the scan
    // returns it, and the UPDATE/DELETE contexts expect it). Inserted rows
    // must not carry it, so the INSERT context works on the data columns only.
    private StructType dataColumnsSchema()
    {
        StructField[] fields = vastTableMetaData.schema.fields();
        String first = fields.length > 0 ? fields[0].name() : null;
        if (!VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName().equals(
                first) && !VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getName().equals(
                first)) {
            throw new IllegalStateException(format(
                    "Expected the row id as the first field of the MERGE table schema: %s",
                    vastTableMetaData.schema));
        }
        return new StructType(Arrays.copyOfRange(fields, 1, fields.length));
    }

    private List<Expression> dataColumnsReferences()
    {
        StructField[] fields = vastTableMetaData.schema.fields();
        return IntStream.range(1, fields.length).mapToObj(
                i -> (Expression) new BoundReference(i, fields[i].dataType(),
                        fields[i].nullable())).collect(Collectors.toList());
    }

    private class VastWriter
            implements DeltaWriter<InternalRow>,
            CompletedWriteExecutionComponent
    {
        private final int dataWriterIndex;
        private final String dataWriteTraceToken;
        private final ExecutorService executorService;
        private final AwaitableCompletionListener bgTaskPhasesCompletionListener;
        private final BufferAllocator writerAllocator;
        private final FunctionalQ<VectorSchemaRoot> insertArrowVectorsQ;
        private final Schema tableArrowSchema;
        private final UnaryOperator<VectorSchemaRoot> writeModeAdaptor;
        private final int chunkSize;
        private final QueueCtx defaultCtx;
        // one context per partition of the table when the rows are grouped, see partitionKey
        private final java.util.Map<Object, QueueCtx> partitionedCtxs;
        // the partition a row belongs to, null when every row goes to defaultCtx: for inserts
        // the partition transforms over the row, for deletes and updates with a 128-bit row id
        // (partitioned and sorted tables) the partition id the row id carries in its high 64
        // bits, so that a DeleteRows/UpdateRows request holds the rows of one partition, as
        // the Trino connector sends them
        private final Function<InternalRow, Object> partitionKey;
        private final VastWriteMode mode;
        private Status status;

        private VastWriter(int dataWriterIndex, Object traceObj,
                VastWriteMode mode, Callable<Void> txRollback)
        {
            this.dataWriteTraceToken = format("(%s:%s:%s)", vastTraceTokenStr,
                    traceObj, dataWriterIndex);
            this.dataWriterIndex = dataWriterIndex;
            this.mode = mode;
            this.bgTaskPhasesCompletionListener = new AwaitableCompletionListener(
                    2); // 2 phases - this, VastBGWriter
            this.bgTaskPhasesCompletionListener.registerFailureAction(() -> {
                DATA_WRITER_LOG.info("VastWriter{} Rolling back tx: {}",
                        dataWriteTraceToken, tx);
                return txRollback.call();
            });
            this.status = new Status(true, null);
            this.writerAllocator = VastArrowAllocator
                    .writeAllocator()
                    .newChildAllocator(
                            format("VastWriter%s", this.dataWriteTraceToken), 0,
                            Long.MAX_VALUE);
            if (mode == VastWriteMode.DELETE) {
                this.chunkSize = vastConfig.getMaxRowsPerDelete();
                this.writeModeAdaptor = complexRowID ?
                        UnaryOperator.identity() :
                        ROW_ID_SIGNED_ADAPTOR;
                this.tableArrowSchema = new Schema(Lists.newArrayList(
                        complexRowID ?
                                ROW_ID_DEC128_FIELD :
                                ROW_ID_INT64_FIELD));
                DATA_WRITER_LOG.info(
                        "VastWriter{}: DELETE chunkSize = {}, writeSchema = {}",
                        dataWriteTraceToken, chunkSize, tableArrowSchema);
                if (complexRowID) {
                    this.defaultCtx = null;
                    this.partitionedCtxs = new HashMap<>();
                    this.partitionKey = VastWriteFactory::rowIdPartition;
                }
                else {
                    this.defaultCtx = new QueueCtx(
                            InternalRowsQFactory.forDelete(chunkSize,
                                    complexRowID));
                    this.partitionedCtxs = null;
                    this.partitionKey = null;
                }
            }
            else if (mode == VastWriteMode.UPDATE) {
                this.chunkSize = vastConfig.getMaxRowsPerUpdate();
                if (!complexRowID) {
                    this.writeModeAdaptor = ROW_ID_SIGNED_ADAPTOR;
                    StructField[] fields = vastTableMetaData.schema.fields();
                    StructField[] adaptedFields = new StructField[fields.length];
                    adaptedFields[0] = VAST_ROW_ID_FIELD_SIGNED_FIELD;
                    System.arraycopy(fields, 1, adaptedFields, 1,
                            fields.length - 1);
                    StructType writeSchema = new StructType(adaptedFields);
                    this.tableArrowSchema = new Schema(
                            TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                }
                else {
                    this.writeModeAdaptor = UnaryOperator.identity();
                    this.tableArrowSchema = new Schema(
                            TypeUtil.sparkSchemaToArrowFieldsList(
                                    vastTableMetaData.schema));
                }
                DATA_WRITER_LOG.info(
                        "VastWriter{}: UPDATE chunkSize = {}, tableArrowSchema = {}",
                        dataWriteTraceToken, chunkSize, this.tableArrowSchema);
                if (complexRowID) {
                    this.defaultCtx = null;
                    this.partitionedCtxs = new HashMap<>();
                    this.partitionKey = VastWriteFactory::rowIdPartition;
                }
                else {
                    this.defaultCtx = new QueueCtx(
                            InternalRowsQFactory.forUpdate(chunkSize,
                                    complexRowID));
                    this.partitionedCtxs = null;
                    this.partitionKey = null;
                }
            }
            else {
                this.chunkSize = vastConfig.getMaxRowsPerInsert();
                this.writeModeAdaptor = UnaryOperator.identity();
                StructType writeSchema = vastTableMetaData.isForMerge() ?
                        dataColumnsSchema() :
                        vastTableMetaData.schema;
                DATA_WRITER_LOG.info(
                        "VastWriter{}: INSERT chunkSize = {}, writeSchema = {}",
                        dataWriteTraceToken, chunkSize, writeSchema);
                this.tableArrowSchema = new Schema(
                        TypeUtil.sparkSchemaToArrowFieldsList(writeSchema));
                if (partitionColumns != null && vastConfig.getPartitionedInsert()) {
                    // the partition key is computed from the rows this context
                    // receives: for MERGE these are the data columns only
                    List<Expression> projRefs = getTransformExpressions(writeSchema);
                    DATA_WRITER_LOG.info("projector: {}", projRefs);
                    MutableProjection projector = MutableProjection.create(
                            JavaConverters.asScalaBuffer(projRefs).toSeq());
                    this.partitionedCtxs = new HashMap<>();
                    this.defaultCtx = null;
                    this.partitionKey = r -> projector.apply(r).copy();
                }
                else {
                    this.defaultCtx = new QueueCtx(
                            InternalRowsQFactory.forInsert(chunkSize));
                    this.partitionedCtxs = null;
                    this.partitionKey = null;
                }
            }
            int ordinal = ordinal();
            this.insertArrowVectorsQ = new FunctionalQ<>(VectorSchemaRoot.class,
                    this.dataWriteTraceToken, ordinal, 100, 2,
                    this.bgTaskPhasesCompletionListener);

            ordinal++;
            URI endpoint = endpoints.get(dataWriterIndex % endpoints.size());
            VastBGWriter vastBgWriter = getWriter(ordinal, endpoint);
            vastBgWriter.registerCompletionListener(
                    this.bgTaskPhasesCompletionListener);

            this.executorService = Executors.newFixedThreadPool(2,
                    new ThreadFactoryBuilder()
                            .setNameFormat(
                                    "write-worker-" + dataWriterIndex + "-%s")
                            .build());
            executorService.submit(vastBgWriter);
        }

        private List<Expression> getTransformExpressions(StructType writeSchema)
        {
            List<String> writeColumns = Arrays.asList(writeSchema.names());
            return IntStream.range(0, partitionColumns.size()).mapToObj(i -> {
                int idx = writeColumns.indexOf(partitionColumns.get(i));
                if (idx < 0) {
                    throw new IllegalStateException(
                            format("VastWriter%s: partition column %s is not in the write schema %s",
                                    dataWriteTraceToken, partitionColumns.get(i),
                                    writeColumns));
                }
                StructField field = writeSchema.apply(idx);
                BoundReference br = new BoundReference(idx, field.dataType(),
                        field.nullable());
                if (transformNames.get(i).startsWith("identity")) {
                    return br;
                }
                else {
                    UnboundFunction uf;
                    if (transformNames.get(i).startsWith("year")) {
                        uf = new YearsFunction();
                    }
                    else if (transformNames.get(i).startsWith("month")) {
                        uf = new MonthsFunction();
                    }
                    else if (transformNames.get(i).startsWith("day")) {
                        uf = new DaysFunction();
                    }
                    else if (transformNames.get(i).startsWith("hour")) {
                        uf = new HoursFunction();
                    }
                    else if (transformNames.get(i).startsWith("bucket")) {
                        uf = new BoundBucketFunction(transformArgs.get(i));
                    }
                    else if (transformNames.get(i).startsWith("truncate")) {
                        int arg = Integer.parseInt(transformNames
                                .get(i)
                                .substring("truncate_".length()));
                        uf = new BoundTruncateFunction(arg);
                    }
                    else {
                        throw new RuntimeException(
                                format("Unsupported transform: %s",
                                        transformNames.get(i)));
                    }
                    ScalarFunction<Integer> boundF = (ScalarFunction<Integer>) uf.bind(
                            new StructType(new StructField[] {field}));
                    return V2ExpressionUtils.resolveScalarFunction(boundF,
                            JavaConverters
                                    .asScalaBuffer(
                                            Arrays.asList((Expression) br))
                                    .toSeq());
                }
            }).collect(Collectors.toList());
        }

        private VastBGWriter getWriter(int ordinal, URI endpoint)
        {
            String endUser = getSessionUser(vastConfig, sessionConfig);
            if (mode == VastWriteMode.IMPORT) {
                return VastBGWriterFactory.forImport(
                        ordinal,
                        clientSupplier(),
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ
                );
            }
            else if (mode == VastWriteMode.UPDATE) {
                return VastBGWriterFactory.forUpdate(ordinal,
                        clientSupplier(),
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ,
                        endUser);
            }
            else if (mode == VastWriteMode.DELETE) {
                return VastBGWriterFactory.forDelete(ordinal,
                        clientSupplier(),
                        this.dataWriteTraceToken, vastConfig, endpoint, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ,
                        endUser);
            }
            else {
                if (splitterMetrics == null) {
                    splitterMetrics = new RecordBatchSplitterMetrics();
                }
                if (insertMetrics == null) {
                    insertMetrics = new ByColumnInserterMetrics();
                }
                if (ioExecutor == null) {
                    int cores = Integer.parseInt(sessionConfig.getOrElse(
                            "spark.executor.cores", () -> Integer.toString(Runtime.getRuntime().availableProcessors())));
                    ioExecutor = Executors.newFixedThreadPool(
                            Math.min(vastConfig.getNodeIoExecutorNumThreads(), cores),
                            new ThreadFactoryBuilder().setNameFormat("vast-insert-io-%d").build());
                }
                if (cpuExecutor == null) {
                    cpuExecutor = Executors.newFixedThreadPool(
                            2 * Runtime.getRuntime().availableProcessors(),
                            new ThreadFactoryBuilder().setNameFormat("vast-insert-cpu-%d").build());
                }
                return VastBGWriterFactory.forInsert(ordinal,
                        clientSupplier(),
                        this.dataWriteTraceToken, vastConfig, endpoints, tx,
                        vastTableMetaData.schemaName,
                        vastTableMetaData.tableName, this.insertArrowVectorsQ, endUser,
                        nonUpdatableColumns,
                        complexRowID ? RowIDStrategyType.DECIMAL_128 : RowIDStrategyType.UNSIGNED_INT64,
                        splitterMetrics,
                        insertMetrics,
                        ioExecutor,
                        cpuExecutor
                );
            }
        }

        private QueueCtx getCtx(InternalRow r)
        {
            if (defaultCtx != null) {
                return defaultCtx;
            }
            return partitionedCtxs.computeIfAbsent(partitionKey.apply(r),
                    key -> new QueueCtx(createRowsQueue()));
        }

        private Queue<InternalRow> createRowsQueue()
        {
            if (mode == VastWriteMode.DELETE) {
                return InternalRowsQFactory.forDelete(chunkSize, complexRowID);
            }

            if (mode == VastWriteMode.UPDATE) {
                return InternalRowsQFactory.forUpdate(chunkSize, complexRowID);
            }

            return InternalRowsQFactory.forInsert(chunkSize);
        }

        private void forAllCtxs(Consumer<QueueCtx> l)
        {
            if (defaultCtx != null) {
                l.accept(defaultCtx);
                return;
            }
            partitionedCtxs.values().forEach(l);
        }

        private int getCtr()
        {
            if (defaultCtx != null) {
                return defaultCtx.getCtr();
            }
            return partitionedCtxs
                    .values()
                    .stream()
                    .mapToInt(QueueCtx::getCtr)
                    .sum();
        }

        @Override
        public void delete(InternalRow metadata, InternalRow id)
                throws IOException
        {
            write(id);
        }

        @Override
        public void update(InternalRow metadata, InternalRow id,
                InternalRow row)
                throws IOException
        {
            long idVal = id.getLong(0);
            long idValFromRow = row.getLong(0); // row id field is the last
            if (idVal != idValFromRow) {
                throw new IllegalStateException(
                        format("VastWriter%s: Value of %s can not be changed: orig id: %s, new id: %s",
                                dataWriteTraceToken,
                                VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName(),
                                idVal, idValFromRow));
            }
            write(row);
        }

        @Override
        public void insert(InternalRow internalRow)
                throws IOException
        {
            write(internalRow);
        }

        private void flushQueues()
        {
            if (mode == VastWriteMode.INSERT) {
                // inserted rows of several partitions may share a request: gather the
                // remainders of all contexts into one
                QueueCtx tmp = new QueueCtx(createRowsQueue());
                partitionedCtxs.values().forEach(tmp::steal);
                tmp.commit();
            }
            else {
                // a delete or update request carries the rows of one partition only
                partitionedCtxs.values().forEach(ctx -> {
                    ctx.commit();
                    ctx.close();
                });
            }
            partitionedCtxs.clear();
        }

        @Override
        public void write(InternalRow internalRow)
                throws IOException
        {
            if (partitionedCtxs != null && partitionedCtxs.size() > vastConfig.getMaxInsertBuckets()) {
                flushQueues();
            }
            bgTaskPhasesCompletionListener.assertFailure();
            getCtx(internalRow).write(internalRow);
        }

        @Override
        public WriterCommitMessage commit()
                throws IOException
        {
            // counted before the flush, which retires the partition contexts
            int rows = getCtr();
            DATA_WRITER_LOG.info("VastWriter{} commit(), ctr = {}",
                    dataWriteTraceToken, rows);
            this.bgTaskPhasesCompletionListener.assertFailure();
            if (partitionedCtxs != null) {
                flushQueues();
            }
            else {
                defaultCtx.commit();
            }
            this.bgTaskPhasesCompletionListener.completed(this);
            try {
                this.bgTaskPhasesCompletionListener.await();
            }
            catch (InterruptedException e) {
                throw new IOException(
                        format("VastWriter%s Interrupted while waiting for BG tasks completion",
                                dataWriteTraceToken), e);
            }
            DATA_WRITER_LOG.debug("VastWriter{} BG tasks threadpool shutdown",
                    dataWriteTraceToken);
            terminateBackgroundProcesses();
            return new VastCommitMessage(
                    new WriteCommitInfo(dataWriterIndex, dataWriteTraceToken,
                            rows).toString());
        }

        @Override
        public void abort()
        {
            DATA_WRITER_LOG.info("VastWriter{} abort()", dataWriteTraceToken);
            this.status = new Status(false, null);
            this.bgTaskPhasesCompletionListener.completed(this);
            terminateBackgroundProcesses();
        }

        private void terminateBackgroundProcesses()
        {
            if (!this.executorService.shutdownNow().isEmpty()) {
                try {
                    DATA_WRITER_LOG.info(
                            "VastWriter{} abort() awaitTermination - start",
                            dataWriteTraceToken);
                    boolean termination = this.executorService.awaitTermination(
                            100, TimeUnit.MILLISECONDS);
                    DATA_WRITER_LOG.info(
                            "VastWriter{} abort() awaitTermination - end: {}",
                            dataWriteTraceToken, termination);
                }
                catch (InterruptedException e) {
                    if (Thread.interrupted()) {
                        throw new RuntimeException(
                                format("VastWriter%s Interrupted while awaiting BG tasks termination",
                                        dataWriteTraceToken), e);
                    }
                }
            }
        }

        @Override
        public void close()
        {
            DATA_WRITER_LOG.info("VastWriter{} close()", dataWriteTraceToken);
            if (!this.executorService.shutdownNow().isEmpty()) {
                DATA_WRITER_LOG.warn(
                        "VastWriter{} Data write is closed without successfully terminating background threads",
                        dataWriteTraceToken);
            }
            VectorSchemaRoot tmp;
            while ((tmp = this.insertArrowVectorsQ.get()) != null) {
                DATA_WRITER_LOG.warn(
                        "VastWriter{} Closing leftover chunk of {} rows: {}",
                        dataWriteTraceToken, tmp.getRowCount(), tmp.hashCode());
                tmp.close();
            }
            forAllCtxs(QueueCtx::close);
            this.bgTaskPhasesCompletionListener.assertFailure();
            long allocated = this.writerAllocator.getAllocatedMemory();
            if (allocated != 0) {
                String msg = format("VastWriter%s: %s bytes are not freed: %s",
                        dataWriteTraceToken, allocated,
                        writerAllocator.toVerboseString());
                DATA_WRITER_LOG.error(msg);
                throw new IllegalStateException(
                        msg); // TODO: consider disabling via config/session
            }
            this.writerAllocator.close();
        }

        @Override
        public String name()
        {
            return format("VastWriter%s", dataWriteTraceToken);
        }

        @Override
        public int ordinal()
        {
            return 0;
        }

        @Override
        public Status status()
        {
            return status;
        }

        private class QueueCtx
        {
            private final Queue<InternalRow> rowsQ;
            private int ctr = 0;
            private ArrowWriter arrowWriter;
            private VectorSchemaRoot currentRoot;


            private QueueCtx(Queue<InternalRow> q)
            {
                this.rowsQ = q;
            }

            public void write(InternalRow internalRow)
                    throws IOException
            {
                if (ctr % chunkSize == 0) {
                    setNextArrowWriter();
                }
                writeArrowRow(internalRow);
                if (++ctr % chunkSize == 0) {
                    submitInsertChunk();
                }
            }

            private void writeArrowRow(InternalRow internalRow)
            {
                rowsQ.add(internalRow.copy());
            }

            private void setNextArrowWriter()
            {
                currentRoot = VectorSchemaRoot.create(tableArrowSchema,
                        writerAllocator);
                try {
                    arrowWriter = TypeUtil.getArrowSchemaWriter(currentRoot);
                }
                catch (Exception any) {
                    DATA_WRITER_LOG.error(
                            format("VastWriter%s: Failed creating new writer, ctr = %s",
                                    dataWriteTraceToken, ctr), any);
                    throw toRuntime(any);
                }
            }

            public void submitInsertChunk()
            {
                while (!rowsQ.isEmpty()) {
                    InternalRow internalRow = rowsQ.remove();
                    try {
                        arrowWriter.write(internalRow);
                    }
                    catch (RuntimeException re) {
                        arrowWriter.finish();
                        currentRoot.close();
                        throw new RuntimeException(
                                format("VastWriter%s: Exception during arrow write of row no. %s",
                                        dataWriteTraceToken, ctr), re);
                    }
                }
                try {
                    arrowWriter.finish();
                }
                catch (RuntimeException re) {
                    currentRoot.close();
                    throw re;
                }
                // from here on the chunk belongs to the queue and to the background
                // writer, which closes it once written: close() must not touch it while
                // it is in flight (flushQueues() closes the contexts it steals from)
                VectorSchemaRoot chunk = currentRoot;
                currentRoot = null;
                arrowWriter = null;
                VectorSchemaRoot adapted = null;
                try {
                    DATA_WRITER_LOG.info(
                            "VastWriter{}: Submitting next chunk of {} rows, hash={}: {} ({}, {})",
                            dataWriteTraceToken, chunk.getRowCount(),
                            chunk.hashCode(), chunk.getSchema(),
                            ctr, chunkSize);
                    adapted = writeModeAdaptor.apply(chunk);
                    insertArrowVectorsQ.accept(adapted);
                }
                catch (Throwable any) {
                    (adapted != null ? adapted : chunk).close();
                    throw any;
                }
            }

            public void steal(QueueCtx other)
            {
                try {
                    while (!other.rowsQ.isEmpty()) {
                        InternalRow internalRow = other.rowsQ.remove();
                        write(internalRow);
                    }
                    other.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            public void commit()
            {
                if (ctr % chunkSize != 0) {
                    submitInsertChunk();
                }
            }

            public void close()
            {
                if (currentRoot != null) {
                    currentRoot.close();
                }
            }

            private int getCtr()
            {
                return ctr;
            }
        }
    }

    /**
     * Writer for MERGE INTO. Spark hands each task a mix of delete, update
     * and insert rows; each kind is written by its own single-mode
     * {@link VastWriter} (own queue, Arrow schema, chunk size and background
     * writer), created on first use and all on the same transaction.
     */
    private class VastMergeWriter
            implements DeltaWriter<InternalRow>
    {
        private final int dataWriterIndex;
        private final Object traceObj;
        private final String dataWriteTraceToken;
        private final Callable<Void> txRollback;
        private VastWriter deleteWriter;
        private VastWriter updateWriter;
        private VastWriter insertWriter;
        private MutableProjection dataColumnsProjection;

        private VastMergeWriter(int dataWriterIndex, Object traceObj)
        {
            this.dataWriterIndex = dataWriterIndex;
            this.traceObj = traceObj;
            this.dataWriteTraceToken = format("(%s:%s:%s)", vastTraceTokenStr,
                    traceObj, dataWriterIndex);
            // whichever context fails first rolls the transaction back, once
            this.txRollback = atMostOnce(rollbackTransaction());
        }

        private VastWriter newWriter(VastWriteMode writeMode)
        {
            VastWriter writer = new VastWriter(dataWriterIndex,
                    format("%s:%s", traceObj, writeMode), writeMode,
                    txRollback);
            DATA_WRITER_LOG.info("VastMergeWriter{} created {} context: {}",
                    dataWriteTraceToken, writeMode, writer.name());
            return writer;
        }

        private VastWriter deleteWriter()
        {
            if (deleteWriter == null) {
                deleteWriter = newWriter(VastWriteMode.DELETE);
            }
            return deleteWriter;
        }

        private VastWriter updateWriter()
        {
            if (updateWriter == null) {
                updateWriter = newWriter(VastWriteMode.UPDATE);
            }
            return updateWriter;
        }

        private VastWriter insertWriter()
        {
            if (insertWriter == null) {
                insertWriter = newWriter(VastWriteMode.INSERT);
                dataColumnsProjection = MutableProjection.create(
                        JavaConverters
                                .asScalaBuffer(dataColumnsReferences())
                                .toSeq());
            }
            return insertWriter;
        }

        private List<VastWriter> createdWriters()
        {
            List<VastWriter> created = new ArrayList<>(3);
            if (deleteWriter != null) {
                created.add(deleteWriter);
            }
            if (updateWriter != null) {
                created.add(updateWriter);
            }
            if (insertWriter != null) {
                created.add(insertWriter);
            }
            return created;
        }

        @Override
        public void delete(InternalRow metadata, InternalRow id)
                throws IOException
        {
            deleteWriter().write(id);
        }

        @Override
        public void update(InternalRow metadata, InternalRow id,
                InternalRow row)
                throws IOException
        {
            assertSameRowId(id, row);
            updateWriter().write(row);
        }

        @Override
        public void insert(InternalRow row)
                throws IOException
        {
            VastWriter writer = insertWriter();
            // drop the (null) row id slot Spark projects for inserted rows
            writer.write(dataColumnsProjection.apply(row));
        }

        @Override
        public void write(InternalRow row)
                throws IOException
        {
            insert(row);
        }

        // Spark projects the row id both as the id row and as field 0 of the
        // updated row (WriteDeltaProjections); they must agree, for either
        // row id width
        private void assertSameRowId(InternalRow id, InternalRow row)
        {
            if (complexRowID) {
                Decimal idVal = id.getDecimal(0, 38, 0);
                Decimal idValFromRow = row.getDecimal(0, 38, 0);
                if (!Objects.equals(idVal, idValFromRow)) {
                    throw rowIdChangedError(
                            VASTDB_SPARK_DEC128_ROW_ID_NONNULL.getName(), idVal,
                            idValFromRow);
                }
            }
            else {
                long idVal = id.getLong(0);
                long idValFromRow = row.getLong(0);
                if (idVal != idValFromRow) {
                    throw rowIdChangedError(
                            VASTDB_SPARK_INT64_ROW_ID_NONNULL.getName(), idVal,
                            idValFromRow);
                }
            }
        }

        private IllegalStateException rowIdChangedError(String field,
                Object origId, Object newId)
        {
            return new IllegalStateException(
                    format("VastMergeWriter%s: Value of %s can not be changed: orig id: %s, new id: %s",
                            dataWriteTraceToken, field, origId, newId));
        }

        @Override
        public WriterCommitMessage commit()
                throws IOException
        {
            List<VastWriter> created = createdWriters();
            DATA_WRITER_LOG.info("VastMergeWriter{} commit(), contexts = {}",
                    dataWriteTraceToken, created.size());
            List<String> messages = new ArrayList<>(created.size());
            for (VastWriter writer : created) {
                messages.add(writer.commit().toString());
            }
            return new VastCommitMessage(
                    format("merge writer %s contexts: %s", dataWriteTraceToken,
                            messages));
        }

        @Override
        public void abort()
        {
            DATA_WRITER_LOG.info("VastMergeWriter{} abort()",
                    dataWriteTraceToken);
            for (VastWriter writer : createdWriters()) {
                writer.abort();
            }
        }

        @Override
        public void close()
        {
            DATA_WRITER_LOG.info("VastMergeWriter{} close()",
                    dataWriteTraceToken);
            RuntimeException failure = null;
            for (VastWriter writer : createdWriters()) {
                try {
                    writer.close();
                }
                catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        public String name()
        {
            return format("VastMergeWriter%s", dataWriteTraceToken);
        }
    }
}
