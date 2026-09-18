# Native `MERGE INTO` for the Spark 3.5 VAST connector — design

Target: `plugin/spark3/spark35` (Spark 3.5.1, Scala 2.13), then ported to `spark35-scala212`.
All Spark behaviour below was read from the `v3.5.1` tag, which is what `spark35.version` pins.

## 1. Verification of the brief's findings

| # | Verdict |
|---|---------|
| 1 | Confirmed. `VastRowLevelOperationBuilder.build()` handles `DELETE`/`UPDATE`, else throws. |
| 2 | Confirmed, with detail: the prepended field is `vastdb_spark_int64_row_id` (INT64) or `$row_id` (DEC128); `VastCatalog.loadTable:542` picks DEC128 when the table has sorted **or** partition columns. |
| 3 | Confirmed. Note also that `chunkSize`, `tableArrowSchema`, `writeModeAdaptor` and the single `FunctionalQ` are all per-`VastWriter` fields that `QueueCtx` reads from its enclosing instance. |
| 4 | Confirmed, all four readers. |
| 5 | Confirmed. |
| 6 | Confirmed — the `DeleteFromTable` branch builds `copy` and drops it. Left alone. |
| 7 | Confirmed. `TestSparkDataSourceV2` is `@Test(enabled = false)`-style disabled. |

### Corrections / additions to the brief

**(a) `UPDATE SET *` and `INSERT *` cannot work with the row id in the target's output.** This is the
one place where the code contradicts the brief, and it is load-bearing. `Analyzer.ResolveReferences`
(Analyzer.scala:1643,1662) expands both stars as

```scala
val assignments = targetTable.output.map { attr => Assignment(attr, UnresolvedAttribute(Seq(attr.name))) }
resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE)
```

so every target column name — including the injected row id — must resolve **against the source**.
`checkResolvedMergeExpr` calls `failAnalysis("UNRESOLVED_COLUMN.WITH_SUGGESTION")`, so the query dies
with `cannot resolve vastdb_spark_int64_row_id` before any connector code runs.

There is no hook early enough to fix this. `ResolveRelations`, `ResolveReferences`,
`ResolveRowLevelCommandAssignments` and `RewriteMergeIntoTable` are consecutive rules in the same
`Resolution` batch; `injectResolutionRule` appends to `extendedResolutionRules` at the **end** of that
batch. And `VastCatalog.loadTable` accepts the RCLS-suffixed name (it only throws `NoSuchTableException`
when the user actually has row filters / masked columns), so for an unrestricted user Spark's own
`ResolveRelations` resolves the VAST target in iteration 1 — our rules never see the plan first.
`MergeAction` is `sealed`, so a marker action is not available either.

The clean fix is to stop putting the row id in the table schema and expose it as a metadata column
(`SupportsMetadataColumns`) — `LogicalPlan.resolve` falls back to `metadataOutput`, which is how
Iceberg's `_file`/`_pos` row ids work. That changes `VastTable.schema()`, `VastScanBuilder`,
`VastColumnarBatchReader` and the UPDATE row layout, i.e. it rewrites the DELETE/UPDATE paths too.
**Out of scope for this PR.** Proposal: reject the two star forms in the parser with an explicit
message, and document the follow-up. See open question Q1.

**(b) Insert-only MERGE never reaches `SupportsDelta` and probably already works today.**
`RewriteMergeIntoTable` has two fast paths (lines 47 and 80): when `matchedActions.isEmpty &&
notMatchedBySourceActions.isEmpty`, MERGE becomes `AppendData.byPosition(r, …)` — a plain insert —
and `newRowLevelOperationBuilder` is never called. So `MERGE … WHEN NOT MATCHED THEN INSERT *` works
on this connector now. Consequence: **we must only add the row-level-op suffix when the MERGE actually
needs the delta path**, i.e. when `matchedActions` or `notMatchedBySourceActions` is non-empty.
Suffixing an insert-only MERGE would break it — the `AppendData` nullability check rejects the
null literal Spark aligns into the non-nullable row-id slot.

**(c) The DELETE/UPDATE parser branch over-suffixes.** `original.transform(...)` adds the row-level-op
suffix to *every* `UnresolvedRelation`, including relations inside subqueries, and it bypasses the
CTE and non-VAST-provider exclusions in `getSecurityWrapper`. Pre-existing; not changed; not copied.

## 2. Row layouts Spark hands the writer

With the target suffixed, `relation.output = [row_id, c1 … cN]` and `requiredMetadataAttributes` is
empty, so `metadataProjection` is `None` and Spark uses `DeltaWritingSparkTask`
(`WriteToDataSourceV2Exec.scala:497`). `rowAttrs = relation.output`, `rowIdAttrs = [row_id]`,
`MergeRows.output = [__row_operation, row_id, c1 … cN]`, and the two lazy projections map by name:

| callback | argument | fields |
|---|---|---|
| `delete(null, id)` | `id` | `[row_id]` |
| `update(null, id, row)` | `id`, `row` | `[row_id]`, `[row_id, c1 … cN]` — row id at field 0, same as UPDATE today |
| `insert(row)` | `row` | `[row_id, c1 … cN]` — **row id slot present and NULL**, must be dropped |

`deltaInsertOutput` emits `assignments.map(_.value)` for every target column; `alignInsertAssignments`
gives the unassigned row id `Literal(null, …)` (`spark.sql.defaultColumn.useNullsForMissingDefaultValues`
defaults to true). `generateExpandOutput` makes the MergeRows row-id attribute nullable, so the
non-null check in `resolveRowIdAttrs` (which runs against the *relation*) still passes.
`representUpdateAsDeleteAndInsert` is false, so updates stay updates. Both projection objects are
reused per row — `QueueCtx.writeArrowRow` already does `internalRow.copy()`.

## 3. Proposed changes

Smallest diff that works; DELETE/UPDATE keep their exact code paths.

1. **`VastTableMetaData`** — add `setForMerge()` / `isForMerge()` beside the existing flags.
2. **`RowLevelMerge`** (new, mirrors `RowLevelDelete`) — extends `VastDeltaOperation`, calls
   `setForMerge()`, `command()` returns `MERGE`. `rowId()`/`newScanBuilder`/`newWriteBuilder` are
   inherited unchanged.
3. **`VastRowLevelOperationBuilder.build()`** — add the `MERGE` branch. Refuse up front when
   `vastTable.partitioning().length > 0` (see trap *Partitioned tables*).
4. **`NDBParser.parsePlan`** — new `MergeIntoTable` branch, only when the plan needs the delta path
   (2b). It (i) refuses `UpdateStarAction`/`InsertStarAction` and any assignment whose key names the
   row-id column, (ii) suffixes **only** the target relation, reaching through `SubqueryAlias`,
   with `addVastResolutionSuffixes(rel, true, true)`, and (iii) runs the existing
   `getSecurityWrapper(original)` over the **source subtree only**, then rebuilds the `MergeIntoTable`.
   Suffixing the target before the wrapper avoids the double-RCLS-suffix that a whole-plan transform
   would produce (`loadTable` strips only one).
5. **`NDBRCLSResolvedRelationAdaptorRule`** — add a `MergeIntoTable` case mirroring `UpdateTable`:
   if the target, after `EliminateSubqueryAliases`, is a `Project` or `Filter`, throw
   "Merge into table is not allowed by current VAST security policy rules".
6. **`VastBatch.createReaderFactory`** — add `|| isForMerge()` so the scan sets `forAlter` and returns
   the row id.
7. **`VastPartitionedTable.newWriteBuilder`** — add `|| tableMD.isForMerge()` (defence in depth; MERGE
   is refused on partitioned tables before this point).
8. **`VastWriteFactory`** —
   * extract the rollback lambda so it can be supplied to `VastWriter`; the single-mode path passes
     exactly today's lambda, so its behaviour is unchanged;
   * give `VastWriter` an explicit `VastWriteMode` constructor argument. The existing call site
     computes it exactly as the constructor does today (`isForDelete → DELETE`, `isForUpdate → UPDATE`,
     else `INSERT`), so DELETE/UPDATE/INSERT are byte-for-byte the same paths;
   * new inner class **`VastMergeWriter implements DeltaWriter<InternalRow>`**, returned by
     `createWriter` when `isForMerge()`. It lazily creates up to three `VastWriter`s — one per mode,
     all on the same `VastTransaction`, each with its own queue, Arrow schema, chunk size
     (`getMaxRowsPerDelete/Update/Insert`), `FunctionalQ`, `VastBGWriter`, executor and
     `AwaitableCompletionListener(2)` — and dispatches `delete`/`update`/`insert` to them.
     `commit()` commits every created sub-writer and returns one combined `VastCommitMessage`;
     `abort()`/`close()` do the same. The insert sub-writer gets a `MutableProjection` over
     `BoundReference(1..N)` that drops the row-id slot, and an insert Arrow schema built from
     `vastTableMetaData.schema` minus field 0.

## 4. Traps

* **Target only** — handled in (4); the source subtree gets only the standard `getSecurityWrapper`
  treatment. `RewriteMergeIntoTable.validateMergeIntoConditions` already forbids subqueries in the
  merge/action conditions, so the source plan is the only other place relations can appear.
* **Security wrapper** — MERGE is *stricter than or equal to* both existing operations. When the user
  has row filters or masked columns, `loadTable` throws, `NDBTablesResolutionRule` resolves the target
  to a `Filter`/`Project`-wrapped relation, and (5) refuses — exactly what `UpdateTable` does today.
  The source is wrapped identically to a `SELECT`. No RCLS path is weakened; nothing is bypassed.
* **Row id is column 0** — insert rows are projected to drop field 0; update rows are passed through
  unchanged (already the UPDATE layout); delete rows are the 1-field id row.
* **Both row-id widths** — `VastMergeWriter.update()` compares `id` and `row` using
  `getDecimal(0, 38, 0)` when `ComplexRowIDPredicate` holds and `getLong(0)` otherwise. The existing
  `VastWriter.update()` is not touched.
* **Multi-mode writer** — three independent sub-writers, created lazily, one shared transaction.
  Each has its own 2-phase completion listener (the existing, tested shape). The rollback action is a
  **single** `Callable` shared by all three and guarded by an `AtomicBoolean`, so a failure in any
  context rolls the transaction back at most once.
* **Partitioned tables** — delta-path MERGE is **refused** on partitioned tables with a clear message.
  MERGE inserts would otherwise bypass `VastPartitionedWriteBuilder` and the partition transforms and
  write incorrectly partitioned data. Insert-only MERGE on a partitioned table is unaffected (2b) and
  keeps going through the normal partitioned insert path.
* **DELETE / UPDATE regression** — the only edits on their paths are an added `||` in two conditions
  that are false for them, and the two mechanical `VastWriter` constructor changes described above.
* **Mutable table metadata** — `setForMerge()` follows the existing pattern; all four readers of the
  existing flags are addressed in (6), (7), and the `VastWriteFactory` constructor (which needs no
  change: MERGE is refused on partitioned tables, so `partitionIndices` is empty and MERGE inserts
  take the identical path a plain insert takes).
* **`UPDATE *` / `INSERT *`, row id not assignable** — stars are refused in the parser (1a); an
  explicit assignment naming the row-id column is refused there too; and the runtime equality check in
  `VastMergeWriter.update()` remains as a backstop. `nonUpdatableColumns` is threaded to the insert
  sub-writer exactly as for a plain insert; note that **UPDATE does not enforce it client-side today**
  either, so MERGE updates are consistent with UPDATE.

## 5. Open questions

* **Q1 (needs your call).** Do you want `UPDATE SET *` / `INSERT *` refused with a clear message (my
  recommendation — it is the conservative reading of your ground rules), or should I also add a
  parser-side expansion that loads the target's columns from `VastCatalog` at parse time and rewrites
  the star actions into explicit ones? The latter makes the headline upsert work, but it puts a
  catalog round-trip in the parser and duplicates namespace resolution — the least "obviously correct"
  part of the change.
* **Q2.** Refusing all delta-path MERGE on partitioned tables also refuses delete/update-only merges,
  which are no riskier than today's DELETE/UPDATE. Narrower rule (only refuse when the MERGE has
  `WHEN NOT MATCHED … INSERT`) is possible but needs the action list at
  `VastRowLevelOperationBuilder.build()` time, which we do not have. Keep the blunt refusal?
* **Q3.** `WHEN NOT MATCHED BY SOURCE` needs nothing extra — Spark's rewrite turns it into ordinary
  update/delete instructions over a `LeftOuter`/`FullOuter` join. Confirm you want it left enabled
  rather than explicitly refused for a first PR.
* **Q4 (FYI, no decision needed).** No JDK 11 is available in this environment, so I cannot run the
  README's exact command. What I have actually run on the unmodified `master` tree:
  * JDK 21 — `./mvnw -pl plugin/spark3/spark35 -am -DskipTests package` → **BUILD SUCCESS**.
    `… test` → `spark-common` fails to even instantiate its Mockito tests (byte-buddy 1.12.19 predates JDK 21).
  * JDK 17 (installed via apt) + `-DargLine="--add-opens=java.base/java.nio=ALL-UNNAMED …"` (Arrow
    needs it from JDK 17 on) — `./mvnw -pl plugin/spark3/spark35 -am test` → **BUILD SUCCESS**,
    `ndb-common` 129, `spark-common` 6, `spark-35-scala-213` 113 tests, 0 failures.

  So I have a green baseline to regress against, on JDK 17 rather than 11. Every test claim in the PR
  description will name the JDK and the flags.
