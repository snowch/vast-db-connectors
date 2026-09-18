# Native `MERGE INTO` for the Spark 3.5 VAST connector — design

Target: `plugin/spark3/spark35` (Spark 3.5.1, Scala 2.13), ported by intent to `spark35-scala212`.
All Spark behaviour below was read from the `v3.5.1` tag, which is what `spark35.version` pins.
This is the design as implemented; §5 records what changed against the first draft.

## 1. Verification of the brief's findings

| # | Verdict |
|---|---------|
| 1 | Confirmed. `VastRowLevelOperationBuilder.build()` handles `DELETE`/`UPDATE`, else throws. |
| 2 | Confirmed, with detail: the prepended field is `vastdb_spark_int64_row_id` (INT64) or `$row_id` (DEC128); `VastCatalog.loadTable` picks DEC128 when the table has sorted **or** partition columns. |
| 3 | Confirmed. `chunkSize`, `tableArrowSchema`, `writeModeAdaptor`, the `FunctionalQ` and the `VastBGWriter` are all per-`VastWriter`. |
| 4 | Confirmed, all four readers. |
| 5 | Confirmed, unchanged. |
| 6 | Confirmed — the `DeleteFromTable` branch builds `copy` and drops it. Left alone. |
| 7 | Confirmed. |

### Corrections to the brief

**(a) `UPDATE SET *` / `INSERT *` cannot be resolved by Spark while the row id is a column of the
target.** `Analyzer.ResolveReferences` expands both stars as `targetTable.output.map(attr =>
Assignment(attr, UnresolvedAttribute(attr.name)))` and resolves every name **against the source**
(`MergeResolvePolicy.SOURCE`), then `failAnalysis`es on the injected row id. No injected resolution
rule runs before it: `ResolveRelations`, `ResolveReferences`, `ResolveRowLevelCommandAssignments` and
`RewriteMergeIntoTable` are consecutive rules of the `Resolution` batch and `extendedResolutionRules`
come last, and `VastCatalog.loadTable` accepts the RCLS-suffixed name for an unrestricted user, so
Spark resolves the VAST target itself in the first iteration. Solved with the marker node below.

**(b) Insert-only MERGE never reaches `SupportsDelta`.** `RewriteMergeIntoTable` turns a MERGE
without `WHEN MATCHED` / `WHEN NOT MATCHED BY SOURCE` actions into a plain `AppendData`; the row level
operation builder is never called, and such a MERGE already works on this connector. The row-level-op
suffix is therefore only added when the delta path is needed. (`testMergeInsertOnlyIsAPlainAppend`.)

**(c) `alignInsertAssignments` insists on an assignment for the row id.** For an INSERT action every
target column needs an assignment or a default; the non-nullable row id has neither, and a `null`
literal would be wrapped in `AssertNotNull` and fail at run time. The rule therefore assigns a typed,
non-null zero to the row id in every INSERT action; the writer drops that slot (§2, §3.6).

**(d) Table-qualified references need the alias cleaned.** Spark aliases a resolved table with the
identifier it looked it up by, i.e. `tgt VAST_DB_ROW_LEVEL_OP_vast_throw_rcls_error`, so
`ON tgt.k = src.k` would not resolve for an unaliased target. The rule restores the plain name when
it unwraps the target. (DELETE/UPDATE appear to have the same limitation today; see "Observed" in
the PR description.)

## 2. Row layouts Spark hands the writer

Target suffixed ⇒ `relation.output = [row_id, c1 … cN]`; `requiredMetadataAttributes` is empty ⇒
`metadataProjection = None` and `DeltaWritingSparkTask` is used. `rowAttrs = relation.output`,
`rowIdAttrs = [row_id]`, `MergeRows.output = [__row_operation, row_id, c1 … cN]`; both lazy
projections map by name. Verified against the real analyzer in `TestVastCatalog.testMerge*`.

| callback | fields |
|---|---|
| `delete(null, id)` | `id = [row_id]` |
| `update(null, id, row)` | `id = [row_id]`, `row = [row_id, c1 … cN]` — identical to UPDATE today |
| `insert(row)` | `row = [row_id, c1 … cN]` — row id slot holds the placeholder, **must be dropped** |

`representUpdateAsDeleteAndInsert` is false, so updates stay updates. Both projection objects are
reused per row; `QueueCtx.writeArrowRow` already copies.

## 3. Changes (both trees)

1. **`VastTableMetaData`** — `setForMerge()` / `isForMerge()` beside the existing flags.
2. **`RowLevelMerge`** (new) — mirrors `RowLevelDelete`; `command()` is `MERGE`.
3. **`VastRowLevelOperationBuilder.build()`** — `MERGE` branch.
4. **`NDBParser`** — the default path's security wrapper now also visits `MergeIntoTable` nodes
   (`transformUp`, so every relation, target included, has already had the regular SELECT treatment).
   For a delta-path MERGE the target relation is re-suffixed for the row level operation, reached
   through its `SubqueryAlias` if any, and the whole target subtree is wrapped in **`NDBMergeTarget`**
   (new): a `LogicalPlan` node that delegates `output` to its child but always reports
   `resolved = false`. Spark resolves the relation inside it, but every Spark rule that touches the
   MERGE actions is guarded by `m.resolved`, so nothing happens to the actions until the marker is gone.
   Insert-only merges are left untouched (1b). Source side, subqueries and CTE bodies get exactly the
   treatment a `SELECT` gets.
5. **`NDBRowLevelResolutionRule`** — new branch, once target and source are resolved: expands
   `SET *` / `INSERT *` over the target's data columns (values resolved by name against the source
   with Spark's own `LogicalPlan.resolve`, i.e. the same case-sensitivity and ambiguity semantics as
   `MergeResolvePolicy.SOURCE`); refuses assignments to the row id; appends the row-id placeholder to
   every INSERT action (1c); refuses MERGE with an INSERT action on a partitioned table; cleans the
   alias (1d); removes the marker. Spark's alignment and `RewriteMergeIntoTable` then run unchanged.
   A MERGE with a CTE source sits under `WithCTE`, so the branch transforms the tree.
6. **`NDBRCLSResolvedRelationAdaptorRule`** — `MergeIntoTable` case mirroring `UpdateTable`: a
   target resolved to `Project`/`Filter` (row filters or column masks) is refused.
7. **`VastBatch.createReaderFactory`** — `|| isForMerge()` so the scan returns the row id.
8. **`VastPartitionedTable.newWriteBuilder`** — `|| isForMerge()` (update/delete-only merges on a
   partitioned table go through `VastWriteBuilder` like DELETE/UPDATE).
9. **`VastWriteFactory`** —
   * `VastWriter` takes an explicit `VastWriteMode` and a rollback `Callable`; the single-mode path
     derives the mode exactly as the constructor did (`forImportData → IMPORT`, `isForDelete →
     DELETE`, `isForUpdate → UPDATE`, else `INSERT`) and passes the same rollback lambda, so
     DELETE/UPDATE/INSERT/IMPORT take the code paths they take today;
   * the client supplier is an instance accessor with the static Spark-context supplier as its only
     production value; a package-private constructor injects a client for tests (transient field —
     the factory is serialised to executors);
   * **`VastMergeWriter`** (new inner class) — up to three lazily created single-mode `VastWriter`s
     on one transaction, one per kind of row, each with its own queue, Arrow schema, chunk size
     (`getMaxRowsPerDelete/Update/Insert`), `FunctionalQ`, `VastBGWriter`, executor and
     `AwaitableCompletionListener(2)`. `update()` checks the id against field 0 of the row with
     `getDecimal(0, 38, 0)` for DEC128 and `getLong(0)` otherwise; `insert()` projects the row onto
     the data columns; the INSERT context's Arrow schema is the table schema minus field 0.
     `commit()`/`abort()`/`close()` fan out to every created context. One `AtomicBoolean`-guarded
     rollback action is shared by all contexts, so a failure rolls the transaction back at most once.

## 4. Traps

* **Target only / security wrapper** — target re-suffixed after the wrapper, source subtree,
  subqueries and CTEs exactly as for SELECT. RCLS on the target ⇒ `loadTable` throws ⇒
  `NDBTablesResolutionRule` wraps ⇒ (6) refuses, i.e. UPDATE's policy. Nothing is bypassed.
* **Row id column 0** — update rows pass through unchanged; insert rows drop field 0.
* **Both widths** — writer tests run the interleaved delete/update/insert scenario for INT64 and
  DEC128 and check the Arrow schema of every RPC.
* **Multi-mode writer** — three contexts, lazy, one transaction, one guarded rollback.
* **Partitioned tables** — refused only when the MERGE has an INSERT action (the insert would bypass
  `VastPartitionedWriteBuilder`); update/delete-only merges are as safe as DELETE/UPDATE. Insert-only
  merges keep using the partitioned insert path (1b).
* **DELETE / UPDATE** — two `||` additions in conditions that are false for them, and the mechanical
  `VastWriter` constructor change. `TestVastCatalog.testDeleteAndUpdateStillResolveAsBeforeMerge` and
  `TestVastRowLevelOperationBuilder` pin their behaviour.
* **`UPDATE *` / `INSERT *`, row id never assignable** — expanded by (5) over data columns only; an
  explicit assignment to the row id is refused at analysis; the writer keeps the runtime equality
  check. `nonUpdatableColumns` reaches the insert context exactly as for a plain insert (UPDATE does
  not enforce it client-side today either).

## 5. Changes against the first draft, and open points

* The parser-side refusal of stars is gone; the marker node makes the headline upsert work.
* Q2 narrowed: only merges with an INSERT action are refused on partitioned tables.
* `WHEN NOT MATCHED BY SOURCE` is enabled; Spark's rewrite needs nothing from the connector
  (`testMergeNotMatchedBySource`).
* The analysis-level tests live in `TestVastCatalog`: `NDBCommon.vastClient` is a static singleton
  that `clearConfig()` does not reset, so only one mock-server port works per JVM — the same reason
  the existing DELETE/UPDATE analysis tests are there.
* Build/test environment: no JDK 11 here. JDK 17 + Arrow's `--add-opens` flags; see the PR
  description for the exact commands and counts.
