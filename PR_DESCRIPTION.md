# Spark 3.5: native `MERGE INTO` for VAST DB tables

## What and why

Spark 3.5 rewrites `MERGE INTO` on a DSv2 table whose row-level operation implements `SupportsDelta`
into a `WriteDelta` plan and calls `DeltaWriter.delete / update / insert` per row. This connector
already implements `SupportsDelta` with a real row id for `DELETE` and `UPDATE`; `MERGE` was refused
in `VastRowLevelOperationBuilder`, and the writer could only run one kind of write per task. This PR
adds the `MERGE` operation and a multi-context writer, in `spark35` and `spark35-scala212`. `DELETE`
and `UPDATE` keep the code paths they take today. `spark34` is out of scope (no DSv2 MERGE rewrite).

The main motivation is migrating Databricks workloads, where
`WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` is the standard upsert.

## Design

The target of a delta-path MERGE is resolved as a row level operation exactly like the target of
DELETE/UPDATE (the `VAST_DB_ROW_LEVEL_OP` suffix makes `VastCatalog.loadTable` prepend the row id as
column 0), the source side gets the same treatment as a `SELECT`. Because Spark's `ResolveReferences`
expands `SET *` / `INSERT *` over the target output and resolves each name against the source, the row
id would make every star fail; and no injected rule runs early enough to intervene. So the parser wraps
the target in `NDBMergeTarget`, a plan node that reports itself unresolved: Spark resolves the relation
inside it but leaves the MERGE actions alone until `NDBRowLevelResolutionRule` expands the stars over
the data columns (values resolved against the source with Spark's own `LogicalPlan.resolve`), refuses
assignments to the row id, adds a placeholder assignment for the row id to INSERT actions (Spark's
alignment requires one; the writer drops the slot), and removes the node. From there Spark's alignment
and `RewriteMergeIntoTable` run unchanged. An unaliased target is aliased with its plain table name at
parse time (`SubqueryAlias(tgt, …)` inside the marker): on a cluster `VastCatalog.loadTable` treats an
empty, non-null masked-columns map as row/column security, so Spark's `ResolveRelations` never resolves
a VAST relation — `NDBTablesResolutionRule` does, and returns a bare relation with no alias, which left
`ON tgt.k = s.k` unresolvable in the first cut of this PR. The rule additionally cleans the suffixed
alias Spark adds when it resolves the relation itself (the mock server path). On the write side,
`VastMergeWriter` owns up to three lazily created single-mode `VastWriter`s — delete, update, insert —
on the one transaction, each with its own queue, Arrow schema, chunk size and background writer, and
one shared rollback action that can fire at most once. A MERGE with only `WHEN NOT MATCHED` actions is
left alone: Spark turns it into a plain `AppendData`, which already works. MERGE with an INSERT action
on a partitioned table is refused (the insert would bypass the partitioned insert path); update/delete
only merges on partitioned tables go through the same builder as DELETE/UPDATE. A target with row
filters or column masks is refused, as `UPDATE` is.

`MERGE_DESIGN.md` at the repo root has the full analysis, the row layouts, and the per-trap notes.
It and this file are meant to be dropped before merging.

## Files

| file (both trees) | change |
|---|---|
| `VastTableMetaData` | `setForMerge()` / `isForMerge()` |
| `RowLevelMerge` (new) | `VastDeltaOperation` for `MERGE`, mirrors `RowLevelDelete` |
| `VastRowLevelOperationBuilder` | `MERGE` branch |
| `NDBMergeTarget` (new) | the unresolved marker node |
| `NDBParser` | security wrapper also adapts `MergeIntoTable` targets; an unaliased target is aliased with its plain table name |
| `NDBRowLevelResolutionRule` | star expansion, row id guard, partitioned guard, alias clean-up, unwrap |
| `NDBRCLSResolvedRelationAdaptorRule` | refuse RCLS-wrapped MERGE targets |
| `VastBatch`, `VastPartitionedTable` | `|| isForMerge()` in the two flag readers |
| `VastWriteFactory` | explicit `VastWriteMode` + rollback action on `VastWriter`; `VastMergeWriter`; injectable client supplier (transient, tests only) |

## What was tested and how

Environment: this machine has JDK 17 and 21 but no JDK 11 (the README's Spark build JDK). Arrow needs
`--add-opens` from JDK 17 on, and Mockito/byte-buddy 1.12 cannot instantiate on JDK 21, so the tests
were run on **JDK 17** with:

```
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./mvnw -pl plugin/spark3/spark35 -am test \
  -DargLine="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
             --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
```

and the same for `plugin/spark3/spark35-scala212`. `./mvnw -pl plugin/spark3/spark35 -am -DskipTests
package` also succeeds on JDK 21. Baseline before the change (JDK 17, same flags): `ndb-common` 129,
`spark-common` 6, `spark35` 113 tests, 0 failures. With the change: see the counts at the end of this
section.

New tests (TestNG, no cluster; the mock VAST server has no `QueryData`, so nothing executes):

* `TestVastRowLevelOperationBuilder` — `MERGE` builds `RowLevelMerge` and sets only the merge flag;
  DEC128 row id for sorted tables; `DELETE`/`UPDATE` unchanged.
* `TestNDBParserMergeInto` — only the target is suffixed and wrapped, for aliased and unaliased
  targets; an unaliased target (three-part and one-part identifiers) is aliased with its plain table
  name, a user alias is kept as is; a VAST-table source, a subquery source and a CTE source get the
  regular security treatment only; insert-only MERGE is left alone; `WHEN NOT MATCHED BY SOURCE`
  takes the row-level path; `DELETE`/`UPDATE` still suffix their target as before.
* `TestNDBRowLevelResolutionRuleMerge` — star expansion over data columns for INT64 and DEC128 row
  ids, case-insensitive column matching, missing source column error, explicit actions pass through
  with the row-id placeholder added to inserts, assignments to the row id refused (unresolved and
  resolved keys), partitioned table with INSERT refused / update-delete allowed, rule waits for
  unresolved target or source, MERGE without the marker untouched, a connector-resolved (bare)
  target keeps the parser's alias and its `tgt` qualifier.
* `TestVastMergeWriter` — a `VastWriteFactory` in MERGE mode with a Mockito `VastClient`: interleaved
  delete/update/insert calls with chunk size 2 end up in `deleteRows` / `updateRows` / `insertRows`
  with the right table path, the right Arrow schema (`$row_id` UInt64 or Decimal(38,0); update rows
  `[$row_id, k, v]`; insert payloads never carry the row id), the right row counts and chunking, and
  ascending row ids within a chunk — for INT64 and DEC128; only used contexts are created and an
  empty task commits; a changed row id in `update()` is refused for both widths; a background failure
  in one context surfaces from `close()` (as for the single-mode writer) and rolls the transaction
  back exactly once across contexts and repeated `close()` calls.
* `TestVastCatalog.testMerge*` — the real parser and analyzer against the mock VAST server: the
  headline upsert with stars resolves to a `WriteDelta` with a `RowLevelMerge` operation and the
  expected row / row-id projections (`[row_id, k, v]` / `[row_id]`); explicit assignments with CDC
  ordering and an unaliased target using `tgt.` qualifiers; **the cluster resolution path**
  (`testMergeUnaliasedTargetResolvedByConnector`: `VastCatalogTestUtils` answers an empty
  `RowColumnSecurityResponse`, so every VAST relation resolves through `NDBTablesResolutionRule` as a
  bare relation — the test first checks a plain `SELECT` has no `SubqueryAlias`, then that
  `MERGE INTO tgt USING src s ON tgt.k = s.k` and a temp-view source `USING src2 ON tgt.k = src2.k`
  resolve; this test failed before the alias fix with the cluster's `UNRESOLVED_COLUMN` error);
  subquery, CTE and inline-values sources; `WHEN NOT MATCHED BY SOURCE`; insert-only MERGE is an `AppendData` on a table without the
  row id; assignments to the row id are refused; column masks and row filters on the target are
  refused with the same message style as UPDATE; `DELETE`/`UPDATE` still resolve to
  `RowLevelDelete`/`RowLevelUpdate` with the same projections. These sit in `TestVastCatalog` because
  `NDBCommon.vastClient` is a static singleton that `clearConfig()` does not reset, so only one mock
  server port works per JVM (same reason the existing DELETE/UPDATE analysis tests are there).

Results (JDK 17, flags above), full module runs on the final tree: `spark35` 151 tests, 0 failures
(113 before the change + 38 new); `spark35-scala212` 150 tests, 0 failures (112 + 38; its
`TestVastCatalog` has one test fewer). `ndb-common` (129) and `spark-common` (6) are unchanged and pass.

## Cluster verification

The connector's unit tests cannot execute a write (the mock VAST server has no `QueryData`), so
end-to-end behaviour was verified by the author's test run against a real VAST cluster (Spark 3.5.1),
not by anything in this repository's test suite. Reported results, for a plain table and for a table
with sorted columns (DEC128 row ids):

* all 25 scenarios below pass on both tables, plus the string-key and partitioned-table checks
  (8 more);
* two 300k-row merges, one per table: 420,000 rows after the merge, 120,000 updated, 150,000
  inserted, no deleted row left;
* the only failures are the two unaliased-source cases noted under "Observed, not changed", which
  fail the same way without this PR.

The scenarios, so a maintainer can repeat them:

```sql
CREATE TABLE ndb.b.s.tgt (k INT, v STRING, n INT);
CREATE TABLE ndb.b.s.tgt_sorted (k INT, v STRING, n INT) TBLPROPERTIES ('sorted_by' = 'k');
CREATE TABLE ndb.b.s.src (k INT, v STRING, n INT, op STRING);
INSERT INTO ndb.b.s.tgt VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);
INSERT INTO ndb.b.s.src VALUES (2, 'B', 2, 'U'), (3, 'C', 3, 'D'), (4, 'd', 40, 'I');

-- upsert
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
-- expected: (1,a,10) (2,B,2) (3,C,3) (4,d,40)

-- partial SET leaves the other columns intact
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET v = s.v;

-- expressions using both sides
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET n = t.n + s.n;

-- CDC ordering
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET v = s.v, n = s.n
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT (k, v, n) VALUES (s.k, s.v, s.n);

-- only-insert (plain AppendData path), only-update, only-delete (exercise the lazy contexts)
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k WHEN NOT MATCHED THEN INSERT *;
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k WHEN MATCHED THEN UPDATE SET *;
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k WHEN MATCHED THEN DELETE;

-- sources: VAST table (above), temp view, subquery, unaliased target with table-qualified references
CREATE OR REPLACE TEMP VIEW src_v AS SELECT * FROM ndb.b.s.src;
MERGE INTO ndb.b.s.tgt t USING src_v s ON t.k = s.k WHEN MATCHED THEN UPDATE SET *;
MERGE INTO ndb.b.s.tgt t USING (SELECT k, v, n FROM ndb.b.s.src WHERE op <> 'D') s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
MERGE INTO ndb.b.s.tgt USING ndb.b.s.src s ON tgt.k = s.k WHEN MATCHED THEN UPDATE SET v = s.v;
CREATE OR REPLACE TEMP VIEW src2 AS SELECT * FROM ndb.b.s.src;
MERGE INTO ndb.b.s.tgt USING src2 ON tgt.k = src2.k WHEN MATCHED THEN UPDATE SET v = src2.v;
-- expected for both, from base (1,a,10) (2,b,20) (3,c,30): (1,a,10) (2,B,20) (3,C,30)

-- WHEN NOT MATCHED BY SOURCE
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- two source rows matching one target row: Spark's cardinality error, table unchanged
INSERT INTO ndb.b.s.src VALUES (2, 'dup', 0, 'U');
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k WHEN MATCHED THEN UPDATE SET *;

-- string keys with quotes and backslashes, NULL keys, empty source
CREATE TABLE ndb.b.s.tgt_s (k STRING, v INT);
INSERT INTO ndb.b.s.tgt_s VALUES ('it\'s', 1), ('a\\b', 2), (NULL, 3);
MERGE INTO ndb.b.s.tgt_s t USING (SELECT 'it\'s' AS k, 10 AS v UNION ALL SELECT 'a\\b', 20 UNION ALL SELECT NULL, 30) s
ON t.k = s.k WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
-- expected: (it's,10) (a\b,20) (NULL,3) (NULL,30)   (NULL never matches)
MERGE INTO ndb.b.s.tgt t USING (SELECT * FROM ndb.b.s.src WHERE 1 = 0) s ON t.k = s.k
WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;   -- no-op

-- several chunks per context and several tasks: e.g. 1M source rows with
--   spark.ndb.max_row_count_per_insert / _update / _delete set low, and a source with many partitions

-- failure injected mid-write leaves the table unchanged
--   (e.g. a source expression that throws for one row: CAST('x' AS INT) under ANSI, or a killed executor)

-- the same merge inside an explicit transaction, rolled back, leaves no trace
SELECT ndb.create_tx();
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src s ON t.k = s.k WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
SELECT ndb.rollback_tx();

-- plain DELETE and UPDATE still behave as before
DELETE FROM ndb.b.s.tgt WHERE k = 1;
UPDATE ndb.b.s.tgt SET n = n + 1 WHERE k = 2;

-- a user restricted by row/column security: MERGE into a table with a row filter or column mask
-- must fail with "Merge into table is not allowed by current VAST security policy rules",
-- and a MERGE whose *source* is such a table must see only what a SELECT sees.
```

The two points the mock server could not cover are part of that run: the `UpdateRows` request built
for MERGE updates (schema `[$row_id, c1 … cN]`, same as UPDATE today) was accepted on the sorted
(DEC128 row id) table, and MERGE with an INSERT action on a partitioned table is refused at analysis
("not supported on partitioned table") before any write happens.

Things a maintainer should know:

* Scan and write use different transactions unless an explicit `create_tx()` is in effect — existing
  DELETE/UPDATE behaviour, unchanged.
* Each context is a full `VastWriter`: a MERGE task can run up to three background writers and
  three 2-thread executors.
* The `INSERT` context reuses the by-column inserter and `nonUpdatableColumns` exactly like a plain
  insert. UPDATE does not enforce `nonUpdatableColumns` client-side today, and neither do MERGE updates.

## Follow-ups (not in this PR)

* Scope the explicit transaction (`NDB.alterTransaction`, `SparkContext.executorEnvs["tx"]`) to the
  Spark session instead of the context — real problem for Spark Connect, separate PR.
* Expose the row id as a metadata column (`SupportsMetadataColumns`) instead of a schema column.
  `LogicalPlan.resolve` falls back to `metadataOutput`, which is how Iceberg's `_file`/`_pos` row ids
  work; it would remove the marker node, the star expansion and the placeholder assignment, but it
  changes `VastTable.schema()`, the scan builder, the reader column order and the UPDATE row layout.
* MERGE inserts on partitioned tables: the insert context already applies the partition transforms
  like a plain insert; what is missing is the clustered distribution `VastPartitionedWriteBuilder`
  requests, which needs a cluster to validate.
* Table-name qualifiers for relations the connector resolves itself (`SELECT src.v FROM ndb.b.s.src`,
  an unaliased MERGE source, unaliased DELETE/UPDATE targets): wrap the relation
  `NDBTablesResolutionRule` resolves in `SubqueryAlias(<table>, <catalog.namespace>)` above the
  row-filter/column-mask wrappers, as Spark's own table lookup does. Separate PR with its own tests
  (SELECT, joins, views, filtered/masked tables, DELETE/UPDATE); this PR's parser-level target alias
  becomes redundant then and can be removed there.
* `spark34`, Trino.

## Observed, not changed

* `NDBParser`'s DELETE/UPDATE branch adds the row-level-op suffix to *every* `UnresolvedRelation` in
  the statement, including relations in subqueries, and bypasses the CTE / non-VAST-provider
  exclusions of the security wrapper. MERGE does not copy this.
* `NDBRowLevelResolutionRule`'s `DeleteFromTable` branch builds a copy of the plan and discards it.
* `VastWriter.commit()` checks for a background failure only before flushing; a failure that happens
  while it waits surfaces from `close()` (which fails the Spark task). `AwaitableCompletionListener`
  re-runs the failure actions on every `assertFailure()`, so the single-mode rollback can be invoked
  more than once; `VastMergeWriter` guards its shared action, the single-mode path is untouched.
* `VastWriter.update()` compares the ids with `getLong(0)` regardless of the row id width; for DEC128
  both sides read the same underlying column so it passes, but it is not a real check. The merge
  writer's check is width-aware.
* `VastTable.partitioning()` is an empty array, never null, so `VastWriteFactory` computes
  `partitionIndices` for every non-partitioned table and plain inserts go through the
  `partitionedCtxs` machinery with an empty key whenever `partitioned_insert` is on (the default).
* `NDBCommon.vastClient` is a static singleton not reset by `clearConfig()`; tests that need a mock
  server must share `TestVastCatalog`'s.
* Table-qualified references to an unaliased VAST relation do not resolve today. On a cluster
  `NDBTablesResolutionRule` resolves every VAST relation to a bare relation with no alias, so
  `SELECT src.v FROM ndb.b.s.src` fails, and so does an aliased-target MERGE with an unaliased VAST
  source (`MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src ON t.k = src.k`). On the Spark resolution path
  (mock server) the alias carries the suffixed lookup identifier, so unaliased DELETE/UPDATE targets
  fail the same way: `DELETE FROM ndb.buck.schem.tgt WHERE tgt.k = 1` and
  `UPDATE ndb.buck.schem.tgt SET v = 'x' WHERE tgt.k = 1` give `UNRESOLVED_COLUMN` (Spark suggests
  ``tgt VAST_DB_ROW_LEVEL_OP`.`k``), `DELETE FROM ndb.buck.schem.tgt AS t WHERE t.k = 1` works
  (verified with a throwaway test, not kept). This PR only aliases the MERGE target; sources,
  SELECT, DELETE and UPDATE are left as they are.
* With `spark.ndb.max_row_count_per_insert=5000`, a plain 300k-row `INSERT` stores 20,000 all-NULL rows
  and loses 20,000 real ones (found on a cluster while testing this PR). Upstream `master` behaves the
  same and the default chunk size is fine, so it is not caused by this PR; it is reported separately.
