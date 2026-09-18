# Spark 3.5: alias the VAST relations the connector resolves with their table name

Branch `claude/connector-table-qualifiers`, stacked on the MERGE INTO branch (`claude/spark35-merge-into`).
Both trees, `spark35` and `spark35-scala212`.

## What and why

Spark's `ResolveRelations` wraps every relation it resolves in `SubqueryAlias(catalog.namespace.table)`;
that alias is what makes `table.column` references resolve. On a real cluster `VastCatalog.loadTable`
treats an empty, non-null masked-columns map as row/column security and refuses every RCLS-suffixed
lookup, so Spark never resolves a VAST relation: `NDBTablesResolutionRule` does, and it returned the
bare `DataSourceV2Relation`. Consequences, all found while testing the MERGE PR on a cluster:

* `SELECT src.v FROM ndb.b.s.src` and joins by table name fail with `UNRESOLVED_COLUMN`;
* `DELETE FROM ndb.b.s.tgt WHERE tgt.k = 1` / `UPDATE ndb.b.s.tgt SET … WHERE tgt.k = 1` fail
  (on the mock-server path they fail too: the alias Spark adds carries the lookup suffixes, and
  the adaptor rule dropped the alias of a DELETE target altogether);
* `MERGE INTO … USING ndb.b.s.src ON t.k = src.k` (unaliased VAST source) fails — the two
  "pre-existing" failures of the MERGE PR's cluster run;
* a view whose query qualifies columns by table name cannot be used.

The MERGE PR worked around this for the MERGE target only (a parser-level alias). This PR fixes the
resolution itself and removes that workaround. Migrated Databricks workloads use table names as
qualifiers all the time, so this matters for the migration story as much as MERGE does.

## Design

1. **`NDBTablesResolutionRule`** — the relation resolved for an `UnresolvedRelation` is wrapped in
   `SubqueryAlias(AliasIdentifier(<table>, [<catalog>, <namespace…>]))`, the exact shape Spark
   produces (`catalog.name +: ident.asMultipartIdentifier`) and the shape the rule already produces
   for views. The alias sits *above* the row-filter / column-mask wrappers, as it does for a view.
   The namespace comes from `VastNamespaceResolver`, so a relative name after `USE ndb.b.s` gets
   the full `ndb.b.s.tgt` qualifier. The `UnresolvedTableOrView` path of the rule is untouched.
2. **`NDBRCLSResolvedRelationAdaptorRule`** —
   * `adaptIdentifier` also trims the row-level-op suffix, so on the Spark resolution path the
     alias of a DELETE / UPDATE / MERGE target is `tgt`, not `tgt VAST_DB_ROW_LEVEL_OP`;
   * the `DeleteFromTable` branch looks *through* the target's aliases (`EliminateSubqueryAliases`,
     exactly what Spark's `RewriteDeleteFromTable` does) instead of removing them from the plan:
     a plain relation is left alone (Spark eliminates the alias itself when it rewrites), a column
     mask is refused as before, a row filter is merged into the DELETE condition as before, with
     the alias kept around the relation. The resulting `WriteDelta` is the same as today; the
     difference is that the alias is still there when `ResolveReferences` resolves the condition
     in the next iteration. Until now the alias was dropped in the same iteration the relation was
     resolved, before Spark ever saw it, which is why `WHERE tgt.k = 1` could not work on either
     path. The UPDATE and MERGE branches only inspect the target and are unchanged.
3. **`NDBParser`** — the parser-level alias for an unaliased MERGE target is removed; the relation
   is aliased by whichever rule resolves it. `NDBRowLevelResolutionRule` still cleans the suffixed
   alias on the Spark path.

**Row/column security.** Today a DELETE on a row-filtered table is allowed with the filter ANDed
into the DELETE condition, a DELETE on a column-masked table is refused, UPDATE and MERGE are
refused on either. The alias must not be able to hide a wrapper from that logic, so the invariant
is: *look through every alias first, then decide on what is underneath, in one application of the
rule.* `NDBRCLSResolvedRelationAdaptorRule` runs in the same Resolution-batch iteration as
`NDBTablesResolutionRule`, after it, so it sees the wrappers the moment they exist, and before
Spark's `RewriteDeleteFromTable` / `RewriteUpdateTable` / `RewriteMergeIntoTable` (which run
earlier in the next iteration) can rewrite anything:

* DELETE: `EliminateSubqueryAliases(target)` is a `Project` → refuse; a `Filter` → its condition
  is ANDed into the DELETE condition and the `Filter` node removed from the plan, every alias
  above it kept; a relation → nothing to do (Spark eliminates the alias itself in the rewrite).
* UPDATE: unchanged — looks through the aliases, refuses `Project` or `Filter`.
* MERGE: unchanged — unwraps the marker and the aliases, refuses `Project` or `Filter`.

The old DELETE branch removed a `SubqueryAlias` in one application and handled the wrapper it
uncovered in a later fixed-point iteration. That was safe only because Spark does not rewrite a
DELETE whose child is a `Filter`/`Project`, and it threw the alias away before Spark had resolved
the condition, which is why `DELETE FROM tgt_f AS t WHERE t.k = 1` could not resolve on the
cluster path either. Sources are not DML targets: a MERGE source (table, subquery) on a secured
table keeps its wrappers inside the rewritten plan exactly as a SELECT does.

Not changed, on purpose: INSERT targets (`InsertIntoStatement.table` is not a plan child, so
neither the parser nor this rule ever sees it; Spark resolves it and strips the alias itself,
`AppendData.table` stays the bare relation), the row/column security policy for DELETE / UPDATE /
MERGE, views (already aliased), `spark34`.

## Files

| file (both trees) | change |
|---|---|
| `ndb/view/NDBTablesResolutionRule` | `aliasedWithTableName`, applied on the `UnresolvedRelation` path |
| `ndb/NDBRCLSResolvedRelationAdaptorRule` | plain table name in the alias; DELETE keeps the alias |
| `ndb/NDBParser` | parser-level MERGE target alias removed |
| `test/java/ndb/TestNDBRCLSResolvedRelationAdaptorRule` (new) | unit tests for the adaptor |
| `test/java/spark/sql/catalog/ndb/TestVastCatalog` | analysis tests on both resolution paths |
| `test/java/ndb/TestNDBParserMergeInto`, `TestNDBRowLevelResolutionRuleMerge` | expectations follow the parser change |

## Tests

All TestNG, no cluster; the analysis tests run the real Spark analyzer (`CommandExecutionMode.SKIP`)
against the mock VAST server. They live in `TestVastCatalog` for the same reason the MERGE tests do:
`NDBCommon.vastClient` is a static singleton, one mock port per JVM.

The connector resolution path is reproduced with `VastCatalogTestUtils`, which answers an empty,
non-null security response for every table; the Spark resolution path is the mock server's default.

* `TestNDBRCLSResolvedRelationAdaptorRule` (6) — a suffixed lookup alias (RCLS suffix, RCLS +
  row-level-op suffix) is renamed to the plain table name with its qualifier kept, a plain alias is
  left alone; a DELETE keeps the alias of its target; a row filter under the alias is merged into
  the DELETE condition and the alias kept (and merged as before without an alias); a column mask
  under the alias is refused for DELETE, a row filter or a column mask under the alias is refused
  for UPDATE, an UPDATE on a plain aliased relation is left alone. Nested aliases (the user's
  above the connector's): a row filter underneath is merged in one application with both aliases
  kept, a column mask refused, UPDATE refused on either. A MERGE target with a row filter or a
  column mask is refused bare, under the connector's alias and under a user alias above it, with
  and without the marker; a plain aliased MERGE target is left alone.
* `TestVastCatalog`, connector path (7): `tgt.k`, `schem.tgt.k`, `buck.schem.tgt.k` and
  `ndb.buck.schem.tgt.k` all resolve, exactly one alias `tgt` with qualifier `[ndb, buck, schem]`
  directly above the relation whose identifier has no suffixes, and the same after
  `USE ndb.buck.schem` for `SELECT tgt.k FROM tgt`; a join by table names, a self join with user
  aliases (four aliases, the user alias above the connector's, `tgt.k` refused because the user
  alias hides it); a row filter stays a `Filter` and a column mask a `Project` under the alias;
  DELETE and UPDATE with `k = 1`, `tgt.k = 1`, `schem.tgt.k = 1` and with `concat(tgt.v, 'x')` in
  the SET clause, with a user alias, and `tgt.k` refused under a user alias; the row filter is
  merged into the DELETE condition with and without a qualifier, UPDATE on a filtered table and
  DELETE / UPDATE on a masked table and MERGE on a filtered table are refused with the same
  messages as before; MERGE with an unaliased VAST source (explicit actions, and `SET *` /
  `INSERT *` with an unaliased target too), insert-only MERGE and plain INSERT keep the bare target
  and get an aliased source; a view whose query uses `tgt.k` can be created and queried with
  `v1.key`, with the `tgt` alias inside.
* `TestVastCatalog`, connector path, row/column security (2): for a row-filtered (`k > 10`) and a
  column-masked table, three target forms — unaliased with unqualified columns, unaliased with
  table-qualified columns, `AS t` with `t.`-qualified columns: DELETE on the filtered table gives
  a `WriteDelta` whose condition contains both the user's predicate and `k > 10`; DELETE on the
  masked table, UPDATE on either, MERGE on either (explicit actions, and `SET *` / `INSERT *`)
  are refused with the existing messages. A MERGE source on the filtered table keeps the `Filter`
  on the source side of the rewritten plan (table source, subquery source, and the insert-only
  `AppendData`), on the masked table the `Project` with the mask, as a plain SELECT does.
* `TestVastCatalog`, Spark path (1): DELETE / UPDATE / MERGE by table name.
* `testMergeUnaliasedTargetResolvedByConnector` now asserts the alias instead of its absence.
* `TestNDBParserMergeInto`: an unaliased target is a bare `UnresolvedRelation` under the marker.

**Fails before / passes after**, both trees, same command with the targeted `-Dtest` filter:
34 tests, 15 failures before the change (every new analysis test and `testMergeUnaliasedTarget…`
with `UNRESOLVED_COLUMN`, the view test with `TABLE_OR_VIEW_NOT_FOUND`, the adaptor's DELETE and
suffix tests, the two parser tests), 0 after. The four security tests, run against the MERGE
branch's main sources with only the tests added: the unqualified forms of the matrix pass there
too (a row filter is merged, the rest refused — the same outcomes this PR keeps), the
table-qualified forms fail with `UNRESOLVED_COLUMN`, the secured-source test fails the same way
on its first MERGE, the nested-alias unit test fails (no merge in one application), the
MERGE-target unit test passes (that branch is unchanged); 0 failures after.

## Results

JDK 17 (no JDK 11 on this machine) with Arrow's `--add-opens`, offline, one Maven JVM at a time:

```
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 ./mvnw -o -pl plugin/spark3/spark35 test \
  -DargLine="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
             --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
```

and the same for `plugin/spark3/spark35-scala212`.

| module | before (MERGE branch) | after |
|---|---|---|
| `spark35` | 151 tests, 0 failures | 166 tests, 0 failures |
| `spark35-scala212` | 150 tests, 0 failures | 165 tests, 0 failures |

Checkstyle runs in the same build and passes.

## Cluster verification

Run in a second session against a real cluster (VAST 5.5.0.1, Spark 3.5.1 `local[2]`, JDK 11; the
`spark35` build of this branch at 6c19016 and of the MERGE branch at bd08870), not by anything in
this repository's test suite. The mock-server tests above stop at analysis.

* The MERGE PR's full regression on this branch: 27/27 on the plain and on the sorted table (the
  25 earlier checks plus the two unaliased-source MERGEs, which now pass), the string-key and
  partitioned checks, the 50k-row ANSI CAST failure (table unchanged), explicit-transaction commit
  and rollback, and the 300k/300k upserts on both tables (420,000 rows, 120,000 updated, 150,000
  inserted, no deleted key left).
* The table-name statements below: 13/13 on this branch, with row counts and values checked after
  every write. On the MERGE branch every table-qualified form fails with `UNRESOLVED_COLUMN`, and
  the view is not created (see "Observed"); `SELECT a.k, b.v … a JOIN … b` and
  `DELETE … WHERE k = 4` behave the same on both.
* Row/column security, on both branches, the session running as a restricted user (no
  impersonation) with a row filter `k > 10` on `tgt_f`, a column mask
  `regexp_replace(v, '[0-9]', '***')` on `tgt_m` and a column deny on `v` of `tgt_d`, an admin
  read-back after every statement and the visible rows restored between DELETEs: **no hidden row
  or value was read, deleted or updated on either branch**, and every form that resolves on both
  branches gives the same outcome. On this branch: SELECT sees only `k > 10` / masked `v`, with
  and without qualifiers; DELETE on `tgt_f` unqualified, table-qualified and `AS t` removes only
  visible rows (`AS t WHERE t.k <= 10` deletes nothing, `WHERE k = 5 OR k = 11` deletes only 11);
  UPDATE on `tgt_f`, DELETE and UPDATE on `tgt_m`, MERGE into either, with and without aliases,
  are refused with the existing messages; a MERGE source `tgt_f`, unaliased or aliased, updates
  and inserts nothing from the hidden rows, a subquery source matches only `k > 10`, a source
  `tgt_m` writes the masked values, a subquery source filtering on the raw value of the masked
  column matches nothing. The denied column is not exposed (`SELECT *` returns the other columns,
  `tgt_d.v` is unresolved, UPDATE/MERGE assigning it fail at analysis); a DELETE on the column-deny
  table fails at run time with the server's 403, nothing deleted (see "Observed").

Not verified on a cluster: the `spark35-scala212` build, and the end-user impersonation path
(`spark.ndb.enable_end_user_impersonation`).

The table-name statements, as run (`tgt` starts as `k = 1..5`, `src = (2,B,2),(3,C,3),(4,d,40)`):

```sql
-- SELECT / joins
SELECT tgt.k, tgt.v FROM ndb.b.s.tgt WHERE tgt.k > 0;
SELECT tgt.k, src.v FROM ndb.b.s.tgt JOIN ndb.b.s.src ON tgt.k = src.k;
SELECT a.k, b.v FROM ndb.b.s.tgt a JOIN ndb.b.s.tgt b ON a.k = b.k;
USE ndb.b.s; SELECT tgt.k FROM tgt;
-- DELETE / UPDATE by table name, and unchanged forms
DELETE FROM ndb.b.s.tgt WHERE tgt.k = 1;
UPDATE ndb.b.s.tgt SET v = concat(tgt.v, 'x') WHERE tgt.k = 2;
DELETE FROM ndb.b.s.tgt AS t WHERE t.k = 3;
DELETE FROM ndb.b.s.tgt WHERE k = 4;
-- MERGE: the two cases that failed in the MERGE PR's cluster run
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.src ON t.k = src.k
  WHEN MATCHED THEN UPDATE SET v = src.v WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v);
MERGE INTO ndb.b.s.tgt USING ndb.b.s.src ON tgt.k = src.k
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
-- INSERT unchanged
INSERT INTO ndb.b.s.tgt SELECT src.k, src.v FROM ndb.b.s.src;
-- a view with table-qualified columns
CREATE VIEW ndb.b.s.v1 AS SELECT tgt.k AS key, tgt.v FROM ndb.b.s.tgt WHERE tgt.k > 0;
SELECT v1.key FROM ndb.b.s.v1 WHERE v1.key = 1;
```

### Row/column security statements

The mock-server tests pin the *plan shapes* (filter merged into the DELETE condition, refusals,
wrappers on the source side); the cluster run above checked the resulting rows. Setup, following
the VAST "Row and Column Security" guide: an identity policy for a restricted user with a
`RowColumnSecurity` statement on
`b/s/tgt_f` (`RowFilter` `{"QueryEngine": ["Spark"], "FilterString": "k > 10"}`) and one on
`b/s/tgt_m` (`ColumnMask` `{"QueryEngine": ["Spark"], "ColumnName": "v", "MaskString":
"regexp_replace(v, '[0-9]', '***')"}`); `s3:TabularGetRowColumnSecurity` allowed for the
connector's credentials; `spark.ndb.enable_row_column_security` left at its default (`true`); the
Spark session either running with the restricted user's credentials, or with
`spark.ndb.enable_end_user_impersonation=true` and `spark.sql.session.user=<restricted user>`
(`s3:TabularEndUserImpersonation` allowed). Tables: `tgt_f` and `tgt_m` `(k INT, v STRING)` with
rows on both sides of `k = 10`, `src (k INT, v STRING, op STRING)`, `tgt` as in the MERGE PR.

As run by the restricted user on both branches, with the outcomes in the comments confirmed
(the MERGE branch has the bare relation, so the table-qualified forms fail there with
`UNRESOLVED_COLUMN`; the tester added an aliased twin of each, which behaves the same on both):

```sql
-- SELECT sees only k > 10 on tgt_f, masked v on tgt_m, qualified or not
SELECT * FROM ndb.b.s.tgt_f;
SELECT tgt_f.k FROM ndb.b.s.tgt_f WHERE tgt_f.k < 100;
SELECT tgt_m.v FROM ndb.b.s.tgt_m;
-- DELETE on the filtered table only touches visible rows: count the rows with k <= 10 as an
-- unrestricted user before and after, it must not change
DELETE FROM ndb.b.s.tgt_f WHERE k < 100;
DELETE FROM ndb.b.s.tgt_f WHERE tgt_f.k < 100;
DELETE FROM ndb.b.s.tgt_f AS t WHERE t.k < 100;
-- refused, with the existing messages
UPDATE ndb.b.s.tgt_f SET v = 'x' WHERE tgt_f.k = 11;   -- Update table is not allowed by current VAST security policy rules
DELETE FROM ndb.b.s.tgt_m WHERE tgt_m.k = 1;           -- Delete from table is not allowed ...
UPDATE ndb.b.s.tgt_m SET v = 'x' WHERE tgt_m.k = 1;    -- Update table is not allowed ...
MERGE INTO ndb.b.s.tgt_f USING ndb.b.s.src ON tgt_f.k = src.k WHEN MATCHED THEN DELETE;                 -- Merge into table is not allowed ...
MERGE INTO ndb.b.s.tgt_m t USING ndb.b.s.src s ON t.k = s.k
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;                                        -- Merge into table is not allowed ...
-- a MERGE source on a secured table sees what a SELECT sees
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.tgt_f ON t.k = tgt_f.k
  WHEN MATCHED THEN UPDATE SET v = tgt_f.v WHEN NOT MATCHED THEN INSERT *;   -- rows of tgt_f with k <= 10 update and insert nothing
MERGE INTO ndb.b.s.tgt t USING ndb.b.s.tgt_m ON t.k = tgt_m.k
  WHEN MATCHED THEN UPDATE SET v = tgt_m.v;                                  -- the written v values are the masked ones
MERGE INTO ndb.b.s.tgt t USING (SELECT * FROM ndb.b.s.tgt_f) s ON t.k = s.k WHEN MATCHED THEN DELETE;   -- only k > 10 matches
```

Column allow/deny policies are read by the connector (`ParsedRowColumnSecurity`) but produce no
plan wrapper, so the alias does not interact with them; the cluster run confirmed the denied
column stays unexposed on both branches.

## Follow-ups (not in this PR)

* Two-part names (`schem.tgt`): `VastNamespaceResolver` maps them to the namespace `[schem]`, so
  they do not resolve with the two-level VAST namespace; not touched, not tested here.
* The MERGE PR's `MERGE_DESIGN.md` (§1d, §3.4) and `PR_DESCRIPTION.md` describe the parser-level
  target alias this PR removes; both files are that PR's and are meant to be dropped before merging.

## Observed, not changed

* `DELETE FROM <temp view or non-VAST relation>`: read from the code, not run. The tables rule
  returns the plain `UnresolvedRelation` when its own lookups fail, and the adaptor's DELETE branch
  then throws a GENERAL "Unexpected child class" `VastRuntimeException` in the same iteration,
  before Spark can resolve the relation. This PR keeps that path exactly as it was.
* `CREATE VIEW` analyses the view query with `Analyzer.execute` and no `checkAnalysis`. On the
  mock server a view whose query does not resolve is created anyway and the first SELECT on it
  fails with `TABLE_OR_VIEW_NOT_FOUND`, because the tables rule swallows the view-query analysis
  error and falls back to a table lookup (seen in the before-run of the view test); on the cluster
  the MERGE branch reported success for the same `CREATE VIEW` and no view existed afterwards.
  Same on both branches; this PR only makes such a query resolve.
* DML on a table with a column-deny policy is not refused by the connector: a DELETE fails at run
  time with the server's 403 ("failed to delete rows, some columns are not allowed"), nothing
  deleted; an UPDATE or MERGE assigning the denied column fails at analysis because the column is
  not exposed. Safe, but unlike row filters and masks there is no connector-level refusal message.
  Same on both branches.
* The `UnresolvedTableOrView` path of `NDBTablesResolutionRule.resolveRCLSTableScanPlan` still
  returns a bare relation; it is only reachable with a literally RCLS-suffixed name.
