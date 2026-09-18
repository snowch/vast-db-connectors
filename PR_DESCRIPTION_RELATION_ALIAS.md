# Spark 3.5: alias the VAST relations the connector resolves with their table name

Branch `claude/vast-relation-alias`, stacked on the MERGE INTO branch (`claude/zealous-davinci-6ptnat`).
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

* `TestNDBRCLSResolvedRelationAdaptorRule` (4) — a suffixed lookup alias (RCLS suffix, RCLS +
  row-level-op suffix) is renamed to the plain table name with its qualifier kept, a plain alias is
  left alone; a DELETE keeps the alias of its target; a row filter under the alias is merged into
  the DELETE condition and the alias kept (and merged as before without an alias); a column mask
  under the alias is refused for DELETE, a row filter or a column mask under the alias is refused
  for UPDATE, an UPDATE on a plain aliased relation is left alone.
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
* `TestVastCatalog`, Spark path (1): DELETE / UPDATE / MERGE by table name.
* `testMergeUnaliasedTargetResolvedByConnector` now asserts the alias instead of its absence.
* `TestNDBParserMergeInto`: an unaliased target is a bare `UnresolvedRelation` under the marker.

**Fails before / passes after**, both trees, same command with the targeted `-Dtest` filter:
34 tests, 15 failures before the change (every new analysis test and `testMergeUnaliasedTarget…`
with `UNRESOLVED_COLUMN`, the view test with `TABLE_OR_VIEW_NOT_FOUND`, the adaptor's DELETE and
suffix tests, the two parser tests), 0 after.

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
| `spark35` | 151 tests, 0 failures | 163 tests, 0 failures |
| `spark35-scala212` | 150 tests, 0 failures | 162 tests, 0 failures |

Checkstyle runs in the same build and passes.

## Not verified — needs a cluster

Nothing here executed against VAST. The mock server has no `QueryData`, so every test stops at
analysis. Things a maintainer should run on a cluster, on a table with row/column security enabled
for the session (the connector path) and on one without:

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
-- row filter on tgt_f (e.g. k > 10): the DELETE must only touch rows the filter lets through
DELETE FROM ndb.b.s.tgt_f WHERE tgt_f.k < 100;   -- then count the rows with k <= 10: unchanged
UPDATE ndb.b.s.tgt_f SET v = 'x' WHERE tgt_f.k = 1;   -- refused
-- column mask on tgt_m: DELETE and UPDATE refused, SELECT tgt_m.v returns the masked value
-- INSERT unchanged
INSERT INTO ndb.b.s.tgt SELECT src.k, src.v FROM ndb.b.s.src;
-- a view with table-qualified columns
CREATE VIEW ndb.b.s.v1 AS SELECT tgt.k AS key, tgt.v FROM ndb.b.s.tgt WHERE tgt.k > 0;
SELECT v1.key FROM ndb.b.s.v1 WHERE v1.key = 1;
```

The MERGE PR's own checklist should be re-run as well, since its target alias now comes from the
resolution rule instead of the parser.

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
* `CREATE VIEW` analyses the view query with `Analyzer.execute` and no `checkAnalysis`, so a view
  whose query does not resolve is created anyway; the first SELECT on it fails with
  `TABLE_OR_VIEW_NOT_FOUND`, because the tables rule swallows the view-query analysis error and
  falls back to a table lookup. Seen in the before-run of the view test (the query used `tgt.k`).
* The `UnresolvedTableOrView` path of `NDBTablesResolutionRule.resolveRCLSTableScanPlan` still
  returns a bare relation; it is only reachable with a literally RCLS-suffixed name.
