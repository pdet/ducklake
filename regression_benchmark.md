# DuckLake regression benchmark plan

Updated on 2026-10-01 after implementing the four local additions to the 13-benchmark suite from `dc893f72`.

Keep the blocking suite small. A new benchmark should expose a material cost that the current suite cannot detect,
not just exercise another SQL spelling or repeat a workload at a slightly different size.

## Recommendation

The blocking list now has 17 cases. The four additions cover:

1. **Wide Parquet-backed commits:** per-column metadata and small-file commit overhead.
2. **Productive inlined flush:** moving inlined rows and file deletions into files.
3. **MERGE upserts:** matching, updates, inserts and the resulting commit.
4. **Partitioned compaction:** many independent compaction groups and output files.

Each addition has deterministic result checks, a pristine-state reset, and a minimal sqltest covering repeated
restores. The regression threshold is unchanged. Local validation is recorded below; repeated calibration on the
actual Linux CI runner is still needed to establish noise and runtime headroom.

**PostgreSQL performance is the next separate benchmark suite.** Start with its wide-table commit path, selective
file-list queries and compaction metadata work. PostgreSQL setup and measurements are deferred to that follow-up;
SQLite performance is outside this plan. Do not expand the local gate with more TPC-H or catalog-listing variants
without evidence that they expose a distinct cost.

## Current blocking coverage

[`.github/regression/micro.csv`](.github/regression/micro.csv) is the authoritative list. Paths below are relative to
`benchmark/`. Fixture construction, result verification, and resets are outside the measured interval.

| Benchmark | Distinct coverage | Timing and reset |
|---|---|---|
| `micro/show_all_tables.benchmark` | Cold catalog access across 2,000 tables | Reattach between runs; attach itself is untimed |
| `micro/compaction/merge_adjacent_small_files.benchmark` | Merge 1,000 files, including metadata commit | Restore pristine metadata and remove newly written files |
| `micro/compaction/merge_adjacent_partitioned.benchmark` | Merge 300 files across 100 partitions into 100 outputs, including commit | Freshly attached lake; restore pristine metadata and remove newly written files |
| `micro/compaction/rewrite_data_files_deletes.benchmark` | Rewrite 500 files with positional deletes, including commit | Restore pristine metadata and remove newly written files |
| `micro/maintenance/expire_snapshots.benchmark` | Expire 1,005 of 1,006 snapshots and clean up metadata | Roll back; commit is excluded |
| `micro/maintenance/flush_inlined_data.benchmark` | Flush 4,000 inlined rows and merge inlined deletes into 10 delete files | Freshly attached lake; time binding and commit; restore pristine metadata and sweep new files |
| `micro/time_travel/at_version_history.benchmark` | Version/timestamp resolution, historical schemas and deletes across 300 commits | Read-only; reattach for cold catalog caches |
| `micro/time_travel/table_changes_incremental_poll.benchmark` | Narrow change windows over a long history | Read-only; keep catalog warm |
| `micro/read/warm_small_queries.benchmark` | Repeated small queries across several catalog/data shapes | Read-only; keep catalog warm |
| `micro/read/file_pruning_many_files.benchmark` | Selective filters over 4,096 files | Read-only; keep catalog warm |
| `micro/write/insert_commits_inlined.benchmark` | 50 small autocommit inserts in a 510-table catalog | Restore metadata and warm the catalog before the next run |
| `micro/write/wide_table_insert_commits.benchmark` | Eight single-row Parquet commits into 120 mixed scalar columns | Restore metadata and sweep files; first INSERT includes cold schema loading |
| `micro/write/ctas_lineitem.benchmark` | Bulk Parquet writing, statistics and commit at TPC-H SF0.5 | Restore an empty lake and remove new output files |
| `micro/write/delete_lineitem.benchmark` | Row-level deletion across the SF0.5 data files | Roll back; commit is excluded |
| `micro/write/merge_into_upsert.benchmark` | 150k updates and 150k inserts into a 1M-row target, including commit | Freshly attached lake; native in-memory source; restore target metadata and sweep output files |
| `ingest/add_files_small_files.benchmark` | Register 1,000 external Parquet files and their metadata | Restore metadata; source files stay outside DATA_PATH |
| `tpch/q09.benchmark` | Representative analytical scans, joins and statistics-sensitive planning | Shared SF1 fixture; read-only query workload |

The no-op CHECKPOINT and cleanup-listing benchmarks, and TPC-H Q1/Q5/Q18 benchmark files, were removed. Do not
restore them to the gate by default. The `many_files` fixture now contains only `events`: 4,096 files and 8,388,608
rows. It no longer constructs a dropped churn table or orphan files.

### What the gate means

[The workflow](.github/workflows/Regression.yml) builds base and PR binaries, then runs
[the wrapper](.github/regression/run.py) with `--threads 2 --early-stop`. Both binaries execute the PR's benchmark
definitions. SQL and options used by a candidate must therefore also work on the comparison base.

The inner runner compares medians from alternating batches, with additional samples for candidates outside its
noise band. The wrapper reruns regressed entries and fails if **any one benchmark is at least 10% slower in both
comparisons**. Execution and result errors also fail the job. Improvements elsewhere cannot offset a regression.

The unit is a CSV entry, not each SQL statement within it. A slowdown in one of the 61 warm queries can disappear
inside the total batch time. Keep batching for short operations, but group queries by the cost being protected and
use deterministic assertions for important small subcases.

## First improve the evidence from the existing suite

These checks often add more value than another timing benchmark:

- **Read results:** the warm-query and pruning batches validate only their first result. Later SQL errors propagate,
  but later wrong answers are not independently checked. Add minimal sqltests for the distinct query shapes or
  equivalent untimed checks; do not turn the timing batch into a collection of unrelated validation work.
- **Compaction results:** check surviving row counts and deterministic checksums, not only processed/created file
  counts. Verify historical reads where the workload is meant to preserve them.
- **Pruning:** pair timings with untimed files-read assertions. Correct answers alone cannot distinguish a pruned
  scan from opening every file. Existing examples include
  [complex filters](test/sql/stats/complex_filter_pushdown.test) and
  [bucket pruning](test/sql/partitioning/bucket_pruning.test).
- **Metadata query counts:** reuse
  [record-count cache tests](test/sql/metadata/duckdb_tables_record_counts.test) and
  [extended file-list query tests](test/sql/delete/extended_file_list_metadata_query.test).
  The warm batch's final `duckdb_tables()` query is a small part of the total; an exact query-count assertion is a
  better guard for accidental per-table metadata queries.
- **Metadata-only aggregates:** reuse the existing COUNT/MIN/MAX plan tests before proposing another timing case.
  MIN/MAX deliberately fall back to scanning when deletes make global extrema insufficient; COUNT can still use
  metadata. A test demanding metadata-only MIN/MAX after deletion would reject correct behavior. See
  [MIN/MAX with deletes](test/sql/stats/min_max_optimization_deletes.test).

## Newly implemented local benchmarks

These scales keep the timed operations focused. Fixture construction, source generation, validation and resets are
untimed. Each case uses the shared `rblob_reload` and `rblob_cleanup` helpers, with a versioned seed marker.

### 1. Wide Parquet-backed commits

[Benchmark](benchmark/micro/write/wide_table_insert_commits.benchmark) ·
[SQLtest](test/sql/benchmark/wide_table_insert_commits.test)

The existing commit loop writes inlined rows. CTAS writes a large batch with one commit and relatively few columns.
Neither isolates the per-file, per-column metadata work of frequent commits into a wide table.

- Eight single-row autocommit inserts into a 120-column table, alternating BIGINT, DOUBLE and VARCHAR columns.
- Persisted `data_inlining_row_limit = 0` forces the Parquet path. Source rows are prepared in a native temporary
  table before timing.
- Each run starts with the target reattached: the first INSERT includes loading its schema, and all eight include
  their commits. This measures a complete small-write batch, rather than only warm commit calls.
- Verification compares every row and column with the source, and checks eight files, 120 columns and the
  expected snapshot delta. The sqltest also checks the ordered column types, the persisted option and two
  write/reset cycles.

This replaces the separate narrow Parquet commit proposal. A narrow variant can help profile width sensitivity,
but does not need its own CI entry. Nested/VARIANT columns remain separate diagnostics.

### 2. Productive inlined flush

[Benchmark](benchmark/micro/maintenance/flush_inlined_data.benchmark) ·
[Fixture](benchmark/micro/fixtures/flush_inlined.benchmark.in) ·
[SQLtest](test/sql/benchmark/flush_inlined_data.test)

Small inserts previously stopped at inlined storage; existing compaction starts from Parquet. This case covers the
transition between them and the handling of inlined file deletions.

- Forty tables contain 100 inlined rows each. Ten more contain 1,000 Parquet-backed rows each; table k has 150 + k
  deletions in an existing delete file and 5 + k inlined deletions. Every table's values also depend on k, so a flush
  that mixes up tables changes the checksums. The persisted inlining limit is 100 rows.
- One catalog-wide `ducklake_flush_inlined_data` call is timed on a freshly attached lake, including binding and
  commit, so per-table first access is part of the timing.
- Validation checks the 40-table/4,000-row result, every table's current and original (snapshot 1) contents, each
  file-backed table at snapshots 2 and 3 (Parquet and inlined deletions), drained inlined deletions, 50 live data
  files, ten live delete files containing 1,640 deletions, and the committed snapshot.
- Restores preserve every seed data/delete file needed by those historical snapshots. The minimal sqltest repeats
  the same transition and restoration with one table of each kind.

Inlined file deletions are flushed during binding, and `rows_flushed` reports inlined data rows rather than all
work done by the call. A deletion-only flush can return no rows. The metadata and history checks protect that part
of the workload. If profiling shows one phase hides the other, use separate diagnostic variants first.

### 3. MERGE upserts with commit

[Benchmark](benchmark/micro/write/merge_into_upsert.benchmark) ·
[SQLtest](test/sql/benchmark/merge_into_upsert.test)

The previous 13 cases did not time UPDATE or MERGE. This case covers matching, updates, insertion and the resulting
commit in one common ingestion operation.

- A 1M-row Parquet-backed target with unique synthetic IDs and a native in-memory source outside the restored lake.
- The source contains 150k existing keys to update and 150k new keys to insert, with deterministic values and strings.
- One autocommit MERGE is timed through commit. Setup persists `data_inlining_row_limit = 0`.
- Checks pin 300k affected rows, 1.15M final rows and distinct IDs, numeric checksums, the expected value and payload
  of every row, and the committed snapshot: a missed reset still reports 300k affected rows, but commits a later
  snapshot. Setup asserts the one-file target layout that the timing depends on.
- The sqltest also checks the files the MERGE writes, the original snapshot and the committed snapshot delta across
  two resets.

Keep mixed multi-statement transactions deferred until they demonstrate a cost beyond this and the commit loops.

### 4. Partitioned compaction

[Benchmark](benchmark/micro/compaction/merge_adjacent_partitioned.benchmark) ·
[Fixture](benchmark/micro/fixtures/partitioned_compaction.benchmark.in) ·
[SQLtest](test/sql/benchmark/merge_adjacent_partitioned.test)

The existing merge produces one output group. A partitioned lake produces many groups and files, exercising
partition-value metadata, planning and commit work that a single-group merge cannot expose.

- One fixed schema with 100 identity partitions, three DuckLake-written files per partition and 2,048 rows per file:
  300 input files and 614,400 rows total. Fixture construction asserts this exact layout.
- Time one merge through commit; restore metadata and sweep output files between runs.
- Check 300 processed/100 created files, one live file per partition, current per-partition counts/checksums, the
  first insert snapshot, unchanged row ids against the last pre-merge snapshot, and the committed snapshot. The
  benchmark uses only public SQL; the three-partition sqltest repeats the merge/reset cycle and keeps the
  internal-metadata assertions.

Keep schema evolution as a separate diagnostic variant. Promote it only if schema-version count exposes a cost
that the fixed-schema partitioned workload cannot detect.

## Conditional later additions

These have distinct value, but should start as individual investigations or informational runs. Select the workload
that demonstrates a missing performance signal; do not automatically promote the whole table.

| Candidate | Gap and initial workload | Timing, reset and validation |
|---|---|---|
| `read/scan_many_delete_files` | Isolate reading 500 data files with 500 positional delete files using `compaction.rw`; rewrite mixes this cost with writing and commit | Warm read-only scan with `sum(id)` and `sum(length(s))` to force reading; validate 409,600 surviving rows and checksums; inspect file layout outside timing |
| `time_travel/table_changes_large_update` | Change volume rather than history depth: disjoint 500k-row UPDATE, 100k-row DELETE and 200k-row INSERT | Read-only warm `table_changes`, grouped by change type with counts/sums; no reset; pin 500k preimages, 500k postimages, 100k deletes and 200k inserts: **1.3M change rows** |
| `read/transformed_partition_pruning` | Bucket and month transforms with overlapping column stats, so ordinary min/max pruning cannot do all the work | Read-only selective batches; verify results and files read for each transform; reuse bucket/partition sqltests |
| `compaction/rewrite_inlined_deletions` | Existing rewrite explicitly disables inlining; its candidate selection does not cover inlined file deletions | Controlled variant of rewrite, including commit; restore seed; verify which files qualified and resulting row contents |
| `read/registered_file_scan` | Name-mapped external Parquet without DuckLake field IDs; registration currently times ingestion only | Read-only projection/filter queries after registration and rename; pin results and pruning; reuse `test/sql/add_files/add_files_rename.test` |

The large-change CDC example assumes the update, delete and insert populations are disjoint. An UPDATE emits both
preimage and postimage rows. Consume a deterministic aggregate inside the timed query rather than printing millions
of rows. Keep the input range explicit so expected counts remain meaningful.

The deleted-file scan is the first scan-throughput candidate. Defer separate clean-many-file and large-file variants
until profiling shows they protect a different bottleneck. One can reuse the existing `many_files` fixture for a
clean scan without adding new fixture data.

## Separate backend and concurrency coverage

A local DuckDB metadata catalog on a local filesystem cannot establish performance for other metadata engines or
remote storage. PostgreSQL is deferred to a dedicated follow-up suite. These tracks need explicit setup and isolation.

| Priority | Track | Initial subset and prerequisites |
|---|---|---|
| Follow-up suite | PostgreSQL metadata | Wide commits, pruning/file-list pushdown, and one compaction metadata workload. Reuse the PostgreSQL setup and scanner build in [Catalogs.yml](.github/workflows/Catalogs.yml). Give base and PR independent databases and data prefixes, with equivalent schema/index/data state. |
| Next | Controlled concurrent writers | Two synchronized connections with a fixed number of intervening commits; measure conflict checking/retry behavior and verify committed rows, snapshots and retry counts. Requires a coordinated harness, not sleep-based SQL scripts. |
| Next | S3-compatible storage | Selective reads over many files, one merge, and cleanup listing/unlink as diagnostics. Reuse [SeaweedFS.yml](.github/workflows/SeaweedFS.yml) and [start-docker.sh](scripts/start-docker.sh). Record requests, bytes and elapsed time; start informational. |

PostgreSQL has dedicated generated SQL and file-list pushdown paths; see
[its metadata manager](src/include/metadata_manager/postgres_metadata_manager.hpp). Existing
[extended file-list tests](test/sql/delete/extended_file_list_metadata_query.test) already show how to verify that
queries reach PostgreSQL rather than falling back to attached metadata scans. Reuse that evidence alongside timing.

The raw local-database copy reset does not apply to PostgreSQL. Backend benchmarks need an equivalent isolated seed
and a tested restore/recreation method outside timing. Do not let old/new binaries operate on one shared metadata
database or storage prefix.

For concurrency, distinguish conflict-processing cost from end-to-end throughput. A focused conflict microbenchmark
may disable retry waits to isolate metadata work; a workload benchmark should retain realistic retry behavior and
report it. Avoid changing the retry policy simply to make a timing look stable.

A local S3-compatible server exercises request behavior, not the latency/bandwidth of a real S3 deployment. Keep
cache policy explicit and use separate storage prefixes. Metadata-only restore is insufficient after physical
unlinking; rebuild or clone the complete physical fixture before the next sample.

## Candidates to consolidate or defer

| Earlier suggestion | Recommendation |
|---|---|
| More TPC-H queries, including Q1/Q5/Q18 | Keep Q9 in the gate. Add another only for a demonstrated DuckLake-specific regression that the targeted suite and Q9 miss. |
| No-op CHECKPOINT and local cleanup listing | Removed from the suite. Revisit only for a measured catalog-wide or path-discovery bottleneck; cleanup is more informative in the remote-storage track. |
| Narrow Parquet commit loop plus wide commit loop | The implemented wide case covers this; keep a narrow variant for calibration only. |
| COUNT/MIN/MAX timing batch, warm metadata listing variants | Prefer existing plan/query-count sqltests; add timing only if metadata work itself becomes the demonstrated bottleneck. |
| Several deleted-file and clean-file scan benchmarks | Start with one isolated many-delete-file scan; add another only after showing distinct sensitivity. |
| Many-schema merge, tiered merge, unrelated-metadata candidate selection | Focused scaling investigations. Keep scale dimensions separate and assert that the intended candidate path runs. |
| Retention DELETE, repeated DELETE, point DML, mixed/multi-table transactions | Useful later variants only if they add a cost beyond the implemented MERGE/upsert case. Whole-file retirement does not by itself prove scan-free deletion. |
| UI catalog queries, cold first query, views/macros, attach loops | Overlap with cold catalog or warm-query coverage unless a distinct scaling or memory issue is demonstrated. Memory leaks belong in bounded correctness/stress tests as well as diagnostics. |
| Productive CHECKPOINT | Its component work should first be covered directly. An end-to-end orchestration test can remain informational. |
| Puffin, encryption, nested/VARIANT, sorted tables | Add targeted correctness tests and workload-specific diagnostics when those features justify a performance baseline. |
| SF10 CTAS, 20k-file scans, 10k-file merges, many-thread variants | Optional scaling runs after smaller cases are stable. No nightly/extended benchmark job has been added by this plan. |

## Promotion criteria and CI budget

For the four additions, finish CI calibration before treating local results as a noise baseline. Before further
expanding the blocking CSV:

1. Identify the source path and scaling dimension that the current suite misses. Demonstrate sensitivity to a known
   or deliberately introduced slowdown in that path; a benchmark that mostly measures unrelated work is not enough.
2. Run repeated same-binary A/A comparisons on the actual Linux CI environment. Record ratio spread, confirmation
   frequency and false failures. A single successful comparison is not a stability measurement.
3. Exercise a fresh fixture, cached reload, warmup, and multiple measured/reset cycles. Pin results and structural
   invariants, including that each mutating repetition still performs the intended work.
4. Record the pinned DuckDB revision, DuckLake revision, hardware, threads, cache policy, fixture size, timed median,
   setup/reset time, and full wrapper wall time. Include the cost of confirmations and the second comparison.
5. Keep explicit runtime headroom. Leave uncertain or noisy cases informational; promote one change at a time and
   reassess overlap before increasing the blocking set.

The workflow timeout is 150 minutes. Available benchmark time depends on real build/cache behavior; do not subtract
an old laptop or single-build estimate and treat the result as a fixed CI budget. Likewise, do not multiply local
M-series timings by an assumed constant to predict Ubuntu runtimes.

If confirmation costs become excessive, first reduce or shard the suite while preserving each benchmark's two-pass
rule. Rerunning only the top N and passing everything else can miss a real regression. Failing solely because there
are more than N candidates also changes the current confirmation policy. A budget-exhausted run must be reported as
incomplete/failing, not as a clean performance result.

The workflow already saves ccache on `main`. Investigate actual hit rates before changing cache keys or claiming
build-time savings. Memory diagnostics should distinguish process peak RSS from `duckdb_memory()` snapshots; the
latter are not peak-process-memory measurements.

## Authoring and reset guide

Reuse [existing fixtures](benchmark/micro/fixtures) and the native benchmark runner before adding orchestration.
Read the pinned runner's behavior when changing directives; the local DuckDB checkout may differ from the CI pin.

### Choose the state being measured

- `load` constructs a fixture when its marker is absent; `reload` runs when the marker exists. This is per cache root,
  not a guarantee that initialization happens only once across CI invocations.
- The runner discards its first execution as warmup, then verifies each measured result before cleanup. Cleanup
  runs outside timing after warmup and successful measured runs. Reload must recover from an interrupted run too.
- Use reattachment for cold DuckLake catalog/schema/statistics caches. It does not clear OS or filesystem caches.
  Keep warm queries attached. State the choice in the benchmark description.
- Include commits when commit performance is part of the operation. An explicit transaction followed by untimed
  rollback excludes commit; do not describe it as an end-to-end write benchmark.
- Batch short operations only when they measure the same important cost. Keep correctness and profiling work outside
  timing, and verify the batch is not dominated by an unrelated query.

### Restore closed local metadata for committed mutations

The existing `rblob_reload` and `rblob_cleanup` includes detach the working lake, copy its pristine metadata, attach
it again, and remove files not referenced by the seed. The seed must be fully detached so its state is in the database
file rather than an outstanding WAL. This pattern supports merge, rewrite, CTAS and committed inserts.

```sql
DETACH DATABASE IF EXISTS lake;
COPY (SELECT content FROM read_blob('${BENCHMARK_DIR}/${lake}_pristine.db'))
TO '${BENCHMARK_DIR}/${work}.db' (FORMAT blob);
COPY (SELECT ''::BLOB AS content) TO '${BENCHMARK_DIR}/${work}.db.wal' (FORMAT blob);
ATTACH 'ducklake:${BENCHMARK_DIR}/${work}.db' AS lake;
CALL ducklake_delete_orphaned_files('lake', cleanup_all => true);
SELECT CASE WHEN (SELECT count(*) FROM glob('${BENCHMARK_DIR}/${lake}_files/**'))
<> (SELECT column0 FROM read_csv('${BENCHMARK_DIR}/${lake}_filecount.csv', header = false))
THEN error('the fixture files were not restored') END;
```

The empty WAL discards commits that a killed process left in the work copy's WAL; otherwise the next ATTACH replays
them onto the restored copy and every later process fails. A fixture load ends with this block verbatim, because the
first process runs `load` instead of `reload`.

The equality guard detects missing as well as surplus files. It does not replace row-content validation. Never use
metadata-only restore for an operation that physically deletes seed files, including cleanup with unlinking or a
CHECKPOINT that performs physical cleanup. Preserve/recreate the complete physical fixture for those workloads.

For rollback-based benchmarks, `BEGIN` belongs in `run` and `ROLLBACK` in cleanup. Verify within the open transaction,
then verify that the next iteration sees the original state. Reuse the expiration and DELETE cases as examples.

### Keep fixtures reproducible and isolated

- Use deterministic data and known row/file layouts. Assert the intended number of files and inlining state rather
  than trusting row-group/target-size settings across implementations.
- Set persisted options with `set_option` where the restored fixture depends on them. Attach-only options must be
  supplied again on every attach. Resolve timestamp-based snapshots separately in each fixture; wall-clock creation
  times will differ between base and PR.
- Write a lowercase, versioned completion marker after fixture construction and validation, before any common
  restore/attach suffix. Change its version when the fixture shape or meaning changes.
- Ensure `load` finishes in the same attached/options state as `reload`. Handle partial initialization explicitly;
  a marker's absence does not imply that a previous attempt left no files or tables behind. The `micro` fixtures and
  `add_files_small_files` start their load by copying a valid empty DuckDB file over the metadata file (a 0-byte
  file is rejected) and writing an empty WAL, and sweep orphan files before detaching the new seed. The empty file
  lives in its own `*_blank/` directory, which a `COPY ... (PARTITION_BY, OVERWRITE)` clears first, so a copy torn by
  a killed load is recreated. The TPC-H template and `show_all_tables` are not restartable yet: delete their cached
  database by hand after an interrupted load.
- Both comparison roots are independent on clean CI, but the runner symlinks preexisting local cache entries into
  them. Use fresh benchmark caches for comparisons or implement isolated copies of immutable seeds. Never run
  independent benchmark instances against the same mutable metadata or DATA_PATH.
- Fixture lakes store their DATA_PATH relative to the runner root (`duckdb_benchmark_data/...`). Inspect a cached
  fixture only from that root. Opening it from the repository creates `<repo>/duckdb_benchmark_data/`, which later
  local runs symlink into both comparison roots; delete that directory before running `run.py` locally.
- Keep optional diagnostics out of a blocking fixture's load path. Do not restore the removed churn/orphan data to
  `many_files`, or add every proposed workload's tables to `compaction` simply because those fixtures already exist.

### Runner syntax and validation pitfalls

- Put `#` comments between blocks. The runner joins block lines with spaces: SQL `--` comments can swallow the rest
  of a block, and `#` within a block reaches the SQL parser. A `run <file.sql>` is read verbatim instead.
- `run <file.sql>` does not substitute `${...}` arguments. Use it for fixed SQL or pass needed state through SQL
  variables where supported.
- Include/cache/run paths are lowercased by directive parsing. Keep paths lowercase.
- The first `argument` definition wins. Declare consumer arguments before includes; template `KEY=VALUE` parameters
  can be overridden by defaults declared inside the template.
- Avoid `USE lake` when cleanup needs to detach it. Use qualified names and idempotent attach/detach statements for
  secondary source databases.
- `__answer` exposes the first result, not a validation of every query in a multi-statement batch. Check important
  later results separately. A benchmark's successful timing is not proof of every intermediate result.
- Keep version/backend-specific metadata assertions in minimal sqltests where possible. Do not make the timed query
  spend most of its time inspecting its own fixture.

### Useful follow-ups before adding more workload

A smoke mode should exercise fresh load, cached reload, warmup and at least two measured/reset cycles for each case.
A single execution can miss a broken reset. A separate list/reference check can catch missing includes cheaply.

The older `ingest/add_files_lineitem` and `ingest/add_files_orders` templates are outside the gate. Their shared
`add_files.benchmark.in` still lacks the reset used by the small-file benchmark, and its `argument table_name lineitem`
overrides the orders template parameter. Fix or retire those definitions before using them as performance evidence.

Consider making `show_all_tables` reload/cleanup attach read-only to reduce untimed work, and using a completion
marker instead of the metadata file as its cache signal. Validate that a change preserves the intended cold-catalog
measurement; this is a harness improvement, not another benchmark.

## Validation and historical research

### Evidence available for the current work

- After the review fixes (empty-WAL restore, restartable fixture loads, stronger result checks), the four sqltests
  pass with **301 assertions** under every test config. Each leaves a stale work WAL that the next restore must
  discard, so they fail without the empty-WAL step. The full 17-entry same-binary A/A comparison through the
  regression wrapper passed in **194 seconds**, with every entry within 3% after the initial ten pairs and no
  confirmations.
- Every `micro` fixture and `add_files_small_files` was rebuilt after a SIGKILL during its load, and every committing
  benchmark recovered after a SIGKILL that left a non-empty WAL behind; with the previous restore block, the same kill
  failed every later process.
- Before the review fixes, all four new sqltests passed after `make format-fix`: **266 assertions**, including
  repeated fixture restoration.
- Each new benchmark passed a fresh fixture plus repeated measured/reset cycles with the native runner built from
  the CI DuckDB pin, `30ad316060f38aecd8ceee67f9984c96b3741756`, and current DuckLake sources. Cached reloads were also
  exercised. The Python regression runner files match that pin.
- Before the review fixes, the full 17-entry same-binary A/A comparison passed locally in **231.6 seconds**, using the
  actual regression wrapper, `--threads 2 --early-stop`, and independent fresh cache roots. All 17 finished within 3%;
  MERGE and small-file registration needed ten confirmation pairs each, and Q9 needed twenty. No benchmark reached the
  wrapper's second-comparison pass.
- After strengthening NULL handling in result checks, all four additions passed fresh and cached runs again, each
  with a warmup and three measured/reset cycles. The timed queries were unchanged.
- This is local macOS validation, not a base-versus-PR performance comparison or repeated Linux CI calibration.
  Neither PostgreSQL performance nor SQLite performance was measured.
- Earlier notes recorded a same-binary run of the **old 18-entry suite** on DuckDB `30ad316060`: 203 seconds locally.
  That remains historical evidence, not a CI runtime forecast for the current suite.
- Earlier pruning checks observed 0-3 of 4,096 files read by each query. After removal of cleanup-only setup, all 40
  query results matched the prior fixture, which had 8,388,608 rows and 4,096 physical files.

### Earlier observations to retain as investigation leads

The original document mixed proposed benchmarks with bug reports and measurements from earlier builds. Preserve
these as leads, not assertions about current HEAD. Reproduce each on the pinned build and add a focused sqltest or
issue before treating it as a current finding. They are not prerequisites for the roadmap.

| Earlier observation | Appropriate follow-up |
|---|---|
| Wide file-backed commits were much slower than narrow commits | Use the wide-commit case and a narrow diagnostic variant to profile per-column metadata work |
| Read-write ATTACH was dominated by a development-version migration probe | Profile current read-write/read-only attach; keep migration-specific timing separate |
| No-op maintenance issued metadata queries per table | Count relevant queries deterministically, then benchmark only if the scaling remains material |
| Inlined tables newer than a requested snapshot were still opened | Inspect snapshot-filtered inlined reads and add an assertion for unnecessary metadata/file access |
| NULL version/timestamp expressions produced internal or misleading errors | Minimal time-travel correctness tests; no performance benchmark needed |
| IMPORT DATABASE with unsuitable statements in `load.sql` raised an internal error | DuckDB importer correctness test; keep generated DDL/DML in the supported setup path |
| Repeated attach/detach cycles retained cache memory | Bounded lifecycle stress test plus process-memory measurement, not another latency gate |
| Cleanup through a read-only attach physically removed files | Reproduce in an isolated fixture; assert filesystem and metadata remain consistent after rejection |
| Orphan discovery spent substantial time canonicalizing paths | Profile listing versus path matching separately, preferably in the storage diagnostics track |
| Missing table stats triggered duplicate file-list reads | Extend metadata-query-count assertions rather than relying on one small query in a large timing batch |
| Re-running dbgen into existing tables appended data | Guard fixture construction/recovery; `tpch05` copies an empty database over its source before `dbgen` |
