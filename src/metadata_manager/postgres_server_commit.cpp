#include "metadata_manager/postgres_server_commit.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_server_commit.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

#include <cmath>

namespace duckdb {

static constexpr const char *FALLBACK_SIGNATURE = "ducklake_commit_fallback_v1(text)";
static constexpr const char *COMMIT_SIGNATURE =
    "ducklake_commit_v1(bigint,text,bigint,bigint[],bigint[],text[],bigint[],text,bigint,bigint,double precision)";

static const char *FallbackFunctionSql() {
	return R"sql(CREATE FUNCTION {METADATA_CATALOG}.ducklake_commit_fallback_v1(p_reason TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog, pg_temp AS $f$
BEGIN
	RAISE EXCEPTION USING ERRCODE = 'DLFB1', MESSAGE = p_reason;
END $f$;)sql";
}

static const char *CommitFunctionSql() {
	return R"sql(CREATE FUNCTION {METADATA_CATALOG}.ducklake_commit_v1(
	p_transaction_snapshot_id BIGINT, p_catalog_version TEXT, p_file_id_count BIGINT,
	p_insert_tables BIGINT[], p_inlined_insert_tables BIGINT[], p_known_change_kinds TEXT[],
	p_row_id_tables BIGINT[], p_body TEXT,
	p_max_retry_count BIGINT, p_retry_wait_ms BIGINT, p_retry_backoff DOUBLE PRECISION)
RETURNS TABLE(status TEXT, committed_snapshot_id BIGINT, committed_schema_version BIGINT,
              committed_next_file_id BIGINT, last_attempt BIGINT, detail TEXT)
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
SET standard_conforming_strings = on
AS $dlc$
DECLARE
	v_schema_oid INTEGER := (SELECT n.oid FROM pg_namespace n WHERE n.nspname = {METADATA_SCHEMA_NAME_LITERAL})::INTEGER;
	v_saved_lock_ms DOUBLE PRECISION := extract(epoch FROM current_setting('lock_timeout')::INTERVAL) * 1000;
	v_transaction_timeout TEXT := current_setting('transaction_timeout', true);
	c_max_ms CONSTANT DOUBLE PRECISION := 3600000;
	v_max BIGINT := LEAST(GREATEST(p_max_retry_count, 0), 10000);
	v_wait DOUBLE PRECISION := LEAST(GREATEST(p_retry_wait_ms, 0), c_max_ms);
	v_backoff DOUBLE PRECISION := CASE WHEN p_retry_backoff > 1 AND p_retry_backoff <> 'NaN' THEN p_retry_backoff
	                                   ELSE 1 END;
	v_attempt BIGINT := 1;
	v_locked BOOLEAN := true;
	v_budget_ms DOUBLE PRECISION := 0;
	v_step_ms DOUBLE PRECISION;
	v_lock_ms DOUBLE PRECISION;
	v_deadline TIMESTAMPTZ;
	v_stop TIMESTAMPTZ;
	v_latest RECORD;
	v_snapshot_id BIGINT;
	v_table BIGINT;
	v_bad TEXT;
	v_conflict RECORD;
	v_state TEXT;
	v_table_name TEXT;
	v_message TEXT;
	c_token CONSTANT TEXT := '[^:,"]+:("([^"]|"")*"(\."([^"]|"")*")*|[0-9]+)';
	c_kind_value CONSTANT TEXT := '([^:,"]+):("([^"]|"")*"(\."([^"]|"")*")*|[0-9]+)';
BEGIN
	IF current_setting('transaction_isolation') <> 'read committed' THEN
		RETURN QUERY SELECT 'disabled'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, 0::BIGINT,
		                    current_setting('transaction_isolation');
		RETURN;
	END IF;
	-- saturating lock budget = the client backoff sleeps attempts 1..max would take
	v_step_ms := v_wait;
	FOR i IN 1 .. v_max LOOP
		-- saturates before multiplying
		v_step_ms := CASE WHEN v_step_ms <= 0 THEN 0 WHEN v_step_ms >= c_max_ms / v_backoff THEN c_max_ms
		                  ELSE v_step_ms * v_backoff END;
		v_budget_ms := LEAST(v_budget_ms + v_step_ms, c_max_ms);
		EXIT WHEN v_budget_ms >= c_max_ms;
	END LOOP;
	-- the budget bounds waits, not attempts
	v_deadline := clock_timestamp() + make_interval(secs => GREATEST(v_budget_ms, 1) / 1000.0);
	IF current_setting('statement_timeout') <> '0' THEN
		v_stop := statement_timestamp() + current_setting('statement_timeout')::INTERVAL * 0.8;
	END IF;
	IF v_transaction_timeout IS NOT NULL AND v_transaction_timeout <> '0' THEN
		v_stop := LEAST(v_stop, transaction_timestamp() + v_transaction_timeout::INTERVAL * 0.8);
	END IF;
	v_deadline := LEAST(v_deadline, v_stop);
	v_step_ms := v_wait;
	LOOP
		BEGIN
			IF v_locked THEN
				v_lock_ms := LEAST(GREATEST(1, ceil(extract(epoch FROM v_deadline - clock_timestamp()) * 1000)),
				                   NULLIF(v_saved_lock_ms, 0));
				BEGIN
					PERFORM set_config('lock_timeout', v_lock_ms::BIGINT::TEXT, true);
					PERFORM pg_advisory_xact_lock(1145848659, v_schema_oid);
				EXCEPTION WHEN lock_not_available THEN
					v_locked := false;
				END;
			END IF;
			-- each lock wait gets the time left to v_stop
			PERFORM set_config('lock_timeout', CASE WHEN v_stop IS NULL THEN v_saved_lock_ms
			        ELSE LEAST(GREATEST(1, ceil(extract(epoch FROM v_stop - clock_timestamp()) * 1000)),
			                   NULLIF(v_saved_lock_ms, 0)) END::BIGINT::TEXT, true);
			SELECT s.snapshot_id, s.snapshot_time, s.schema_version, s.next_catalog_id, s.next_file_id INTO v_latest
			FROM {METADATA_CATALOG}.ducklake_snapshot s ORDER BY s.snapshot_id DESC LIMIT 1;
			v_snapshot_id := v_latest.snapshot_id + 1;
			-- the claim is the first write
			INSERT INTO {METADATA_CATALOG}.ducklake_snapshot
			    (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
			VALUES (v_snapshot_id, GREATEST(clock_timestamp(), v_latest.snapshot_time), v_latest.schema_version,
			        v_latest.next_catalog_id, v_latest.next_file_id + p_file_id_count);
			PERFORM set_config('ducklake_commit.snapshot_id', v_snapshot_id::TEXT, true),
			        set_config('ducklake_commit.schema_version', v_latest.schema_version::TEXT, true),
			        set_config('ducklake_commit.next_catalog_id', v_latest.next_catalog_id::TEXT, true),
			        set_config('ducklake_commit.file_id_base', v_latest.next_file_id::TEXT, true);
			IF (SELECT max(m.value) FROM {METADATA_CATALOG}.ducklake_metadata m WHERE m.key = 'version')
			   IS DISTINCT FROM p_catalog_version THEN
				PERFORM {METADATA_CATALOG}.ducklake_commit_fallback_v1('catalog version changed');
			END IF;
			IF v_latest.snapshot_id > p_transaction_snapshot_id THEN
				-- fail closed: every concurrent change list must be fully well-formed with known kinds
				SELECT c.snapshot_id::TEXT INTO v_bad
				FROM {METADATA_CATALOG}.ducklake_snapshot_changes c
				WHERE c.snapshot_id > p_transaction_snapshot_id AND c.snapshot_id < v_snapshot_id
				  AND COALESCE(c.changes_made, '') <> ''
				  AND (c.changes_made !~ ('^' || c_token || '(,' || c_token || ')*$')
				       OR EXISTS (SELECT 1 FROM regexp_matches(c.changes_made, c_kind_value, 'g') k
				                  WHERE NOT (k[1] = ANY (p_known_change_kinds))))
				LIMIT 1;
				IF v_bad IS NOT NULL THEN
					PERFORM {METADATA_CATALOG}.ducklake_commit_fallback_v1('unrecognized changes in snapshot ' || v_bad);
				END IF;
				SELECT t.table_id, m.kind INTO v_conflict
				FROM (SELECT 0 AS set_rank, x AS table_id FROM unnest(p_insert_tables) AS x
				      UNION ALL SELECT 1, x FROM unnest(p_inlined_insert_tables) AS x) AS t
				-- whole tokens, so quoted names never match
				JOIN (SELECT k[1] AS kind, CASE WHEN k[2] ~ '^[0-9]+$' THEN k[2]::BIGINT END AS table_id
				      FROM {METADATA_CATALOG}.ducklake_snapshot_changes c,
				           LATERAL regexp_matches(c.changes_made, c_kind_value, 'g') AS k
				      WHERE c.snapshot_id > p_transaction_snapshot_id AND c.snapshot_id < v_snapshot_id
				        AND k[1] IN ('dropped_table', 'altered_table', 'deleted_from_table', 'inlined_delete')) AS m
				  ON m.table_id = t.table_id
				ORDER BY t.set_rank, t.table_id,
				         array_position(ARRAY['dropped_table', 'altered_table', 'deleted_from_table', 'inlined_delete'], m.kind)
				LIMIT 1;
				IF FOUND THEN
					RAISE EXCEPTION USING ERRCODE = 'DLCF1', MESSAGE = v_conflict.table_id || ':' || v_conflict.kind;
				END IF;
			END IF;
			FOREACH v_table IN ARRAY p_row_id_tables LOOP
				PERFORM set_config('ducklake_commit.row_id_base_' || v_table,
				                   COALESCE((SELECT ts.next_row_id FROM {METADATA_CATALOG}.ducklake_table_stats ts
				                             WHERE ts.table_id = v_table), 0)::TEXT, true);
			END LOOP;
			-- refreshed for the body's lock waits
			PERFORM set_config('lock_timeout', CASE WHEN v_stop IS NULL THEN v_saved_lock_ms
			        ELSE LEAST(GREATEST(1, ceil(extract(epoch FROM v_stop - clock_timestamp()) * 1000)),
			                   NULLIF(v_saved_lock_ms, 0)) END::BIGINT::TEXT, true);
			EXECUTE p_body;
			PERFORM set_config('lock_timeout', (v_saved_lock_ms::BIGINT)::TEXT, true);
			RETURN QUERY SELECT 'committed'::TEXT, v_snapshot_id, v_latest.schema_version,
			                    v_latest.next_file_id + p_file_id_count, v_attempt, NULL::TEXT;
			RETURN;
		EXCEPTION
			WHEN SQLSTATE 'DLCF1' THEN
				GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;
				RETURN QUERY SELECT 'conflict'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, v_attempt, v_message;
				RETURN;
			WHEN SQLSTATE 'DLFB1' THEN
				GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;
				RETURN QUERY SELECT 'fallback'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, v_attempt, v_message;
				RETURN;
			WHEN unique_violation OR serialization_failure OR deadlock_detected THEN
				GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_table_name = TABLE_NAME, v_message = MESSAGE_TEXT;
				IF v_state = '23505' AND v_table_name IS DISTINCT FROM 'ducklake_snapshot' THEN
					RETURN QUERY SELECT 'error'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, v_attempt,
					                    v_state || ': ' || v_message;
					RETURN;
				END IF;
				IF v_attempt >= v_max OR (v_stop IS NOT NULL AND clock_timestamp() >= v_stop) THEN
					RETURN QUERY SELECT 'exhausted'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, v_attempt, v_message;
					RETURN;
				END IF;
			WHEN OTHERS THEN
				GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_message = MESSAGE_TEXT;
				RETURN QUERY SELECT 'error'::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, v_attempt,
				                    v_state || ': ' || v_message;
				RETURN;
		END;
		-- the attempt's subtransaction aborted, so the advisory lock is already released
		v_step_ms := CASE WHEN v_step_ms <= 0 THEN 0 WHEN v_step_ms >= c_max_ms / v_backoff THEN c_max_ms
		                  ELSE v_step_ms * v_backoff END;
		PERFORM pg_sleep(LEAST(v_step_ms * (0.5 + random() / 2),
		                       GREATEST(extract(epoch FROM v_deadline - clock_timestamp()) * 1000, 0)) / 1000.0);
		v_attempt := v_attempt + 1;
	END LOOP;
END
$dlc$;)sql";
}

static string RegProcedure(const string &schema_literal, const char *signature) {
	return StringUtil::Format("pg_catalog.to_regprocedure(pg_catalog.quote_ident(%s) || '.%s')", schema_literal,
	                          signature);
}

string PostgresServerCommit::ProbeSql(const string &schema_literal) {
	return StringUtil::Format(
	    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_language l WHERE l.lanname = 'plpgsql' "
	    "AND pg_catalog.has_language_privilege(l.oid, 'USAGE')) AS has_plpgsql, "
	    "p.fallback_oid IS NOT NULL AND p.commit_oid IS NOT NULL AS installed, "
	    "COALESCE(pg_catalog.has_function_privilege(p.fallback_oid, 'EXECUTE'), false) AND "
	    "COALESCE(pg_catalog.has_function_privilege(p.commit_oid, 'EXECUTE'), false) AS executable, "
	    "COALESCE(pg_catalog.has_schema_privilege(p.schema_oid, 'CREATE'), false) AS can_create, "
	    "pg_catalog.current_setting('default_transaction_isolation') AS default_isolation "
	    "FROM (SELECT (SELECT n.oid FROM pg_catalog.pg_namespace n WHERE n.nspname = %s) AS schema_oid, "
	    "%s::oid AS fallback_oid, %s::oid AS commit_oid) p",
	    schema_literal, RegProcedure(schema_literal, FALLBACK_SIGNATURE),
	    RegProcedure(schema_literal, COMMIT_SIGNATURE));
}

//! A dollar-quote tag absent from the text
static string DollarQuoteTag(const string &base, const string &text) {
	for (idx_t suffix = 0;; suffix++) {
		auto tag = "$" + base + (suffix == 0 ? string() : std::to_string(suffix)) + "$";
		// the closing tag must not overlap the text
		auto tail = text.substr(text.size() - MinValue<idx_t>(text.size(), tag.size() - 1)) + tag;
		if (!StringUtil::Contains(text, tag) && tail.find(tag) == tail.size() - tag.size()) {
			return tag;
		}
	}
}

//! Substitutes the template, retagging its body if needed
static string TagFunctionBody(const string &sql_template, const string &tag,
                              const std::function<string(const string &)> &substitute) {
	auto new_tag =
	    DollarQuoteTag(tag.substr(1, tag.size() - 2), substitute(StringUtil::Replace(sql_template, tag, "")));
	auto result = substitute(StringUtil::Replace(sql_template, tag, new_tag));
	idx_t occurrences = 0;
	for (auto pos = result.find(new_tag); pos != string::npos; pos = result.find(new_tag, pos + new_tag.size())) {
		occurrences++;
	}
	if (occurrences != 2) {
		throw InternalException("Server-side commit function body has %d \"%s\" delimiters", occurrences, new_tag);
	}
	return result;
}

//! Takes the lake's advisory key with lock_function
static string AdvisoryKeySql(const char *lock_function, int32_t key, const string &schema_literal) {
	return StringUtil::Format("PERFORM pg_catalog.%s(%d, (SELECT n.oid FROM pg_catalog.pg_namespace n "
	                          "WHERE n.nspname = %s)::INTEGER);\n",
	                          lock_function, key, schema_literal);
}

//! DO block body with a bounded lock wait
static string BoundedLockBody(const string &lock_sql, int64_t lock_ms, int64_t statement_timeout_permille,
                              const string &handled_errors, const string &handler_sql) {
	return StringUtil::Format(
	    "\nDECLARE\n"
	    "\tv_saved_lock_timeout TEXT := pg_catalog.current_setting('lock_timeout');\n"
	    "\tv_transaction_timeout TEXT := pg_catalog.current_setting('transaction_timeout', true);\n"
	    "\tv_lock_ms DOUBLE PRECISION := %d;\n"
	    "BEGIN\n"
	    "\tIF pg_catalog.current_setting('statement_timeout') <> '0' THEN\n"
	    "\t\tv_lock_ms := LEAST(v_lock_ms, extract(epoch FROM "
	    "pg_catalog.current_setting('statement_timeout')::INTERVAL) * %d);\n"
	    "\tEND IF;\n"
	    "\tIF v_transaction_timeout IS NOT NULL AND v_transaction_timeout <> '0' THEN\n"
	    "\t\tv_lock_ms := LEAST(v_lock_ms, extract(epoch FROM v_transaction_timeout::INTERVAL - "
	    "(pg_catalog.clock_timestamp() - pg_catalog.transaction_timestamp())) * 500);\n"
	    "\tEND IF;\n"
	    "\tIF v_saved_lock_timeout <> '0' THEN\n"
	    "\t\tv_lock_ms := LEAST(v_lock_ms, extract(epoch FROM v_saved_lock_timeout::INTERVAL) * 1000);\n"
	    "\tEND IF;\n"
	    "\tv_lock_ms := GREATEST(pg_catalog.ceil(v_lock_ms), 1);\n"
	    "\tBEGIN\n"
	    "\t\tPERFORM pg_catalog.set_config('lock_timeout', v_lock_ms::BIGINT::TEXT, true);\n"
	    "%s"
	    "\tEXCEPTION WHEN %s THEN\n"
	    "%s"
	    "\tEND;\n"
	    "\tPERFORM pg_catalog.set_config('lock_timeout', v_saved_lock_timeout, true);\n"
	    "END\n",
	    lock_ms, statement_timeout_permille, lock_sql, handled_errors, handler_sql);
}

string PostgresServerCommit::InstallSql(const string &schema_literal,
                                        const std::function<string(const string &)> &substitute) {
	auto fallback_sql = TagFunctionBody(FallbackFunctionSql(), "$f$", substitute);
	auto commit_sql = TagFunctionBody(CommitFunctionSql(), "$dlc$", substitute);
	D_ASSERT(StringUtil::Contains(commit_sql, std::to_string(COMMIT_LOCK_KEY)));
	auto statement_tag = DollarQuoteTag("dlb", fallback_sql + "\n" + commit_sql);
	auto create_sql =
	    StringUtil::Format("\t\tIF %s IS NULL THEN\n\t\t\tEXECUTE %s%s%s;\n\t\tEND IF;\n"
	                       "\t\tIF %s IS NULL THEN\n\t\t\tEXECUTE %s%s%s;\n\t\tEND IF;\n",
	                       RegProcedure(schema_literal, FALLBACK_SIGNATURE), statement_tag, fallback_sql, statement_tag,
	                       RegProcedure(schema_literal, COMMIT_SIGNATURE), statement_tag, commit_sql, statement_tag);
	// WHEN OTHERS cannot catch statement_timeout cancels
	auto body = BoundedLockBody(
	    "\t\t" + AdvisoryKeySql("pg_advisory_xact_lock", INSTALL_LOCK_KEY, schema_literal) + create_sql, 5000, 500,
	    "OTHERS", "\t\tRAISE WARNING 'DuckLake could not install its server-side commit functions: %', SQLERRM;\n");
	auto block_tag = DollarQuoteTag("dli", body);
	return "DO " + block_tag + body + block_tag + ";";
}

static constexpr double MAX_LOCK_BUDGET_MS = 3600000;
static constexpr idx_t MAX_BUDGET_ATTEMPTS = 10000;

//! step_ms * factor, saturating before multiplying like ducklake_commit_v1
static double SaturatedStepMs(double step_ms, double factor) {
	if (step_ms <= 0) {
		return 0;
	}
	return step_ms >= MAX_LOCK_BUDGET_MS / factor ? MAX_LOCK_BUDGET_MS : step_ms * factor;
}

int64_t PostgresServerCommit::SequencedLockBudgetMs(const DuckLakeRetryConfig &retry_config) {
	// saturating, the same budget as ducklake_commit_v1
	auto backoff = retry_config.retry_backoff > 1 ? retry_config.retry_backoff : 1.0;
	auto max_attempt = MinValue<idx_t>(retry_config.max_retry_count, MAX_BUDGET_ATTEMPTS);
	auto step_ms = MinValue<double>(static_cast<double>(retry_config.retry_wait_ms), MAX_LOCK_BUDGET_MS);
	double budget_ms = 0;
	for (idx_t i = 1; i <= max_attempt && budget_ms < MAX_LOCK_BUDGET_MS; i++) {
		step_ms = SaturatedStepMs(step_ms, backoff);
		budget_ms = MinValue<double>(budget_ms + step_ms, MAX_LOCK_BUDGET_MS);
	}
	return static_cast<int64_t>(budget_ms);
}

double PostgresServerCommit::RetrySleepMs(const DuckLakeRetryConfig &retry_config, idx_t attempt) {
	auto backoff = retry_config.retry_backoff >= 0 ? retry_config.retry_backoff : 1.0;
	auto wait_ms = MinValue<double>(static_cast<double>(retry_config.retry_wait_ms), MAX_LOCK_BUDGET_MS);
	return SaturatedStepMs(wait_ms, std::pow(backoff, static_cast<double>(attempt)));
}

string PostgresServerCommit::SequencerSql(const string &schema_literal, int64_t lock_budget_ms) {
	// both waits share the lock budget
	auto body = BoundedLockBody(
	    "\t\t" + AdvisoryKeySql("pg_advisory_xact_lock", COMMIT_LOCK_KEY, schema_literal) +
	        "\t\tPERFORM pg_catalog.set_config('lock_timeout', GREATEST(pg_catalog.ceil(v_lock_ms - extract(epoch "
	        "FROM pg_catalog.clock_timestamp() - pg_catalog.statement_timestamp()) * 1000), 1)::BIGINT::TEXT, "
	        "true);\n\t\t" +
	        AdvisoryKeySql("pg_advisory_xact_lock", BATCH_LOCK_KEY, schema_literal) +
	        "\t\tPERFORM pg_catalog.set_config('ducklake_commit.sequenced', 'true', true);\n",
	    lock_budget_ms, 800, "lock_not_available",
	    "\t\tPERFORM pg_catalog.set_config('ducklake_commit.sequenced', 'false', true),\n"
	    "\t\t        pg_catalog.set_config('ducklake_commit.lock_wait_ms', v_lock_ms::BIGINT::TEXT, true);\n");
	auto block_tag = DollarQuoteTag("dlseq", body);
	return "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\nDO " + block_tag + body + block_tag + ";";
}

string PostgresServerCommit::SharedKeySql(int64_t lock_budget_ms,
                                          const std::function<string(const string &)> &substitute) {
	// waiting with written rows could deadlock
	auto body = BoundedLockBody(
	    "\t\tIF pg_catalog.txid_current_if_assigned() IS NULL THEN\n\t\t\t" +
	        AdvisoryKeySql("pg_advisory_xact_lock_shared", BATCH_LOCK_KEY, "{METADATA_SCHEMA_NAME_LITERAL}") +
	        "\t\tEND IF;\n",
	    lock_budget_ms, 800, "OTHERS", "\t\tNULL;\n");
	auto block_tag = DollarQuoteTag("dlshr", substitute(body));
	return "DO " + block_tag + body + block_tag + ";\n";
}

vector<string> PostgresServerCommit::SequencedStateColumns() {
	// the latest inlined table rule of LatestInlinedTableQuery
	return {DuckLakeMetadataManager::CatalogVersionQuery() + " AS catalog_version",
	        "(SELECT STRING_AGG('\"' || i.table_id || '\",\"' || replace(i.table_name, '\"', '\"\"') || '\"', ',' "
	        "ORDER BY i.table_id) FROM (SELECT DISTINCT ON (t.table_id) t.table_id, t.table_name "
	        "FROM {METADATA_CATALOG}.ducklake_inlined_data_tables t ORDER BY t.table_id, t.schema_version DESC) i) "
	        "AS inlined_tables",
	        "pg_catalog.current_setting('ducklake_commit.sequenced', true) AS sequenced",
	        "pg_catalog.current_setting('ducklake_commit.lock_wait_ms', true) AS lock_wait_ms"};
}

void PostgresServerCommit::SetSequencedSnapshotTime(string &batch) {
	auto insert_snapshot = DuckLakeMetadataManager::InsertSnapshotSql();
	if (!StringUtil::StartsWith(batch, insert_snapshot)) {
		return;
	}
	auto sequenced_insert = StringUtil::Replace(insert_snapshot, "NOW()",
	                                            "GREATEST(pg_catalog.statement_timestamp(), (SELECT s.snapshot_time "
	                                            "FROM {METADATA_CATALOG}.ducklake_snapshot s "
	                                            "ORDER BY s.snapshot_id DESC LIMIT 1))");
	batch = sequenced_insert + batch.substr(insert_snapshot.size());
}

static string BigintArray(const vector<TableIndex> &tables) {
	vector<string> ids;
	for (auto &table : tables) {
		ids.push_back(std::to_string(table.index));
	}
	return "ARRAY[" + StringUtil::Join(ids, ", ") + "]::BIGINT[]";
}

string PostgresServerCommit::CommitCallSql(const string &metadata_catalog, idx_t transaction_snapshot_id,
                                           const string &catalog_version, const DuckLakeRelativeCommit &relative,
                                           const string &body, bool redact_body,
                                           const DuckLakeRetryConfig &retry_config) {
	vector<string> kinds;
	for (auto &kind : SnapshotChangeInformation::KnownChangeKinds()) {
		kinds.push_back(SQLString::ToString(kind));
	}
	auto max_bigint = static_cast<idx_t>(NumericLimits<int64_t>::Maximum());
	string body_sql;
	if (redact_body) {
		body_sql = StringUtil::Format("<%d bytes>", body.size());
	} else {
		auto body_tag = DollarQuoteTag("dlbody", body);
		body_sql = body_tag + body + body_tag;
	}
	return StringUtil::Format(
	    "SELECT * FROM %s.ducklake_commit_v1(p_transaction_snapshot_id => %d, p_catalog_version => %s, "
	    "p_file_id_count => %d, p_insert_tables => %s, p_inlined_insert_tables => %s, p_known_change_kinds => "
	    "ARRAY[%s]::TEXT[], p_row_id_tables => %s, p_body => %s, p_max_retry_count => %d, p_retry_wait_ms => %d, "
	    "p_retry_backoff => %s::DOUBLE PRECISION)",
	    metadata_catalog, transaction_snapshot_id, SQLString::ToString(catalog_version), relative.file_id_count,
	    BigintArray(relative.insert_tables), BigintArray(relative.inlined_insert_tables), StringUtil::Join(kinds, ", "),
	    BigintArray(relative.row_id_tables), body_sql, MinValue(retry_config.max_retry_count, max_bigint),
	    MinValue(retry_config.retry_wait_ms, max_bigint),
	    SQLString::ToString(Value::DOUBLE(retry_config.retry_backoff).ToString()));
}

static constexpr idx_t MAX_ERROR_BYTES = 1000;

string PostgresServerCommit::RedactCommitCallError(const string &message, const string &call_sql) {
	auto result = call_sql.empty() ? message : StringUtil::Replace(message, call_sql, "<ducklake_commit_v1 call>");
	if (result.size() <= MAX_ERROR_BYTES) {
		return result;
	}
	idx_t end = MAX_ERROR_BYTES;
	// cut at a UTF-8 character boundary
	while (end > 0 && (static_cast<uint8_t>(result[end]) & 0xC0) == 0x80) {
		end--;
	}
	return result.substr(0, end) + "... (truncated)";
}

string PostgresServerCommit::RelativeUsableSql(const string &schema_literal) {
	return StringUtil::Format(
	    "SELECT CASE WHEN p.fallback_oid IS NULL THEN false ELSE "
	    "COALESCE(pg_catalog.has_function_privilege(p.fallback_oid, 'EXECUTE'), false) AND "
	    "pg_catalog.has_language_privilege('plpgsql', 'USAGE') END FROM (SELECT %s::oid AS fallback_oid) p",
	    RegProcedure(schema_literal, FALLBACK_SIGNATURE));
}

string PostgresServerCommit::DebugFallbackSetting() {
	return DuckLakeRelativeCommitBase::SettingName("debug_fallback");
}

//! SQL-time fallbacks the debug template may take
static constexpr const char *EXPECTED_FALLBACKS =
    "^(missing size of table [0-9]+|incomparable stored (min|max) bound of table [0-9]+)$";

string PostgresServerCommit::RelativeExecutionSql(const string &claim, const vector<pair<string, string>> &bases,
                                                  const string &relative_body, const string &absolute_body) {
	string set_bases;
	for (auto &base : bases) {
		set_bases += set_bases.empty() ? "SELECT " : ",\n       ";
		set_bases +=
		    StringUtil::Format("pg_catalog.set_config(%s, %s, true)",
		                       SQLString(DuckLakeRelativeCommitBase::SettingName(base.first)), SQLString(base.second));
	}
	// the template runs like inside ducklake_commit_v1
	auto relative_tag = DollarQuoteTag("dlrt", relative_body);
	auto absolute_tag = DollarQuoteTag("dlat", absolute_body);
	auto block = StringUtil::Format(
	    "\nDECLARE\n"
	    "\tv_search_path TEXT := pg_catalog.current_setting('search_path');\n"
	    "\tv_standard_strings TEXT := pg_catalog.current_setting('standard_conforming_strings');\n"
	    "BEGIN\n"
	    "\tBEGIN\n"
	    "\t\tPERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true),\n"
	    "\t\t        pg_catalog.set_config('standard_conforming_strings', 'on', true);\n"
	    "\t\tEXECUTE %s%s%s;\n"
	    "\t\tPERFORM pg_catalog.set_config('search_path', v_search_path, true),\n"
	    "\t\t        pg_catalog.set_config('standard_conforming_strings', v_standard_strings, true);\n"
	    "\tEXCEPTION WHEN SQLSTATE 'DLFB1' THEN\n"
	    "\t\tIF SQLERRM !~ %s THEN\n"
	    "\t\t\tRAISE EXCEPTION USING MESSAGE = 'DuckLake relative commit fell back unexpectedly: ' || SQLERRM;\n"
	    "\t\tEND IF;\n"
	    "\t\tPERFORM pg_catalog.set_config(%s, SQLERRM, true);\n"
	    "\t\tEXECUTE %s%s%s;\n"
	    "\tEND;\n"
	    "END\n",
	    relative_tag, relative_body, relative_tag, SQLString(EXPECTED_FALLBACKS), SQLString(DebugFallbackSetting()),
	    absolute_tag, absolute_body, absolute_tag);
	auto block_tag = DollarQuoteTag("dlrel", block);
	return claim + "\n" + set_bases + ";\nDO " + block_tag + block + block_tag + ";";
}

} // namespace duckdb
