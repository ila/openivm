#include "core/published_view.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>

namespace duckdb {

string PublishedViewName(const string &view_name) {
	return string(openivm::VISIBLE_TABLE_PREFIX) + view_name;
}

string PublishedSourceViewName(string source_name) {
	if (StringUtil::StartsWith(source_name, openivm::DELTA_PREFIX)) {
		source_name = source_name.substr(strlen(openivm::DELTA_PREFIX));
	}
	for (auto prefix : {openivm::DATA_TABLE_PREFIX, openivm::VISIBLE_TABLE_PREFIX}) {
		if (StringUtil::StartsWith(source_name, prefix)) {
			return source_name.substr(strlen(prefix));
		}
	}
	return source_name;
}

namespace {
class PublicationCopyTask : public BaseExecutorTask {
public:
	PublicationCopyTask(TaskExecutor &executor, ClientContext &context, FileSystem &fs, string source,
	                    string destination, vector<string> &owned_files, mutex &ownership_lock)
	    : BaseExecutorTask(executor), context(context), fs(fs), source(std::move(source)),
	      destination(std::move(destination)), owned_files(owned_files), ownership_lock(ownership_lock) {
	}

	void ExecuteTask() override {
		auto input = fs.OpenFile(source, FileFlags::FILE_FLAGS_READ);
		auto output = fs.OpenFile(destination, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                           FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
		{
			lock_guard<mutex> guard(ownership_lock);
			owned_files.push_back(destination);
		}
		vector<char> buffer(4 * 1024 * 1024);
		idx_t size = input->GetFileSize();
		for (idx_t offset = 0; offset < size;) {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			auto bytes = MinValue<idx_t>(buffer.size(), size - offset);
			input->Read(context, buffer.data(), bytes, offset);
			idx_t written = 0;
			while (written < bytes) {
				auto count = output->Write(context, buffer.data() + written, bytes - written);
				if (count <= 0) {
					throw IOException("Could not write copied publication file '%s'", destination);
				}
				written += count;
			}
			offset += bytes;
		}
		output->Sync();
	}

private:
	ClientContext &context;
	FileSystem &fs;
	string source;
	string destination;
	vector<string> &owned_files;
	mutex &ownership_lock;
};
} // namespace

bool TryCreatePublicationFromFiles(Connection &connection, const string &catalog, const string &schema,
                                   const string &published_name, const string &data_table,
                                   const string &published_query, vector<string> &uncommitted_files,
                                   const std::function<void(const string &, int64_t)> &record_step) {
	auto started = std::chrono::steady_clock::now();
	auto record = [&](const string &name) {
		auto ended = std::chrono::steady_clock::now();
		record_step(name, std::chrono::duration_cast<std::chrono::milliseconds>(ended - started).count());
		started = ended;
	};
	if (Catalog::GetEntry<TableCatalogEntry>(*connection.context, catalog, schema, published_name,
	                                         OnEntryNotFound::RETURN_NULL)) {
		return false;
	}
	// Imported files have independent ownership. Encrypted files need their original
	// keys and remote storage needs a different copy primitive, so retain CTAS there.
	auto options = connection.Query("SELECT count(*) FROM ducklake_options('" + SqlUtils::EscapeValue(catalog) +
	                                "') WHERE option_name='encrypted' AND lower(value)='true'");
	if (options->HasError()) {
		throw CatalogException(options->GetError());
	}
	record("publication_file_options");
	if (options->GetValue(0, 0).GetValue<int64_t>() != 0) {
		return false;
	}
	auto files = connection.Query("SELECT DISTINCT filename FROM " + data_table);
	if (files->HasError()) {
		throw CatalogException(files->GetError());
	}
	record("publication_file_discovery");
	if (files->RowCount() == 0) {
		return false;
	}
	auto &fs = FileSystem::GetFileSystem(*connection.context);
	vector<string> sources;
	for (idx_t i = 0; i < files->RowCount(); i++) {
		auto file = files->GetValue(0, i);
		if (file.IsNull() || FileSystem::IsRemoteFile(file.ToString()) || !fs.FileExists(file.ToString())) {
			return false;
		}
		sources.push_back(file.ToString());
	}
	auto described = connection.Query("DESCRIBE " + published_query);
	if (described->HasError()) {
		throw CatalogException(described->GetError());
	}
	vector<string> source_paths;
	for (const auto &source : sources) {
		source_paths.push_back("'" + SqlUtils::EscapeValue(source) + "'");
	}
	// Some SQL types use a different physical Parquet representation (HUGEINT,
	// for example). IMPORT requires compatible file types; CTAS applies the casts.
	auto parquet_schema = connection.Query("DESCRIBE SELECT * FROM read_parquet([" +
	                                       StringUtil::Join(source_paths, ", ") + "], union_by_name=true)");
	if (parquet_schema->HasError()) {
		throw CatalogException(parquet_schema->GetError());
	}
	case_insensitive_map_t<string> file_types;
	for (idx_t i = 0; i < parquet_schema->RowCount(); i++) {
		file_types.emplace(parquet_schema->GetValue(0, i).ToString(), parquet_schema->GetValue(1, i).ToString());
	}
	vector<string> columns;
	for (idx_t i = 0; i < described->RowCount(); i++) {
		auto name = described->GetValue(0, i).ToString();
		auto type = described->GetValue(1, i).ToString();
		if (name != openivm::PUBLISHED_ORDINAL_COL) {
			auto file_type = file_types.find(name);
			if (file_type == file_types.end() || file_type->second != type) {
				record("publication_file_schema");
				return false;
			}
		}
		columns.push_back(SqlUtils::QuoteIdentifier(name) + " " + type +
		                  (name == openivm::PUBLISHED_ORDINAL_COL ? " DEFAULT 0" : ""));
	}
	record("publication_file_schema");
	vector<string> imported_paths;
	vector<string> destinations;
	for (const auto &source : sources) {
		auto destination = source + ".openivm-publication-" + UUID::ToString(UUID::GenerateRandomUUID()) + ".parquet";
		destinations.push_back(destination);
		imported_paths.push_back("'" + SqlUtils::EscapeValue(destination) + "'");
	}
	TaskExecutor executor(*connection.context);
	mutex ownership_lock;
	vector<unique_ptr<Task>> tasks;
	for (idx_t i = 0; i < sources.size(); i++) {
		tasks.push_back(make_uniq<PublicationCopyTask>(executor, *connection.context, fs, sources[i], destinations[i],
		                                               uncommitted_files, ownership_lock));
	}
	for (auto &task : tasks) {
		executor.ScheduleTask(std::move(task));
	}
	executor.WorkOnTasks();
	record("publication_file_copy");
	auto published = SqlUtils::FullName(catalog, schema, published_name);
	auto created = connection.Query("CREATE TABLE " + published + " (" + StringUtil::Join(columns, ", ") + ")");
	if (created->HasError()) {
		throw CatalogException(created->GetError());
	}
	auto imported = connection.Query(
	    "CALL ducklake_add_data_files('" + SqlUtils::EscapeValue(catalog) + "', '" +
	    SqlUtils::EscapeValue(published_name) + "', [" + StringUtil::Join(imported_paths, ", ") + "], schema='" +
	    SqlUtils::EscapeValue(schema) + "', allow_missing=true, ignore_extra_columns=true)");
	if (imported->HasError()) {
		throw CatalogException(imported->GetError());
	}
	record("publication_file_import");
	return true;
}

string BuildPublishViewSQL(const string &view_name, const string &prefix, const string &query,
                           const vector<string> &columns, bool ducklake, const string &metadata_table,
                           const vector<string> &scope_columns, const string &timestamp_sql, SqlDialect dialect,
                           const string &appended_rows, const string &scope_rows,
                           const vector<string> &metadata_catalogs) {
	auto quote = [&](const string &name) {
		return DialectQuoteIdent(name, dialect);
	};
	auto visible = prefix + quote(PublishedViewName(view_name));
	auto delta_name = SqlUtils::DeltaName(PublishedViewName(view_name));
	auto delta = prefix + quote(delta_name);
	auto next = (dialect == SqlDialect::SPARK ? prefix : "") + quote("openivm_publish_next_" + view_name);
	auto changes = (dialect == SqlDialect::SPARK ? prefix : "") + quote("openivm_publish_changes_" + view_name);
	vector<string> quoted_columns;
	for (auto &column : columns) {
		quoted_columns.push_back(quote(column));
	}
	auto column_list = StringUtil::Join(quoted_columns, ", ");
	if (!appended_rows.empty()) {
		// The projection compiler supplies the exact rows appended to maintenance
		// state, including bag expansion and the refresh timestamp filter. Reuse
		// them instead of scanning and diffing the entire published relation.
		vector<string> projection;
		for (auto &column : columns) {
			projection.push_back(column == openivm::PUBLISHED_ORDINAL_COL ? "CAST(0 AS BIGINT) AS " + quote(column)
			                                                              : quote(column));
		}
		auto rows = "SELECT " + StringUtil::Join(projection, ", ") + " FROM (" + appended_rows + ") appended_rows";
		string sql = "INSERT INTO " + visible + " " + rows + ";\n";
		if (!ducklake) {
			auto timestamp = timestamp_sql.empty() ? openivm::UTC_NOW_SQL : timestamp_sql;
			sql += "INSERT INTO " + delta + " (" + column_list +
			       ", openivm_multiplicity, openivm_timestamp) SELECT *, 1::INTEGER, " + timestamp + " FROM (" + rows +
			       ") published_rows WHERE EXISTS (SELECT 1 FROM " + metadata_table + " WHERE table_name = '" +
			       SqlUtils::EscapeValue(delta_name) + "');\n";
			sql += RefreshMetadata::BuildDeltaCleanupSQL(delta, delta_name, metadata_table, nullptr, metadata_catalogs);
		}
		OPENIVM_DEBUG_PRINT("[PUBLISH] Appending visible projection delta for %s\n", view_name.c_str());
		return sql;
	}
	string equality;
	for (auto &column : columns) {
		if (!equality.empty()) {
			equality += " AND ";
		}
		auto quoted = quote(column);
		equality += "v." + quoted + " IS NOT DISTINCT FROM d." + quoted;
	}
	string scope;
	for (auto &column : scope_columns) {
		if (!scope.empty()) {
			scope += " AND ";
		}
		auto quoted = quote(column);
		scope += "v." + quoted + " IS NOT DISTINCT FROM d." + quoted;
	}
	string scoped_query = query;
	string old_query = "SELECT * FROM " + visible;
	if (!scope.empty()) {
		OPENIVM_DEBUG_PRINT("[PUBLISH] Scoping %s by %zu affected columns\n", view_name.c_str(), scope_columns.size());
		auto raw_delta = scope_rows.empty() ? prefix + quote(SqlUtils::DeltaName(view_name)) : scope_rows;
		// The native executor disables deliminator for deeply nested maintenance
		// SQL. Express the semijoin directly to avoid grouping all target rows
		// merely to decorrelate an EXISTS predicate.
		auto predicate = dialect == SqlDialect::DUCKDB
		                     ? " SEMI JOIN " + raw_delta + " d ON " + scope
		                     : " WHERE EXISTS (SELECT 1 FROM " + raw_delta + " d WHERE " + scope + ")";
		scoped_query = "SELECT v.* FROM (" + query + ") v" + predicate;
		old_query = "SELECT v.* FROM " + visible + " v" + predicate;
	}
	// Diff the visible output, not the base query. In particular LIMIT refill and
	// HAVING threshold crossings must retract the previously published rows.
	string create = dialect == SqlDialect::SPARK ? "CREATE TABLE " : "CREATE TEMP TABLE ";
	string sql = create + next + " AS " + scoped_query + ";\n";
	auto weight = quote("openivm_publish_weight");
	sql += create + changes + " AS SELECT " + column_list + ", SUM(" + weight + ") AS " + weight +
	       " FROM (SELECT *, CAST(-1 AS BIGINT) AS " + weight + " FROM (" + old_query +
	       ") old_rows UNION ALL SELECT *, CAST(1 AS BIGINT) FROM " + next + ") signed_rows GROUP BY " + column_list +
	       " HAVING SUM(" + weight + ") <> 0;\n";
	if (!ducklake) {
		string timestamp = timestamp_sql.empty() ? openivm::UTC_NOW_SQL : timestamp_sql;
		vector<string> delta_columns;
		for (auto &column : quoted_columns) {
			delta_columns.push_back("c." + column);
		}
		auto expansion = dialect == SqlDialect::SPARK
		                     ? " LATERAL VIEW explode(sequence(CAST(1 AS BIGINT), CAST(abs(c." + weight +
		                           ") AS BIGINT))) repetitions AS repetition"
		                     : " CROSS JOIN UNNEST(range(CAST(abs(c." + weight + ") AS BIGINT))) repetitions";
		sql += "INSERT INTO " + delta + " (" + column_list + ", openivm_multiplicity, openivm_timestamp) SELECT " +
		       StringUtil::Join(delta_columns, ", ") + ", CAST(sign(c." + weight + ") AS INTEGER), " + timestamp +
		       " FROM " + changes + " c" + expansion + " WHERE EXISTS (SELECT 1 FROM " + metadata_table +
		       " WHERE table_name = '" + SqlUtils::EscapeValue(delta_name) + "');\n";
	}
	// Replace only changed bags. DuckLake keeps its table identity and records the
	// modifications in snapshots; native consumers read the explicit signed delta.
	if (dialect == SqlDialect::DUCKDB) {
		sql += "DELETE FROM " + visible + " AS v USING " + changes + " d WHERE " + equality + ";\n";
		sql += "INSERT INTO " + visible + " SELECT v.* FROM " + next + " v SEMI JOIN " + changes + " d ON " + equality +
		       ";\n";
	} else {
		sql +=
		    "DELETE FROM " + visible + " AS v WHERE EXISTS (SELECT 1 FROM " + changes + " d WHERE " + equality + ");\n";
		sql += "INSERT INTO " + visible + " SELECT v.* FROM " + next + " v WHERE EXISTS (SELECT 1 FROM " + changes +
		       " d WHERE " + equality + ");\n";
	}
	sql += "DROP TABLE " + changes + ";\nDROP TABLE " + next + ";\n";
	if (!ducklake) {
		sql += RefreshMetadata::BuildDeltaCleanupSQL(delta, delta_name, metadata_table, nullptr, metadata_catalogs);
	}
	OPENIVM_DEBUG_PRINT("[PUBLISH] Compiled visible-row publication for %s\n", view_name.c_str());
	return sql;
}

} // namespace duckdb
