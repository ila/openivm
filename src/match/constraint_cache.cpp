#include "match/constraint_cache.hpp"

#include "core/openivm_constants.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/refresh_locks.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace openivm {

static optional_ptr<TableCatalogEntry> ResolveSource(ClientContext &context, const string &name) {
	auto parts = SqlUtils::ParseQualifiedIdentifier(name);
	if (parts.size() > 3) {
		throw ParserException("Invalid trusted-FK table name '%s'", name);
	}
	Identifier catalog(parts.size() == 3 ? parts[0] : INVALID_CATALOG);
	Identifier schema(parts.size() >= 2 ? parts[parts.size() - 2] : INVALID_SCHEMA);
	Binder::BindSchemaOrCatalog(context, catalog, schema);
	return Catalog::GetEntry<TableCatalogEntry>(context, Identifier(catalog), Identifier(schema),
	                                            Identifier(parts.back()), OnEntryNotFound::RETURN_NULL);
}

static string Generation(ClientContext &context, TableCatalogEntry &table) {
	if (table.catalog.IsDuckCatalog()) {
		return RefreshMetadata::SourceGeneration(context, table.catalog.GetName().GetIdentifierName(),
		                                         table.schema.name.GetIdentifierName(), table.name.GetIdentifierName());
	}
	if (table.catalog.GetCatalogType() == "ducklake") {
		Connection con(*context.db);
		auto identity = RefreshMetadata(con).ResolveDuckLakeSourceIdentity("", table.name.GetIdentifierName(),
		                                                                   table.catalog.GetName().GetIdentifierName(),
		                                                                   table.schema.name.GetIdentifierName());
		return identity.current_table_id < 0 ? string() : "ducklake:" + to_string(identity.current_table_id);
	}
	// Other external engines have no verified durable incarnation identifier.
	return "";
}

static unique_ptr<LogicalOperator> BindDeclaration(ClientContext &context, TableFunctionBindInput &input,
                                                   TableIndex bind_index, vector<Identifier> &names) {
	TransactionalMVLockState::Get(context).AcquireMutationLock();
	vector<string> tables;
	vector<string> columns;
	vector<string> generations;
	string program;
	case_insensitive_set_t data_catalogs;
	for (idx_t side = 0; side < 2; side++) {
		auto reference = input.inputs[side * 2].ToString();
		auto table = ResolveSource(context, reference);
		if (!table) {
			throw CatalogException("Trusted-FK source '%s' does not exist", reference);
		}
		auto column_list = SqlUtils::ParseColumnList(input.inputs[side * 2 + 1].ToString());
		if (column_list.empty()) {
			throw InvalidInputException("openivm_declare_rely_fk requires matching child/parent column lists");
		}
		for (auto &column : column_list) {
			if (!table->ColumnExists(Identifier(column))) {
				throw BinderException("Column with name %s does not exist in trusted-FK table %s", column, reference);
			}
		}
		if (side && SqlUtils::ParseColumnList(input.inputs[1].ToString()).size() != column_list.size()) {
			throw InvalidInputException("openivm_declare_rely_fk requires matching child/parent column lists");
		}
		tables.push_back(SqlUtils::FullName(table->catalog.GetName().GetIdentifierName(),
		                                    table->schema.name.GetIdentifierName(), table->name.GetIdentifierName()));
		columns.push_back(SqlUtils::JsonArray(column_list));
		if (table->catalog.IsDuckCatalog()) {
			auto &state = TransactionalMVLockState::Get(context);
			state.AcquireSourcePublicationLock(table->catalog.GetName().GetIdentifierName(),
			                                   table->schema.name.GetIdentifierName(), table->name.GetIdentifierName());
			state.VerifySourceSnapshot(table->catalog.GetName().GetIdentifierName(),
			                           table->schema.name.GetIdentifierName(), table->name.GetIdentifierName());
			Connection committed(*context.db);
			committed.context->RunFunctionInTransaction([&]() {
				auto fresh = Catalog::GetEntry<TableCatalogEntry>(
				    *committed.context, Identifier(table->catalog.GetName().GetIdentifierName()),
				    Identifier(table->schema.name.GetIdentifierName()), Identifier(table->name.GetIdentifierName()),
				    OnEntryNotFound::RETURN_NULL);
				if ((!fresh || fresh->oid != table->oid) &&
				    table->timestamp != DuckTransaction::Get(context, table->catalog).GetTransactionId()) {
					throw TransactionException(
					    "Trusted-FK source changed after this transaction's snapshot; retry the declaration");
				}
			});
			data_catalogs.insert(table->catalog.GetName().GetIdentifierName());
			program += RefreshMetadata::SourceGenerationDDL(table->catalog.GetName().GetIdentifierName(),
			                                                table->schema.name.GetIdentifierName(),
			                                                table->name.GetIdentifierName());
			generations.push_back(
			    "(SELECT generation FROM " +
			    SqlUtils::FullName(table->catalog.GetName().GetIdentifierName(), DEFAULT_SCHEMA,
			                       openivm::SOURCE_GENERATIONS_TABLE) +
			    " WHERE source_schema=" +
			    Value(StringUtil::Lower(table->schema.name.GetIdentifierName())).ToSQLString() +
			    " AND table_name=" + Value(StringUtil::Lower(table->name.GetIdentifierName())).ToSQLString() + ")");
		} else {
			auto generation = Generation(context, *table);
			if (generation.empty()) {
				throw NotImplementedException(
				    "Trusted-FK declarations require native DuckDB or DuckLake sources with durable identities");
			}
			generations.push_back(Value(generation).ToSQLString());
		}
	}
	Connection con(*context.db);
	auto location = RefreshMetadata::ResolveLocation(context, con);
	MetadataLocator::RequireWritable(location, "declare a trusted foreign key");
	data_catalogs.insert(location.catalog);
	bool staged = !location.IsNative() || data_catalogs.size() > 1;
	if (staged && !context.transaction.IsAutoCommit()) {
		throw TransactionException("Trusted-FK declaration across databases requires autocommit; local source tokens "
		                           "and control metadata cannot commit in one transaction");
	}
	auto target = location.Table(openivm::CONSTRAINTS_CACHE_TABLE);
	program += "CREATE SCHEMA IF NOT EXISTS " + SqlUtils::QuoteIdentifier(location.catalog) + "." +
	           SqlUtils::QuoteIdentifier(location.schema) + ";" +
	           CreateMetadataTableSQL(openivm::CONSTRAINTS_CACHE_TABLE, target) + ";";
	for (auto column : {"child_generation", "parent_generation"}) {
		program += "ALTER TABLE " + target + " ADD COLUMN IF NOT EXISTS " + column + " VARCHAR;";
	}
	program += "INSERT OR REPLACE INTO " + target +
	           " (table_name,constraint_kind,columns_json,referenced_table,referenced_columns_json,is_trusted,child_"
	           "generation,parent_generation) SELECT " +
	           Value(tables[0]).ToSQLString() + ", 'RELY_FK', " + Value(columns[0]).ToSQLString() + ", " +
	           Value(tables[1]).ToSQLString() + ", " + Value(columns[1]).ToSQLString() + ", true, " + generations[0] +
	           ", " + generations[1] + ";";
	names.push_back("Success");
	return BindLifecycleProgram(context, *input.binder, bind_index, program, staged);
}

TableFunction DeclareRelyFKFunction() {
	TableFunction function("openivm_register_rely_fk",
	                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       nullptr);
	function.bind_operator = BindDeclaration;
	return function;
}

vector<CachedConstraint> ReadConstraintDeclarations(ClientContext &context, const case_insensitive_set_t &sources) {
	vector<CachedConstraint> constraints;
	if (sources.empty()) {
		return constraints;
	}
	Connection con(*context.db);
	RefreshMetadata::UseSameLocation(context, con);
	auto info = con.TableInfo(Identifier(CONSTRAINTS_CACHE_TABLE));
	auto has_column = [&](const string &name) {
		if (info) {
			for (auto &column : info->columns) {
				if (StringUtil::CIEquals(column.Name().GetIdentifierName(), name)) {
					return true;
				}
			}
		}
		return false;
	};
	if (!has_column("child_generation") || !has_column("parent_generation")) {
		return constraints;
	}
	vector<string> predicates;
	for (auto &source : sources) {
		predicates.push_back(SqlUtils::IdentifierEqualsSQL("table_name", Value(source).ToSQLString()));
	}
	unordered_map<string, string> generations;
	auto generation = [&](const string &name) {
		auto key = StringUtil::Lower(name);
		auto found = generations.find(key);
		if (found != generations.end()) {
			return found->second;
		}
		auto table = ResolveSource(context, name);
		auto value = table ? Generation(context, *table) : string();
		generations.emplace(key, value);
		return value;
	};
	auto result = con.Query("SELECT table_name, constraint_kind, columns_json, referenced_table, "
	                        "referenced_columns_json, is_trusted, child_generation, parent_generation FROM " +
	                        string(CONSTRAINTS_CACHE_TABLE) + " WHERE " + StringUtil::Join(predicates, " OR "));
	if (result->HasError()) {
		return constraints;
	}
	for (idx_t r = 0; r < result->RowCount(); r++) {
		string stored_table = result->Collection().GetValue(0, r).ToString();
		if (result->Collection().GetValue(3, r).IsNull() || result->Collection().GetValue(6, r).IsNull() ||
		    result->Collection().GetValue(7, r).IsNull()) {
			continue;
		}
		auto parent = result->Collection().GetValue(3, r).ToString();
		if (!sources.count(parent) || generation(stored_table) != result->Collection().GetValue(6, r).ToString() ||
		    generation(parent) != result->Collection().GetValue(7, r).ToString()) {
			continue;
		}
		CachedConstraint c;
		c.table_name = stored_table;
		c.kind = result->Collection().GetValue(1, r).ToString();
		c.columns = SqlUtils::ParseColumnList(result->Collection().GetValue(2, r).ToString());
		if (!result->Collection().GetValue(3, r).IsNull()) {
			c.referenced_table = result->Collection().GetValue(3, r).ToString();
		}
		if (!result->Collection().GetValue(4, r).IsNull()) {
			c.referenced_columns = SqlUtils::ParseColumnList(result->Collection().GetValue(4, r).ToString());
		}
		c.is_trusted =
		    result->Collection().GetValue(5, r).IsNull() || result->Collection().GetValue(5, r).GetValue<bool>();
		if (!c.columns.empty()) {
			constraints.push_back(std::move(c));
		}
	}

	return constraints;
}

} // namespace openivm
} // namespace duckdb
