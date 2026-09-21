#ifndef OPENIVM_TIME_TRAVEL_PINS_HPP
#define OPENIVM_TIME_TRAVEL_PINS_HPP

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "lpts_pipeline.hpp"
#include "sql_dialect.hpp"

namespace duckdb {

class SQLStatement;

namespace openivm {

// Foreign snapshots cannot bind against the Spark bridge's local schema-only tables. Keep their
// parsed AT clauses while binding those stand-ins, then resolve them during LPTS's scan walk.
// Native time-travel catalogs (DuckLake) retain their clauses and use LPTS's existing scan metadata.
class TimeTravelPins {
public:
	// Peel every unbindable pin out of `statement`, recording relation -> qualifier. Throws when a
	// relation cannot be given one unambiguous pin (two different pins, or pinned in one scan and
	// unpinned in another), because re-attaching by relation would conflate the two.
	static TimeTravelPins Peel(ClientContext &context, SQLStatement &statement);

	// Peel the pins out of `statement` and discard them: the statement is only being bound or
	// planned locally, where the pin cannot be honoured and is not rendered back out.
	static void PeelForLocalBinding(ClientContext &context, SQLStatement &statement);

	// Peel the pins recorded by a stored materialized-view body without mutating anything.
	static TimeTravelPins FromViewSql(ClientContext &context, const string &view_query_sql);

	bool Empty() const {
		return pins.empty();
	}

	// Resolve pins during LPTS's existing scan walk, using the original catalog identity.
	SnapshotResolver Resolver() const;

	// Strip pins from a SELECT for local stand-in execution; stored metadata keeps the pinned query.
	string StripFrom(const string &sql) const;

	// Re-attach every recorded qualifier, in `dialect`'s own spelling, to the scans of that relation
	// in already-rendered `sql`. Refresh programs for several view shapes (min/max aggregates,
	// group recompute, interrupted-refresh recovery, ...) are assembled as SQL text rather than
	// through the AST, so the scan resolver never sees them; without this they would ship to the target
	// engine reading the latest snapshot instead of the pinned one. A raw DuckDB `AT (...)` left on
	// such a scan is replaced rather than duplicated, and scans that already carry the qualifier in
	// the target spelling are left alone, so it is safe to run over AST-rendered SQL as well. Throws
	// through LPTS for dialects with no verified time-travel syntax rather than emitting an unpinned
	// scan.
	string RestoreIntoSql(const string &sql, SqlDialect dialect) const;

private:
	struct Pin {
		string catalog;
		string schema;
		unique_ptr<AtClause> snapshot;
	};

	// Qualifiers are checked against the original catalog entry, before LPTS output overrides.
	case_insensitive_map_t<Pin> pins;
};

} // namespace openivm
} // namespace duckdb

#endif // OPENIVM_TIME_TRAVEL_PINS_HPP
