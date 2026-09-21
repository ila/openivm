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

	// Collect and strip foreign pins in one parse/walk, leaving an unpinned query for local binding.
	static TimeTravelPins PeelFromSql(ClientContext &context, string &view_query_sql);

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
	// engine reading the latest snapshot instead of the pinned one. Foreign DuckDB pins must already
	// be peeled from the source query. Scans with target-dialect pins are left alone. Throws
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
