#ifndef OPENIVM_TIME_TRAVEL_PINS_HPP
#define OPENIVM_TIME_TRAVEL_PINS_HPP

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "lpts_ast.hpp"
#include "sql_dialect.hpp"

namespace duckdb {

class SQLStatement;

namespace openivm {

// A time-travel pin (`FROM t AT (VERSION => 366)`, the DuckDB spelling LPTS normalises Spark's
// `FROM t VERSION AS OF 366` into) is only bindable when the relation lives in a catalog that
// implements time travel. OpenIVM routinely binds against catalogs that do not: the Spark bridge
// registers schema-only stand-ins with plain in-memory `CREATE TABLE`, so binding a pinned scan
// fails with `Catalog type does not support time travel`.
//
// Dropping the pin to make binding succeed would silently turn a pinned scan into a read of the
// latest snapshot, so instead the pin is *peeled* off the parsed statement, kept here keyed by the
// relation it belongs to, and re-attached to the matching `AstGetNode` once the plan has been
// converted back to an AST. LPTS then renders it in the target dialect (`VERSION AS OF 366` for
// Spark) and refuses for dialects with no verified time-travel syntax.
//
// Pins on catalogs that *do* support time travel (DuckLake) are left in place: those bind natively
// and already round-trip through LPTS.
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

	// Re-attach every recorded qualifier to the `AstGetNode`s naming that relation.
	void RestoreInto(AstNode &ast) const;

	// Remove every recorded qualifier from `sql`. Generated SQL that OpenIVM executes itself binds
	// against the same catalog the pin was peeled from, so it must not carry the pin; the pin is
	// kept in the stored view SQL and re-attached when rendering for a foreign dialect.
	string StripFrom(const string &sql) const;

	// Re-attach every recorded qualifier, in `dialect`'s own spelling, to the scans of that relation
	// in already-rendered `sql`. Refresh programs for several view shapes (min/max aggregates,
	// group recompute, interrupted-refresh recovery, ...) are assembled as SQL text rather than
	// through the AST, so `RestoreInto` never sees them; without this they would ship to the target
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
		string suffix; // " AT (VERSION => 366)"
	};

	// Keyed by the bare table name: `AstGetNode` carries catalog/schema separately, and an
	// unqualified reference resolves its catalog/schema only during binding.
	case_insensitive_map_t<Pin> pins;
};

// Spark writes a pin *between* a relation and its alias (`FROM t VERSION AS OF 366 v`) where DuckDB
// wants the alias first (`FROM t AS v AT (VERSION => 366)`), so normalising the body has to carry
// the alias across the rewrite. That is the one step where a pin could silently land on a
// neighbouring relation, attach to the wrong alias, or be dropped altogether — and any of those
// reads a different snapshot while still compiling cleanly.
//
// So the association is checked end to end rather than trusted: the bindings are read straight off
// the *source* text, before normalisation, and re-checked against the parse tree DuckDB actually
// produced. This is deliberately an independent derivation of the same facts, not a reuse of the
// normaliser's own bookkeeping, so a regression there fails the compile instead of passing quietly.
struct SnapshotBinding {
	string relation;  // relation the pin was written against
	string alias;     // alias that relation carries, unquoted; empty when it has none
	string qualifier; // "AT (VERSION => 366)"
};

// Read every time-travel pin written in `sql` using `dialect`'s own spelling, together with the
// relation and alias it is written against. Empty for dialects with no temporal syntax of their own.
vector<SnapshotBinding> CollectSourceSnapshotBindings(const string &sql, SqlDialect dialect);

// Throw unless every source binding survived normalisation and parsing with its pin still on the
// same relation and alias. Compiling a mis-associated or dropped pin would read the wrong snapshot.
void VerifySnapshotBindings(SQLStatement &statement, const vector<SnapshotBinding> &bindings);

} // namespace openivm
} // namespace duckdb

#endif // OPENIVM_TIME_TRAVEL_PINS_HPP
