#ifndef OPENIVM_VECTOR_UTILS_HPP
#define OPENIVM_VECTOR_UTILS_HPP

#include "duckdb.hpp"

namespace duckdb {

template <class T>
void AddUnique(vector<T> &entries, T entry) {
	for (auto &existing : entries) {
		if (existing == entry) {
			return;
		}
	}
	entries.push_back(std::move(entry));
}

} // namespace duckdb

#endif // OPENIVM_VECTOR_UTILS_HPP
