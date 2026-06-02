#ifndef IVM_DELTA_MODEL_HPP
#define IVM_DELTA_MODEL_HPP

#include "core/ivm_view_classifier.hpp"

namespace duckdb {

const char *DeltaMaintenanceModeName(DeltaMaintenanceMode mode);
const char *DeltaMaintenanceStateKindName(DeltaMaintenanceStateKind state);

void BuildDeltaModelNodes(DeltaViewModel &model, const CreateMVPlanFacts &facts, const vector<string> &output_names);
void BuildDeltaModelBaseAffectedDomains(DeltaViewModel &model, const CreateMVPlanFacts &facts);
void ValidateDeltaViewModelInvariants(const DeltaViewModel &model);
void PopulateDeltaViewModelLineage(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                   const vector<string> &output_names);
string BuildDeltaViewModelLineageJson(const DeltaViewModel &model);
string BuildDeltaViewModelProfileDetail(const DeltaViewModel &model);

} // namespace duckdb

#endif // IVM_DELTA_MODEL_HPP
