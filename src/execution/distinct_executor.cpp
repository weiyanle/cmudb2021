//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), iter_(hashset_.begin()) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> keys;
    for (const Column &clm : plan_->OutputSchema()->GetColumns()) {
      keys.emplace_back(tuple.GetValue(plan_->GetChildPlan()->OutputSchema(),
                                       plan_->GetChildPlan()->OutputSchema()->GetColIdx(clm.GetName())));
    }
    DistinctKey key = {keys};
    hashset_.emplace(key);
  }
  iter_ = hashset_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ != hashset_.end()) {
    *tuple = Tuple(iter_->col_vals_, plan_->OutputSchema());
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
