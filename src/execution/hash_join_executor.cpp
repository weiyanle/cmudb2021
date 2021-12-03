//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    JoinKey cur_key = {plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_executor_->GetOutputSchema())};
    if (hashmap_.find(cur_key) == hashmap_.end()) {
      hashmap_[cur_key] = {tuple};
    } else {
      hashmap_[cur_key].emplace_back(tuple);
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  RID inner_rid;
  JoinKey cur_key;
  if (outer_index_ != -1) {
    cur_key.col_val_ = plan_->RightJoinKeyExpression()->Evaluate(&inner_tuple_, right_executor_->GetOutputSchema());
  }
  if (outer_index_ == -1 || hashmap_.find(cur_key) == hashmap_.end() ||
      outer_index_ == static_cast<int>(hashmap_[cur_key].size())) {
    while (true) {
      if (right_executor_->Next(&inner_tuple_, &inner_rid)) {
        cur_key.col_val_ = plan_->RightJoinKeyExpression()->Evaluate(&inner_tuple_, right_executor_->GetOutputSchema());
        if (hashmap_.find(cur_key) != hashmap_.end()) {
          outer_index_ = 0;
          break;
        }
      } else {
        return false;
      }
    }
  }
  std::vector<Value> values;
  const Schema *outer_schema = left_executor_->GetOutputSchema();
  const Schema *inner_schema = right_executor_->GetOutputSchema();
  for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); ++i) {
    values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(
        &hashmap_[cur_key][outer_index_], outer_schema, &inner_tuple_, inner_schema));
  }
  //    for (uint32_t i = 0; i < outer_schema->GetColumnCount(); ++i) {
  //        values.emplace_back(hashmap_[cur_key][outer_index_].GetValue(outer_schema, i));
  //    }
  //    for (uint32_t i = 0; i < inner_schema->GetColumnCount(); ++i) {
  //        values.emplace_back(inner_tuple_.GetValue(inner_schema, i));
  //    }
  *tuple = Tuple(values, plan_->OutputSchema());
  ++outer_index_;
  return true;
}

}  // namespace bustub
