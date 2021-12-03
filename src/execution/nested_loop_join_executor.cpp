//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  outer_not_end_ = left_executor_->Next(&outer_tuple_, &outer_rid_);
  inner_not_end_ = right_executor_->Next(&inner_tuple_, &inner_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *outer_schema = left_executor_->GetOutputSchema();
  const Schema *inner_schema = right_executor_->GetOutputSchema();
  while (outer_not_end_ || inner_not_end_) {
    if (outer_not_end_ && inner_not_end_ &&
        (plan_->Predicate() == nullptr ||
         plan_->Predicate()->EvaluateJoin(&outer_tuple_, outer_schema, &inner_tuple_, inner_schema).GetAs<bool>())) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); ++i) {
        values.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateJoin(&outer_tuple_, outer_schema,
                                                                                        &inner_tuple_, inner_schema));
      }
      //      for (uint32_t i = 0; i < outer_schema->GetColumnCount(); ++i) {
      //        values.emplace_back(outer_tuple_.GetValue(outer_schema, i));
      //      }
      //      for (uint32_t i = 0; i < inner_schema->GetColumnCount(); ++i) {
      //        values.emplace_back(inner_tuple_.GetValue(inner_schema, i));
      //      }
      *tuple = Tuple(values, plan_->OutputSchema());
      inner_not_end_ = right_executor_->Next(&inner_tuple_, &inner_rid_);
      return true;
    }
    if (inner_not_end_) {
      inner_not_end_ = right_executor_->Next(&inner_tuple_, &inner_rid_);
    } else {
      outer_not_end_ = left_executor_->Next(&outer_tuple_, &outer_rid_);
      if (outer_not_end_) {
        right_executor_->Init();
        inner_not_end_ = right_executor_->Next(&inner_tuple_, &inner_rid_);
      }
    }
  }
  return false;
}

}  // namespace bustub
