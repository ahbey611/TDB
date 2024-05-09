#pragma once

#include "include/query_engine/parser/value.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/conjunction_expression.h"
#include "include/query_engine/structor/expression/expression.h"
#include "include/query_engine/structor/tuple/join_tuple.h"
#include "include/query_engine/structor/tuple/tuple.h"
#include "include/query_engine/structor/tuple/tuple_cell.h"
#include "include/storage_engine/recorder/record.h"
#include "include/storage_engine/recorder/record_manager.h"
#include "physical_operator.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator {
 public:
  JoinPhysicalOperator();
  JoinPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> join_condition_);
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

 private:
  RC loadLeftTuples();
  RC loadRightTuples();
  RC matchLeftRightTuples();
  void printLeftRightRecords();

  // ================================================
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  // 自己添加的
  std::vector<std::unique_ptr<Expression>> join_condition;
  Tuple *left_tuple_ = nullptr;
  Tuple *right_tuple_ = nullptr;
  std::vector<std::vector<Record *>>
      left_tuples_records;  // 记录左tuple的Record
  std::vector<std::vector<Record *>>
      right_tuples_records;  // 记录右tuple的Record
  std::vector<std::pair<int, int>>
      left_right_match_index;  // 记录左右tuple匹配的下标
  int current_index = 0;       // left_right_match_index的当前下标
};
