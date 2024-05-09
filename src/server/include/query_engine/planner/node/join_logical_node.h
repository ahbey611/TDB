#pragma once

#include <memory>

#include "logical_node.h"

// TODO [Lab3] 请根据需要实现自己的JoinLogicalNode，当前实现仅为建议实现
class JoinLogicalNode : public LogicalNode {
 public:
  JoinLogicalNode() = default;
  ~JoinLogicalNode() override = default;

  LogicalNodeType type() const override { return LogicalNodeType::JOIN; }

  void add_condition(std::unique_ptr<Expression> &&condition) {
    condition_.push_back(std::move(condition));
  }

  void set_condition(std::vector<std::unique_ptr<Expression>> &&condition) {
    condition_ = std::move(condition);
  }

  // 自己添加的
  const std::vector<std::unique_ptr<Expression>> &expressions() const {
    return expressions_;
  }

  // 获取Join的条件
  std::vector<std::unique_ptr<Expression>> &condition() { return condition_; }

  bool has_condition() const { return !condition_.empty(); }

 private:
  // Join的条件
  // 例如 SELECT * FROM t1 JOIN t2 ON t1.a = t2.b
  // condition_就是t1.a = t2.b
  std::vector<std::unique_ptr<Expression>> condition_;
};
