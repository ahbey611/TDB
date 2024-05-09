#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>

#include "common/log/log.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/storage_engine/recorder/table.h"

// 新增加 Lab2
#include "src/server/include/query_engine/planner/operator/index_scan_physical_operator.h"
#include "src/server/include/query_engine/structor/expression/comparison_expression.h"
#include "src/server/include/query_engine/structor/expression/expression.h"
#include "src/server/include/query_engine/structor/expression/field_expression.h"
#include "src/server/include/query_engine/structor/expression/value_expression.h"

// 新增加 Lab3
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/join_physical_operator.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator,
                                     unique_ptr<PhysicalOperator> &oper,
                                     bool is_delete) {
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator),
                         oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator),
                         oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator),
                         oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator),
                         oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator),
                         oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator),
                         oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator),
                         oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator),
                         oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator),
                         oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN: {
      return create_plan(static_cast<JoinLogicalNode &>(logical_operator),
                         oper);
    }
    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2]
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
// 首先检查扫描的table是否存在索引，如果存在可以使用的索引，那么我们可以直接生成IndexScanOperator来减少磁盘的扫描
RC PhysicalOperatorGenerator::create_plan(TableGetLogicalNode &table_get_oper,
                                          unique_ptr<PhysicalOperator> &oper,
                                          bool is_delete) {
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Index *index = nullptr;
  // TODO [Lab2] 生成IndexScanOperator的准备工作,主要包含:
  // 1. 通过predicates获取具体的值表达式， 目前应该只支持等值表达式的索引查找
  // example:
  //  if(predicate.type == ExprType::COMPARE){
  //   auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
  //   if(compare_expr->comp != EQUAL_TO) continue;
  //   [process]
  //  }
  // 2. 对应上面example里的process阶段，
  // 找到等值表达式中对应的FieldExpression和ValueExpression(左值和右值)
  // 通过FieldExpression找到对应的Index, 通过ValueExpression找到对应的Value
  // ps: 由于我们只支持单键索引，所以只需要找到一个等值表达式即可

  /*
    对于一个正常的等值表达式 a = 1，它本身是一个comparison_expression，
    a是field_expression；
    1是value_expression；
    它们被comparison_expression包含
   */

  ValueExpr *value_expression = nullptr;

  for (auto &predicate : predicates) {
    // 如果是比较表达式
    if (predicate->type() == ExprType::COMPARISON) {
      // 获取比较表达式
      auto compare_expr = dynamic_cast<ComparisonExpr *>(predicate.get());

      // 如果是等值表达式
      if (compare_expr->comp() != EQUAL_TO) continue;

      // 获取左值和右值
      Expression *left_expr = compare_expr->left().get();
      Expression *right_expr = compare_expr->right().get();

      // 如果左值是字段表达式，右值是值表达式
      if (left_expr->type() == ExprType::FIELD &&
          right_expr->type() == ExprType::VALUE) {
        // 获取字段表达式
        FieldExpr *field_expression = dynamic_cast<FieldExpr *>(left_expr);
        // 获取值表达式
        value_expression = dynamic_cast<ValueExpr *>(right_expr);
        // 获取表
        Table *table = table_get_oper.table();
        // 通过FieldExpression找到对应的Index
        index = table->find_index_by_field(field_expression->field_name());

        if (index != nullptr) {
          break;
        }
      }
    }
  }

  if (index == nullptr) {
    Table *table = table_get_oper.table();
    auto table_scan_oper = new TableScanPhysicalOperator(
        table, table_get_oper.table_alias(), table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  } else {
    // TODO [Lab2] 生成IndexScanOperator,
    // 并放置在算子树上，下面是一个实现参考，具体实现可以根据需要进行修改
    // IndexScanner
    // 在设计时，考虑了范围查找索引的情况，但此处我们只需要考虑单个键的情况
    // const Value &value = value_expression->get_value();
    // IndexScanPhysicalOperator *operator =
    //              new IndexScanPhysicalOperator(table, index, readonly,
    //              &value, true, &value, true);
    // oper = unique_ptr<PhysicalOperator>(operator);

    if (value_expression != nullptr) {
      // 先获取值
      const Value &value = value_expression->get_value();
      Table *table = table_get_oper.table();
      // 创建IndexScanPhysicalOperator
      IndexScanPhysicalOperator *index_scan_operator =
          new IndexScanPhysicalOperator(table, index, table_get_oper.readonly(),
                                        &value, true, &value, true);
      index_scan_operator->isdelete_ = is_delete;
      index_scan_operator->set_predicates(predicates);
      oper = unique_ptr<PhysicalOperator>(index_scan_operator);
      LOG_TRACE("use index scan");
    }
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(PredicateLogicalNode &pred_oper,
                                          unique_ptr<PhysicalOperator> &oper,
                                          bool is_delete) {
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1,
         "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s",
             strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1,
         "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(
      new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode &aggr_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalNode>> &child_opers = aggr_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN(
          "failed to create project logical operator's child physical "
          "operator. rc=%s",
          strrc(rc));
      return rc;
    }
  }

  auto *aggr_operator = new AggrPhysicalOperator(&aggr_oper);

  if (child_phy_oper) {
    aggr_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(aggr_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode &order_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalNode>> &child_opers = order_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN(
          "failed to create project logical operator's child physical "
          "operator. rc=%s",
          strrc(rc));
      return rc;
    }
  }

  OrderPhysicalOperator *order_operator =
      new OrderPhysicalOperator(std::move(order_oper.order_units()));

  if (child_phy_oper) {
    order_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(order_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(ProjectLogicalNode &project_oper,
                                          unique_ptr<PhysicalOperator> &oper,
                                          bool is_delete) {
  vector<unique_ptr<LogicalNode>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN(
          "failed to create project logical operator's child physical "
          "operator. rc=%s",
          strrc(rc));
      return rc;
    }
  }

  auto *project_operator = new ProjectPhysicalOperator(&project_oper);
  for (const auto &i : project_oper.expressions()) {
    project_operator->add_projector(i->copy());
  }

  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(project_operator);
  oper->isdelete_ = is_delete;

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode &insert_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  Table *table = insert_oper.table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_oper.multi_values().size(); i++) {
    vector<Value> &values = insert_oper.values(i);
    multi_values.push_back(values);
  }
  InsertPhysicalOperator *insert_phy_oper =
      new InsertPhysicalOperator(table, std::move(multi_values));
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode &delete_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalNode>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(
      new DeletePhysicalOperator(delete_oper.table()));
  oper->isdelete_ = true;
  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode &update_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalNode>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(
      update_oper.table(), update_oper.update_units()));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(ExplainLogicalNode &explain_oper,
                                          unique_ptr<PhysicalOperator> &oper,
                                          bool is_delete) {
  vector<unique_ptr<LogicalNode>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(
      new ExplainPhysicalOperator);
  for (unique_ptr<LogicalNode> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  oper->isdelete_ = is_delete;
  return rc;
}

/* 1. 获取左右子算子
2. 创建JoinPhysicalOperator
3. 将左右子算子加入到JoinPhysicalOperator中
4. 返回JoinPhysicalOperator */

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(JoinLogicalNode &join_oper,
                                          unique_ptr<PhysicalOperator> &oper) {
  vector<unique_ptr<LogicalNode>> &children = join_oper.children();
  if (children.size() != 2) {
    LOG_WARN("Join operator should have 2 children");
    return RC::VARIABLE_NOT_VALID;
  }

  unique_ptr<PhysicalOperator> left_child;
  unique_ptr<PhysicalOperator> right_child;

  RC rc = create(*children[0].get(), left_child);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init left child operator: %s", strrc(rc));
    return rc;
  }

  rc = create(*children[1].get(), right_child);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init right child operator: %s", strrc(rc));
    return rc;
  }

  // 使用join_condition构造JoinPhysicalOperator
  JoinPhysicalOperator *join_physical_operator =
      new JoinPhysicalOperator(std::move(join_oper.condition()));
  // 添加左右子算子
  join_physical_operator->add_child(std::move(left_child));
  join_physical_operator->add_child(std::move(right_child));
  // 设置
  oper = unique_ptr<PhysicalOperator>(join_physical_operator);

  return rc;
}