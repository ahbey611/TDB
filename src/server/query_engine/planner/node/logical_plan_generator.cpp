#include "include/query_engine/planner/logical_plan_generator.h"

#include <list>

#include "common/lang/bitmap.h"
#include "include/query_engine/analyzer/statement/delete_stmt.h"
#include "include/query_engine/analyzer/statement/explain_stmt.h"
#include "include/query_engine/analyzer/statement/filter_stmt.h"
#include "include/query_engine/analyzer/statement/group_by_stmt.h"
#include "include/query_engine/analyzer/statement/insert_stmt.h"
#include "include/query_engine/analyzer/statement/select_stmt.h"
#include "include/query_engine/analyzer/statement/stmt.h"
#include "include/query_engine/analyzer/statement/update_stmt.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/node/group_by_logical_node.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/structor/expression/aggregation_expression.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/conjunction_expression.h"

using namespace std;

RC LogicalPlanGenerator::create(Stmt *stmt,
                                unique_ptr<LogicalNode> &logical_node) {
  RC rc;
  switch (stmt->type()) {
    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
      rc = plan_node(select_stmt, logical_node);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);
      rc = plan_node(insert_stmt, logical_node);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
      rc = plan_node(delete_stmt, logical_node);
    } break;

    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
      rc = plan_node(update_stmt, logical_node);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);
      rc = plan_node(explain_stmt, logical_node);
    } break;

    default: {
      rc = RC::UNIMPLENMENT;
    }
  }
  return rc;
}

unique_ptr<ConjunctionExpr> _transfer_filter_stmt_to_expr(
    FilterStmt *filter_stmt) {
  std::vector<unique_ptr<Expression>> cmp_exprs;
  for (const FilterUnit *filter_unit : filter_stmt->filter_units()) {
    Expression *left_expr = filter_unit->left_expr()->copy();
    Expression *right_expr = nullptr;
    if (filter_unit->right_expr() != nullptr) {
      right_expr = filter_unit->right_expr()->copy();
    }
    auto *cmp_expr = new ComparisonExpr(filter_unit->comp(),
                                        unique_ptr<Expression>(left_expr),
                                        unique_ptr<Expression>(right_expr));
    cmp_exprs.emplace_back(cmp_expr);
  }
  if (!cmp_exprs.empty()) {
    unique_ptr<ConjunctionExpr> conjunction_expr(
        new ConjunctionExpr(filter_stmt->get_conjunction_type(), cmp_exprs));
    return conjunction_expr;
  }
  return nullptr;
}

RC LogicalPlanGenerator::plan_node(SelectStmt *select_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  const std::vector<Table *> &tables = select_stmt->tables();
  const std::vector<Field *> &all_fields = select_stmt->query_fields();
  RC rc;

  std::unique_ptr<LogicalNode> root;

  /**
   *
   * TDB > select * from join_table_1 inner join join_table_2 on
      join_table_1.id=join_table_2.id inner join join_table_3 on
      join_table_1.salary=join_table_3.salary;

   * tables是什么？
   * tables.size() = 3
   * tables = {join_table_1, join_table_2, join_table_3}
   *
   * all_fields是什么？
   * all_fields.size() = 4
   * all_fields = {join_table_1.id, join_table_1.salary,
      join_table_2.id,join_table_3.salary}
  */

  // 1. Table scan node
  // TODO [Lab3]
  // 当前只取一个表作为查询表,当引入Join后需要考虑同时查询多个表的情况 参考思路:
  // 遍历tables中的所有Table，针对每个table生成TableGetLogicalNode

  /**
   * 创建对应的Join节点，并在逻辑计划生成中往算子树中添加该节点。
   * 是否创建Join节点，应该由查询涉及的table的数量来决定，当仅有一个table时，保持原来的生成逻辑。当table数量大于1时，为了符合原有的节点逻辑，需要JoinNode将两个table的结果进行拼接。
   * JoinNode主要应该关注两个变量：孩子节点和连接条件。
   * 为了简单考虑，建议JoinNode只考虑两个子节点的情况，当存在多个table的时候，通过嵌套的方式实现Join，如下图所示（你也可以选择实现多孩子节点的JoinNode）。
   */

  // 存储所有的table_get_node节点
  std::vector<std::unique_ptr<LogicalNode>> all_table_get_node = {};
  int count = 0;
  // 遍历tables中的所有Table，针对每个table生成TableGetLogicalNode
  for (Table *table : tables) {
    // 获取表名字
    const char *table_name = table->name();
    std::vector<Field> fields;
    // 遍历属于此表的字段
    for (auto *field : all_fields) {
      if (0 == strcmp(field->table_name(), table->name())) {
        fields.push_back(*field);
      }
    }
    // 生成一个table_get_node节点
    std::unique_ptr<TableGetLogicalNode> table_get_node(new TableGetLogicalNode(
        table, select_stmt->table_alias()[count], fields, false /*readonly*/));
    // table->name()
    // 添加至所有table_get_node
    all_table_get_node.emplace_back(std::move(table_get_node));
    count++;
  }

  // 2. inner join node
  // TODO [Lab3] 完善Join节点的逻辑计划生成,
  // 需要解析并设置Join涉及的表,以及Join使用到的连接条件
  // 如果只有一个TableGetLogicalNode,直接将其设置为root节点，跳过该阶段
  // 如果有多个TableGetLogicalNode,则需要生成JoinLogicalNode进行连接
  // 生成JoinLogicalNode可以参考下面的生成流程：
  // * 遍历TableGetLogicalNode
  // * 生成JoinLogicalNode, 通过select_stmt中的join_filter_stmts
  // ps: 需要考虑table数大于2的情况

  /**
   * TDB > select * from join_table_1 inner join join_table_2 on
      join_table_1.id=join_table_2.id inner join join_table_3 on
      join_table_1.salary=join_table_3.salary;
   *
   * join_filter_stmts是什么？
   * join_filter_stmts.size() = 2
   * join_filter_stmts = {join_table_1.id=join_table_2.id,
       join_table_1.salary=join_table_3.salary}
   */

  // 根据all_table_get_node长度来做判断
  int table_get_node_size = all_table_get_node.size();
  // 如果只有一个TableGetLogicalNode
  // 例如：TDB > select * from join_table_1 inner join join_table_2 on
  // join_table_1.id=join_table_2.id;
  /**
   * 不用生成JoinLogicalNode
   * 【FilterNofe】
   *       |
   * 【TableGetLogicalNode】
   */
  if (table_get_node_size == 1) {
    // 直接将其设置为root节点，跳过该阶段
    root = std::move(all_table_get_node[0]);
  }
  // 否则考虑>=2的情况
  /**
   * 两个表，需要生成JoinLogicalNode
   *                  【FilterNode】
   *                        |
   *                【JoinLogicalNode】
   *                    /     \
   * 【TableGetLogicalNode】【TableGetLogicalNode】
   *
   *
   * >= 两个表，递归生成JoinLogicalNode
   *                            【FilterNode】
   *                                  |
   *                          【JoinLogicalNode】
   *                               /     \
   *                【JoinLogicalNode】【TableGetLogicalNode】
   *                    /     \
   * 【TableGetLogicalNode】【TableGetLogicalNode】
   */
  else {
    std::vector<FilterStmt *> filter_stmt = select_stmt->join_filter_stmts();
    int join_filter_node_size = filter_stmt.size();
    // LOG_ERROR("join_filter_node_size(几个join): %d", join_filter_node_size);
    // LOG_ERROR("table_get_node_size(几个table): %d", table_get_node_size);

    // JoinLogicalNode的左右孩子节点
    std::unique_ptr<LogicalNode> left_node = nullptr;
    std::unique_ptr<LogicalNode> right_node = nullptr;
    // 递归生成JoinLogicalNode
    // while (t_size < table_get_node_size) {
    for (int t_size = 1; t_size < table_get_node_size; t_size++) {
      // LOG_ERROR("t_size: %d", t_size);
      //  生成JoinLogicalNode
      std::unique_ptr<JoinLogicalNode> join_node(new JoinLogicalNode);

      // 两个Node之间的连接条件
      FilterStmt *filter = filter_stmt[t_size - 1];

      // 取出filter_units_
      std::vector<FilterUnit *> filter_units_ = filter->filter_units();

      // 打印filter_units_信息
      /* LOG_ERROR("filter_units_.size(): %d", filter_units_.size());
      for (FilterUnit *filter_unit : filter_units_) {
        LOG_ERROR("filter_unit->left_expr()->name(): %s",
                  filter_unit->left_expr()->name().c_str());
        LOG_ERROR("filter_unit->right_expr()->name(): %s",
                  filter_unit->right_expr()->name().c_str());
      } */

      // 提取filterUnit中的左右表达式
      std::vector<std::unique_ptr<Expression>> all_condition_expr;
      std::unique_ptr<Expression> comparison_expr_ptr;
      for (FilterUnit *filter_unit : filter_units_) {
        // 比较符号
        CompOp comp_ = filter_unit->comp();
        // 创建比较表达式ComparisonExpr
        ComparisonExpr *comparison_expr = new ComparisonExpr(
            comp_, std::unique_ptr<Expression>(filter_unit->left_expr()),
            std::unique_ptr<Expression>(filter_unit->right_expr()));
        // 转为Expression类unique_ptr
        comparison_expr_ptr = std::unique_ptr<Expression>(comparison_expr);

        // 设置连接条件
        // join_node->set_condition(std::move(comparison_expr_ptr));
        all_condition_expr.push_back(std::move(comparison_expr_ptr));
      }
      join_node->set_condition(std::move(all_condition_expr));

      // {1，2，3，4，5}
      // 1，2先拼接，拼接结果为root，然后和3拼接，然后和4拼接，然后和5拼接
      if (t_size == 1) {
        // 设置左右孩子节点
        left_node = std::move(all_table_get_node[t_size - 1]);
        right_node = std::move(all_table_get_node[t_size]);
      } else {
        // 设置左右孩子节点
        left_node = std::move(root);
        right_node = std::move(all_table_get_node[t_size]);
      }
      join_node->add_child(std::move(left_node));
      join_node->add_child(std::move(right_node));
      // 设置root
      root = std::move(join_node);
    }
  }

  // 3. Table filter node
  auto *table_filter_stmt = select_stmt->filter_stmt();
  unique_ptr<LogicalNode> predicate_node;
  plan_node(table_filter_stmt, predicate_node);
  if (predicate_node) {
    predicate_node->add_child(std::move(root));
    root = std::move(predicate_node);
  }

  // 4. aggregation node
  std::vector<AggrExpr *> aggr_exprs;
  for (auto *expr : select_stmt->projects()) {
    AggrExpr::getAggrExprs(expr, aggr_exprs);
  }
  if (!aggr_exprs.empty()) {
    unique_ptr<LogicalNode> aggr_node =
        unique_ptr<LogicalNode>(new AggrLogicalNode(aggr_exprs));
    aggr_node->add_child(std::move(root));
    root = std::move(aggr_node);
  }

  // 5. Having filter node
  if (select_stmt->having_stmt() != nullptr) {
    for (auto *filter_unit : select_stmt->having_stmt()->filter_units()) {
      AggrExpr::getAggrExprs(filter_unit->left_expr(), aggr_exprs);
      AggrExpr::getAggrExprs(filter_unit->right_expr(), aggr_exprs);
    }
  }
  if (select_stmt->having_stmt() != nullptr &&
      !select_stmt->having_stmt()->filter_units().empty()) {
    unique_ptr<LogicalNode> having_node;
    plan_node(select_stmt->having_stmt(), having_node);
    having_node->add_child(std::move(root));
    root = std::move(having_node);
  }

  // 6. Sort node
  if (select_stmt->order_stmt() != nullptr &&
      !select_stmt->order_stmt()->order_units().empty()) {
    unique_ptr<LogicalNode> order_node = unique_ptr<LogicalNode>(
        new OrderByLogicalNode(select_stmt->order_stmt()->order_units()));
    order_node->add_child(std::move(root));
    root = std::move(order_node);
  }

  // 7. project node
  unique_ptr<LogicalNode> project_logical_node =
      unique_ptr<LogicalNode>(new ProjectLogicalNode(select_stmt->projects()));
  project_logical_node->add_child(std::move(root));
  root = std::move(project_logical_node);

  logical_node.swap(root);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::plan_node(FilterStmt *filter_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  auto conjunction_expr = _transfer_filter_stmt_to_expr(filter_stmt);
  unique_ptr<PredicateLogicalNode> predicate_node;
  if (conjunction_expr != nullptr) {
    predicate_node =
        std::make_unique<PredicateLogicalNode>(std::move(conjunction_expr));
    logical_node = std::move(predicate_node);
  }
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::plan_node(InsertStmt *insert_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  Table *table = insert_stmt->table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_stmt->record_amount(); i++) {
    vector<Value> values(insert_stmt->values(i),
                         insert_stmt->values(i) + insert_stmt->value_amount());
    multi_values.push_back(values);
  }
  InsertLogicalNode *insert_node = new InsertLogicalNode(table, multi_values);
  logical_node.reset(insert_node);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::plan_node(DeleteStmt *delete_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  Table *table = delete_stmt->table();
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num();
       i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalNode> table_get_node(new TableGetLogicalNode(
      table, table->name(), fields, false /*readonly*/));

  unique_ptr<LogicalNode> predicate_node;
  RC rc = plan_node(filter_stmt, predicate_node);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalNode> delete_node(new DeleteLogicalNode(table));

  if (predicate_node) {
    predicate_node->add_child(std::move(table_get_node));
    delete_node->add_child(std::move(predicate_node));
  } else {
    delete_node->add_child(std::move(table_get_node));
  }

  logical_node = std::move(delete_node);
  return rc;
}

RC LogicalPlanGenerator::plan_node(UpdateStmt *update_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  Table *table = update_stmt->table();
  FilterStmt *filter_stmt = update_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num();
       i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalNode> table_get_node(new TableGetLogicalNode(
      table, table->name(), fields, false /*readonly*/));

  unique_ptr<LogicalNode> predicate_node;
  RC rc = plan_node(filter_stmt, predicate_node);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  std::vector<UpdateUnit> update_units;
  for (const UpdateUnit update_unit : update_stmt->update_units()) {
    Expression *expr = update_unit.value->copy();
    UpdateUnit unit;
    unit.value = expr;
    unit.attribute_name = update_unit.attribute_name;
    update_units.emplace_back(unit);
  }

  unique_ptr<LogicalNode> update_node(
      new UpdateLogicalNode(table, update_units));

  if (predicate_node) {
    predicate_node->add_child(std::move(table_get_node));
    update_node->add_child(std::move(predicate_node));
  } else {
    update_node->add_child(std::move(table_get_node));
  }

  logical_node = std::move(update_node);
  return rc;
}

RC LogicalPlanGenerator::plan_node(ExplainStmt *explain_stmt,
                                   unique_ptr<LogicalNode> &logical_node) {
  Stmt *child_stmt = explain_stmt->child();
  unique_ptr<LogicalNode> child_node;
  RC rc = create(child_stmt, child_node);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child node. rc=%s", strrc(rc));
    return rc;
  }

  logical_node = unique_ptr<LogicalNode>(new ExplainLogicalNode);
  logical_node->add_child(std::move(child_node));
  return rc;
}