#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/
/**
 * 坑点：第二次进入循环时，right tuple没有遍历到
  错误
  Left tuple: 1, a,
  Right tuple: 1, 2,
  Right tuple: 2, 15,
  ============================
  Left tuple: 2, b,
  ============================
  Left tuple: 3, c,
  ============================

   正确
  * Left tuple: 1, a,
   Right tuple: 1, 2,
   Right tuple: 2, 15,
   ============================
   Left tuple: 2, b,
   Right tuple: 1, 2,
   Right tuple: 2, 15,
   ============================
   Left tuple: 3, c,
   Right tuple: 1, 2,
   Right tuple: 2, 15,
   ============================

 * 解决：
      左右tuple分别遍历一次；
        将tuple的Record存储起来；
      进行联合遍历；
        每次遍历都设置左右tuple的Record；
        满足条件的存储下标；
 */

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 从多个条件表达式中构造JoinPhysicalOperator
JoinPhysicalOperator ::JoinPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> join_condition_) {
  join_condition = std::move(join_condition_);
}

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx) {
  LOG_WARN("JoinPhysicalOperator open");

  trx_ = trx;
  // 检查左右子算子是否存在
  if (children_.size() != 2) {
    LOG_WARN("Join operator should have 2 children");
    return RC::VARIABLE_NOT_VALID;
  }

  // 打开左右子算子
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init left child operator: %s", strrc(rc));
    return rc;
  }
  rc = children_[1]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init right child operator: %s", strrc(rc));
    return rc;
  }

  // 加载左tuple
  rc = loadLeftTuples();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to load left tuples: %s", strrc(rc));
    return rc;
  }

  // 加载右tuple
  rc = loadRightTuples();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 进行匹配
  rc = matchLeftRightTuples();
  if (rc != RC::SUCCESS) {
    return rc;
  }
  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next() {
  RC rc = RC::SUCCESS;

  int left_right_match_index_size = left_right_match_index.size();

  // 完全没有匹配的数据
  if (left_right_match_index_size == 0) return RC::RECORD_EOF;

  // 已经匹配完所有的数据
  if (current_index >= left_right_match_index_size) return RC::RECORD_EOF;

  // 获取之前已经匹配的左右tuple的下标
  int left_index = left_right_match_index[current_index].first;
  int right_index = left_right_match_index[current_index].second;

  // 左右tuple设置Record
  left_tuple_->set_record(left_tuples_records[left_index]);
  right_tuple_->set_record(right_tuples_records[right_index]);

  // 设置joined_tuple的左右tuple
  joined_tuple_.set_left(left_tuple_);
  joined_tuple_.set_right(right_tuple_);

  // 下标加1
  current_index++;

  // 还有数据
  if (current_index - 1 < left_right_match_index_size) return RC::SUCCESS;

  return RC::RECORD_EOF;
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close() {
  // 关闭左子算子
  RC rc = children_[0]->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left child operator: %s", strrc(rc));
    return rc;
  }
  // 关闭右子算子
  rc = children_[1]->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close right child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

// 返回当前的tuple
Tuple *JoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

// 匹配所有的左右tuple
RC JoinPhysicalOperator::matchLeftRightTuples() {
  RC rc = RC::SUCCESS;

  int left_tuple_record_size = left_tuples_records.size();
  int right_tuple_record_size = right_tuples_records.size();

  // 嵌套循环连接 neested loop join
  for (int i = 0; i < left_tuple_record_size; i++) {
    // 内层
    for (int j = 0; j < right_tuple_record_size; j++) {
      // 设置左右tuple的record
      std::vector<Record *> left_tuple_record = left_tuples_records[i];
      std::vector<Record *> right_tuple_record = right_tuples_records[j];
      left_tuple_->set_record(left_tuple_record);
      right_tuple_->set_record(right_tuple_record);
      // 设置joined_tuple的左右tuple

      // JoinedTuple joined_tuple;
      joined_tuple_.set_left(left_tuple_);
      joined_tuple_.set_right(right_tuple_);

      bool join_result = false;  // join的结果

      // 遍历每一个join的条件
      // 例如：a inner join b on a.id = b.id and a.name = b.name
      // 假设都以 AND 连接
      for (std::unique_ptr<Expression> &condition : join_condition) {
        // 转换为ComparisonExpr
        ComparisonExpr *comparison_expr =
            dynamic_cast<ComparisonExpr *>(condition.get());

        if (comparison_expr != nullptr) {
          Value join_tuple_value;
          // get_value中会调用compare_value
          comparison_expr->get_value(joined_tuple_, join_tuple_value);
          // 将join的结果转化为bool
          join_result = join_tuple_value.get_boolean();

          // 如果join的结果为false，即不满足其中一个条件，直接跳出
          if (!join_result) {
            break;
          }
        }
        // 转化失败
        else {
          LOG_WARN("comparison_expr is nullptr");
          return RC::INVALID_ARGUMENT;
        }
      }

      // 满足所有的条件，记录下标
      if (join_result) {
        left_right_match_index.push_back(std::make_pair(i, j));
      }
    }
  }
  return rc;
}

// 拿到left_tuple中的record
RC JoinPhysicalOperator::loadLeftTuples() {
  RC rc = RC::SUCCESS;
  while (true) {
    rc = children_[0]->next();
    if (rc == RC::RECORD_EOF) {
      return RC::SUCCESS;
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to init left child operator: %s", strrc(rc));
      return rc;
    }
    left_tuple_ = children_[0]->current_tuple();

    // 拷贝record
    std::vector<Record *> left_tuple_record_copy;
    std::vector<Record *> left_tuple_record;
    left_tuple_->get_record(left_tuple_record);
    for (Record *record : left_tuple_record) {
      left_tuple_record_copy.push_back(new Record(*record));
    }
    left_tuples_records.push_back(left_tuple_record_copy);
  }

  return rc;
}

// 拿到right_tuple中的record
RC JoinPhysicalOperator::loadRightTuples() {
  RC rc = RC::SUCCESS;
  while (true) {
    rc = children_[1]->next();
    if (rc == RC::RECORD_EOF) {
      return RC::SUCCESS;
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to init right child operator: %s", strrc(rc));
      return rc;
    }
    right_tuple_ = children_[1]->current_tuple();

    // 拷贝record
    std::vector<Record *> right_tuple_record_copy;
    std::vector<Record *> right_tuple_record;
    right_tuple_->get_record(right_tuple_record);
    for (Record *record : right_tuple_record) {
      right_tuple_record_copy.push_back(new Record(*record));
    }
    right_tuples_records.push_back(right_tuple_record_copy);
  }

  return rc;
}

void JoinPhysicalOperator::printLeftRightRecords() {
  LOG_WARN("-------------------------------------");
  LOG_WARN("Left tuples: ");
  for (int i = 0; i < left_tuples_records.size(); i++) {
    LOG_WARN("Left tuple: ");
    for (int j = 0; j < left_tuples_records[i].size(); j++) {
      LOG_WARN("Left record: %p", left_tuples_records[i][j]);
      LOG_WARN("data: %s", left_tuples_records[i][j]->to_string().c_str());
    }
  }

  LOG_WARN("Right tuples: ");
  for (int i = 0; i < right_tuples_records.size(); i++) {
    LOG_WARN("Right tuple: ");
    for (int j = 0; j < right_tuples_records[i].size(); j++) {
      LOG_WARN("Right record: %p", right_tuples_records[i][j]);
      LOG_WARN("data: %s", right_tuples_records[i][j]->to_string().c_str());
    }
  }
  LOG_WARN("-------------------------------------");
}