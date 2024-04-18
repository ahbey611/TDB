#include "include/query_engine/planner/operator/index_scan_physical_operator.h"

#include "include/storage_engine/index/index.h"

// TODO [Lab2]
// IndexScanOperator的实现逻辑,通过索引直接获取对应的Page来减少磁盘的扫描

RC IndexScanPhysicalOperator::open(Trx *trx) {
  if (table_ == nullptr || index_ == nullptr) {
    return RC::INTERNAL;
  }

  const char *left_key = left_null_ ? nullptr : left_value_.data();
  const char *right_key = right_null_ ? nullptr : right_value_.data();
  IndexScanner *index_scanner = index_->create_scanner(left_key, 
                                                       left_value_.length(), 
                                                       left_inclusive_,
                                                       right_key, 
                                                       right_value_.length(), 
                                                       right_inclusive_);
  if(index_scanner == nullptr)
  {
    return RC::INTERNAL;
  }

  record_handler_ = table_->record_handler();
  if (record_handler_ == nullptr) {
    index_scanner->destroy();
    return RC::INTERNAL;
  }
  index_scanner_ = index_scanner;

  tuple_.set_schema(table_, "", table_->table_meta().field_metas());

  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next() {
  RID rid;
  record_page_handler_.cleanup();

  // TODO [Lab2]
  // 通过IndexScanner循环获取下一个RID，然后通过RecordHandler获取对应的Record
  // 在现有的查询实现中，会在调用next()方法后通过current_tuple()获取当前的Tuple,
  // 从current_tuple()的实现中不难看出,
  // 数据会通过current_record_传递到Tuple中并返回,
  // 因此该next()方法的主要目的就是将recordHandler获取到的数据填充到current_record_中

  // 提示：indexScan是基于过滤条件利用索引进行优化，当使用索引进行数据遍历时，别忘了过滤条件
  // 参考TableScanPhysicalOperator::next()的实现

  RC rc = RC::SUCCESS;
  // 过滤结果
  bool filter_result = false;

  // 先获取下一个RID
  while ((rc = index_scanner_->next_entry(&rid, this->isdelete_)) !=
         RC::RECORD_EOF) {
        // RID获取失败，退出循环
    if (rc != RC::SUCCESS) {
      return rc;
    }

    // 通过RecordHandler获取对应的Record
    Record record;
    rc = record_handler_->get_record(record_page_handler_, &rid, readonly_,
                                     &record);

    // 获取失败，退出循环
    if (rc != RC::SUCCESS) {
      return rc;
    }

    // 将Record填充到current_record_中
    current_record_ = record;

    // 通过Tuple的_set_record方法将current_record_填充到Tuple中
    tuple_._set_record(&current_record_);

    // 进行过滤
    rc = filter(tuple_, filter_result);

    // 过滤失败
    if (rc != RC::SUCCESS) {
      return rc;
    }

    // 过滤成功，返回
    if (filter_result) {
      break;
    } else {
      rc = RC::RECORD_EOF;
    }
  }

  return rc;
}

RC IndexScanPhysicalOperator::close() {
  index_scanner_->destroy();
  index_scanner_ = nullptr;
  return RC::SUCCESS;
}

Tuple *IndexScanPhysicalOperator::current_tuple() {
  tuple_._set_record(&current_record_);
  return &tuple_;
}

std::string IndexScanPhysicalOperator::param() const {
  return std::string(index_->index_meta().name()) + " ON " + table_->name();
}

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result) {
  RC rc = RC::SUCCESS;
  Value value;
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}