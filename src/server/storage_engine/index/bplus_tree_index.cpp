#include "include/storage_engine/index/bplus_tree_index.h"

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(const char *file_name, const IndexMeta &index_meta,
                          const std::vector<FieldMeta> &multi_field_metas) {
  if (inited_) {
    LOG_WARN(
        "Failed to create index due to the index has been inited before. "
        "file_name:%s, index:%s, field_amount:%d, field_names:%s",
        file_name, index_meta.name(), index_meta.field_amount(),
        index_meta.multi_fields());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, multi_field_metas);

  std::vector<AttrType> multi_attr_types;
  std::vector<int> multi_attr_length;
  for (int i = 0; i < multi_field_metas.size(); i++) {
    multi_attr_types.emplace_back(multi_field_metas[i].type());
    multi_attr_length.emplace_back(multi_field_metas[i].len());
  }

  RC rc = index_handler_.create(file_name, index_meta.is_unique(),
                                multi_attr_types, multi_attr_length);
  if (RC::SUCCESS != rc) {
    LOG_WARN(
        "Failed to create index_handler, file_name:%s, index:%s, "
        "field_amount:%d, field_names:%s rc:%s",
        file_name, index_meta.name(), index_meta.field_amount(),
        index_meta.multi_fields(), strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully create index, file_name:%s, index:%s, field_amount:%d, "
      "field_names:%s",
      file_name, index_meta.name(), index_meta.field_amount(),
      index_meta.multi_fields());
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(const char *file_name, const IndexMeta &index_meta,
                        const std::vector<FieldMeta> &multi_field_metas) {
  if (inited_) {
    LOG_WARN(
        "Failed to open index due to the index has been inited before. "
        "file_name:%s, index:%s, field_amount:%d,  field_names:%s",
        file_name, index_meta.name(), index_meta.field_amount(),
        index_meta.multi_fields());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, multi_field_metas);

  RC rc = index_handler_.open(file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN(
        "Failed to open index_handler, file_name:%s, index:%s, "
        "field_amount:%d, field_names:%s, rc:%s",
        file_name, index_meta.name(), index_meta.field_amount(),
        index_meta.multi_fields(), strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully open index, file_name:%s, index:%s, field_amount:%d, "
      "field_names:%s",
      file_name, index_meta.name(), index_meta.field_amount(),
      index_meta.multi_fields());
  return RC::SUCCESS;
}

RC BplusTreeIndex::close() {
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field_amount:%d, field_names:%s",
             index_meta_.name(), index_meta_.field_amount(),
             index_meta_.multi_fields());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

/**
 * 由于支持多字段索引，需要从record中取出multi_field_metas_中的字段值，作为key。
 * 需要调用BplusTreeHandler的insert_entry完成插入操作。
 * 注意如果是唯一索引（unique），需要判断是否存在重复的字段值，如果有，返回RECORD_DUPLICATE_KEY，插入失败。
 */

/**
 温馨提示：
    1. 注意TDB支持在多列字段上创建索引
    2. 需要调用BplusTreeHandler的insert_entry和delete_entry方法
    3. 考虑各种异常的情况，比如在唯一索引上插入了重复数据
*/
RC BplusTreeIndex::insert_entry(const char *record, const RID *rid) {
  // TODO [Lab2] 增加索引项的处理逻辑
  /* Index基类有std::vector<FieldMeta> multi_field_metas_; */

  // 从record中取出multi_field_metas_中的字段值，作为key
  std::vector<const char *> multi_keys;
  int length = multi_field_metas_.size();
  for (int i = 0; i < length; i++) {
    // multi_field_metas_中的字段值
    multi_keys.emplace_back(record + multi_field_metas_[i].offset());
  }

  // 考虑唯一索引的情况
  bool is_unique = index_meta().is_unique();

  // 如果是唯一索引，需要判断是否存在重复的字段值
  if (is_unique) {
    // 调用BplusTreeHandler的get_entry方法，查找是否存在重复的字段值
    std::list<RID> find_rids;
    RC rc = index_handler_.get_entry(multi_keys.data(), find_rids, length);
    // 如果get_entry失败，返回错误
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get entry. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
    // 如果存在重复的字段值，返回RECORD_DUPLICATE_KEY，插入失败
    if (!find_rids.empty()) {
      LOG_WARN("duplicate key");
      return RC::RECORD_DUPLICATE_KEY;
    }
  }

  // 调用BplusTreeHandler的insert_entry完成插入操作
  RC rc = index_handler_.insert_entry(multi_keys.data(), rid, length);

  // 如果insert_entry失败，返回错误
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert entry. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // 验证b+树是否合法
  if (!index_handler_.validate_tree()) {
    LOG_WARN("failed to validate tree");
    return RC::INTERNAL;
  }

  return RC::SUCCESS;
}

/**
 * 由于支持多字段索引，需要从record中取出multi_field_metas_中的字段值，作为key。
 * 需要调用BplusTreeHandler的delete_entry完成插入操作。
 */
RC BplusTreeIndex::delete_entry(const char *record, const RID *rid) {
  // TODO [Lab2] 增加索引项的处理逻辑
  std::vector<const char *> multi_keys;
  int length = multi_field_metas_.size();
  for (int i = 0; i < length; i++) {
    multi_keys.emplace_back(record + multi_field_metas_[i].offset());
  }

  RC rc = index_handler_.delete_entry(multi_keys.data(), rid, length);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to delete entry. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // 验证b+树是否合法
  if (!index_handler_.validate_tree()) {
    LOG_WARN("failed to validate tree");
    return RC::INTERNAL;
  }
  return RC::SUCCESS;
}

IndexScanner *BplusTreeIndex::create_scanner(const char *left_key, int left_len,
                                             bool left_inclusive,
                                             const char *right_key,
                                             int right_len,
                                             bool right_inclusive) {
  BplusTreeIndexScanner *index_scanner =
      new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key,
                              right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////

BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler)
    : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept {
  tree_scanner_.close();
}

RC BplusTreeIndexScanner::open(const char *left_key, int left_len,
                               bool left_inclusive, const char *right_key,
                               int right_len, bool right_inclusive) {
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key,
                            right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid, bool isdelete) {
  return tree_scanner_.next_entry(*rid, isdelete);
}

RC BplusTreeIndexScanner::destroy() {
  delete this;
  return RC::SUCCESS;
}