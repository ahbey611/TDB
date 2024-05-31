#include "include/storage_engine/transaction/mvcc_trx.h"

using namespace std;

/**
 * MVCC原理
 *  版本号与可见性
 *
与常见的MVCC实现方案相似，在TDB中使用单调递增的数字来作为版本号，并且在表上增加两个额外的字段来表示这条记录有效的版本范围。
两个版本字段是begin_xid和end_xid，其中begin_xid和插入相关，end_xid和删除相关。
每个事务在开始时，就会生成一个自己的版本号，当访问某条记录时，判断自己的版本号是否在该条记录的版本号的范围内，如果在，就是可见的，否则就不可见。
 */

/**  记录版本号与事务版本号
行数据上的版本号，是事务设置的，这个版本号也是事务的版本号。一个写事务，通常会有两个版本号，在启动时，会生成一个版本号，用来在运行时做数据的可见性判断。
在提交时，会再生成一个版本号，这个版本号是最终设置在记录上的。

*/

/**
 * *  版本号与插入删除
新插入的记录，在提交后，它的版本号是 begin_xid =
事务提交版本号，end_xid=无穷大。表示此数据从当前事务开始生效，对此后所有的新事务都可见。而删除相反，begin_xid
保持不变，而
end_xid变成了当前事务提交的版本号。表示这条数据对当前事务之后的新事务，就不可见了。
记录还有一个中间状态，就是事务刚插入或者删除，但是还没有提交时，这里的修改对其它事务应该都是不可见的。
比如新插入一条数据，只有当前事务可见，而新删除的数据，只有当前事务不可见。需要使用一种特殊的方法来标记，当然也是在版本号上做动作。对插入的数据，begin_xid
改为 (-当前事务版本号)(负数)，删除记录将end_xid改为
(-当前事务版本号)。在做可见性判断时，对负版本号做特殊处理即可。假设某个事务运行时trx
id是 Ta，提交时是 Tc：
*/

MvccTrxManager::~MvccTrxManager() {
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);
  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxManager::init() {
  fields_ = vector<FieldMeta>{
      FieldMeta("__trx_xid_begin", AttrType::INTS, 0 /*attr_offset*/,
                4 /*attr_len*/, false /*visible*/),
      FieldMeta("__trx_xid_end", AttrType::INTS, 4 /*attr_offset*/,
                4 /*attr_len*/, false /*visible*/)};
  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxManager::trx_fields() const { return &fields_; }

Trx *MvccTrxManager::create_trx(RedoLogManager *log_manager) {
  Trx *trx = new MvccTrx(*this, log_manager);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

Trx *MvccTrxManager::create_trx(int32_t trx_id) {
  Trx *trx = new MvccTrx(*this, trx_id);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    lock_.unlock();
  }
  return trx;
}

void MvccTrxManager::destroy_trx(Trx *trx) {
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend;
       ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();
  delete trx;
}

Trx *MvccTrxManager::find_trx(int32_t trx_id) {
  lock_.lock();
  for (Trx *trx : trxes_) {
    if (trx->id() == trx_id) {
      lock_.unlock();
      return trx;
    }
  }
  lock_.unlock();
  return nullptr;
}

void MvccTrxManager::all_trxes(std::vector<Trx *> &trxes) {
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

int32_t MvccTrxManager::next_trx_id() { return ++current_trx_id_; }

int32_t MvccTrxManager::max_trx_id() const {
  return numeric_limits<int32_t>::max();
}

////////////////////////////////////////////////////////////////////////////////
MvccTrx::MvccTrx(MvccTrxManager &kit, RedoLogManager *log_manager)
    : trx_kit_(kit), log_manager_(log_manager) {}

MvccTrx::MvccTrx(MvccTrxManager &kit, int32_t trx_id)
    : trx_kit_(kit), trx_id_(trx_id) {
  started_ = true;
  recovering_ = true;
}

/**
 * 插入一行数据。事务可能需要对记录做一些修改，然后调用table的插入记录接口。提交之前插入的记录通常对其它事务不可见
 */
RC MvccTrx::insert_record(Table *table, Record &record) {
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现记录的插入，相关提示见文档
  // LOG_ERROR("插入：trx id=%d", trx_id_);

  //  获取表的版本号字段
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);

  // 未提交时，begin版本号为负数
  begin_xid_field.set_int(record, -trx_id_);

  // end版本号为+∞
  end_xid_field.set_int(record, trx_kit_.max_trx_id());

  /* LOG_ERROR("插入：begin_xid=%d end_xid=%d", begin_xid_field.get_int(record),
            end_xid_field.get_int(record)); */

  // 进行插入记录
  rc = table->insert_record(record);
  // 插入记录失败
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record. rc=%s", strrc(rc));
    return rc;
  }

  //  插入操作
  pair<OperationSet::iterator, bool> ret = operations_.insert(
      Operation(Operation::Type::INSERT, table, record.rid()));
  // 插入失败, 说明有重复的操作
  if (!ret.second) {
    rc = RC::INTERNAL;
    LOG_WARN(
        "failed to insert operation(insertion) into operation set: duplicate");
  }
  return rc;
}

/**
 * 参考TDB事务模块解析的版本号与可见性原理，对记录的新增字段做一些修改，
 * 对MVCC来说，并不是真正的将其删除，而是让他对其它事务不可见（提交后）
 */
RC MvccTrx::delete_record(Table *table, Record &record) {
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现逻辑上的删除，相关提示见文档

  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);
  // 未提交时，begin版本号不变
  // 未提交时，end版本号为负数
  end_xid_field.set_int(record, -trx_id_);
  /* LOG_ERROR("删除：begin_xid=%d end_xid=%d", begin_xid_field.get_int(record),
            end_xid_field.get_int(record)); */

  operations_.insert(Operation(Operation::Type::DELETE, table, record.rid()));
  return rc;
}

/**
 * @brief 当访问到某条数据时，使用此函数来判断是否可见，或者是否有访问冲突
 * @param table    要访问的数据属于哪张表
 * @param record   要访问哪条数据
 * @param readonly 是否只读访问
 * @return RC      - SUCCESS 成功
 *                 - RECORD_INVISIBLE 此数据对当前事务不可见，应该跳过
 *                 - LOCKED_CONCURRENCY_CONFLICT 与其它事务有冲突
 */
RC MvccTrx::visit_record(Table *table, Record &record, bool readonly) {
  RC rc = RC::SUCCESS;
  // TODO [Lab4] 需要同学们补充代码实现记录是否可见的判断，相关提示见文档

  Field begin_xid_field, end_xid_field;
  // 获取表的版本号字段
  trx_fields(table, begin_xid_field, end_xid_field);
  /*
  LOG_ERROR("访问：trx id=%d", trx_id_);
  LOG_ERROR("访问：begin_xid=%d end_xid=%d, readOnly=%d",
            begin_xid_field.get_int(record), end_xid_field.get_int(record),
            readonly); */

  // *************************未提交的事务*******************

  // 插入：如果begin_xid为负数，表示未提交
  if (begin_xid_field.get_int(record) < 0) {
    // 只有当前事务自己可见
    if (begin_xid_field.get_int(record) == -trx_id_) {
      return RC::SUCCESS;
    }
    // 对于其它事务
    else {
      // 只读事务，不可见
      if (readonly) return RC::RECORD_INVISIBLE;
      // 写事务，冲突
      else
        return RC::LOCKED_CONCURRENCY_CONFLICT;
    }
  }

  // 删除：如果end_xid为负数，表示未提交
  if (end_xid_field.get_int(record) < 0) {
    // 对自己不可见，（逻辑删除）
    if (end_xid_field.get_int(record) == -trx_id_) {
      return RC::RECORD_INVISIBLE;
    }
    // 对其他事务
    else {
      // 只读事务，可见
      if (readonly) return RC::SUCCESS;
      // 写事务，冲突
      else
        return RC::LOCKED_CONCURRENCY_CONFLICT;
    }
  }

  // ***********************已提交的事务*******************

  if (begin_xid_field.get_int(record) > 0 &&
      end_xid_field.get_int(record) > 0) {
    // 判断是否可见
    if (trx_id_ >= begin_xid_field.get_int(record) &&
        trx_id_ < end_xid_field.get_int(record)) {
      return RC::SUCCESS;
    }
    // 不在范围内，不可见
    else {
      return RC::RECORD_INVISIBLE;
    }
  }

  return rc;
}

RC MvccTrx::start_if_need() {
  if (!started_) {
    ASSERT(operations_.empty(),
           "try to start a new trx while operations is not empty");
    trx_id_ = trx_kit_.next_trx_id();
    LOG_DEBUG("current thread change to new trx with %d", trx_id_);
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit() {
  int32_t commit_xid = trx_kit_.next_trx_id();
  RC rc = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &begin_xid_field,
                               commit_xid](Record &record) {
          LOG_WARN(
              "before commit insert record. trx id=%d, begin xid=%d, commit "
              "xid=%d, lbt=%s",
              trx_id_, begin_xid_field.get_int(record), commit_xid, lbt());
          ASSERT(begin_xid_field.get_int(record) == -this->trx_id_,
                 "got an invalid record while committing. begin xid=%d, this "
                 "trx id=%d",
                 begin_xid_field.get_int(record), trx_id_);
          begin_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false /*readonly*/,
                                             record_updater);
        ASSERT(rc == RC::SUCCESS,
               "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field,
                               commit_xid](Record &record) {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_,
                 "got an invalid record while committing. end xid=%d, this trx "
                 "id=%d",
                 end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, commit_xid);
        };
        rc = operation.table()->visit_record(rid, false /*readonly*/,
                                             record_updater);
        ASSERT(rc == RC::SUCCESS,
               "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d",
               static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();
  return rc;
}

RC MvccTrx::rollback() {
  RC rc = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID rid(operation.page_num(), operation.slot_num());
        Record record;
        Table *table = operation.table();
        rc = table->get_record(rid, record);
        ASSERT(rc == RC::SUCCESS,
               "failed to get record while rollback. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
        rc = table->delete_record(record);
        ASSERT(rc == RC::SUCCESS,
               "failed to delete record while rollback. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID rid(operation.page_num(), operation.slot_num());
        ASSERT(rc == RC::SUCCESS,
               "failed to get record while rollback. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &end_xid_field](Record &record) {
          ASSERT(end_xid_field.get_int(record) == -trx_id_,
                 "got an invalid record while rollback. end xid=%d, this trx "
                 "id=%d",
                 end_xid_field.get_int(record), trx_id_);
          end_xid_field.set_int(record, trx_kit_.max_trx_id());
        };
        rc = table->visit_record(rid, false /*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS,
               "failed to get record while committing. rid=%s, rc=%s",
               rid.to_string().c_str(), strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d",
               static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();
  return rc;
}

/**
 * @brief 获取指定表上的与版本号相关的字段
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field,
                         Field &end_xid_field) const {
  const TableMeta &table_meta = table->table_meta();
  const std::pair<const FieldMeta *, int> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.second >= 2, "invalid trx fields number. %d",
         trx_fields.second);

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields.first[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields.first[1]);
}
