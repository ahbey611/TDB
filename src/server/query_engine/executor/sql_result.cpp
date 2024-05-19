#include "include/query_engine/executor/sql_result.h"
#include "include/session/session.h"
#include "include/storage_engine/transaction/trx.h"
#include "common/log/log.h"
#include "include/query_engine/structor/tuple/tuple.h"

/**
 * The head of operator tree, it's the interface between diver and the operator tree.
 * Query result can be fetched from init(), next() methods in a volcano structure.
 * @param session
 */
SqlResult::SqlResult(Session *session) : session_(session)
{}

void SqlResult::set_tuple_schema(const TupleSchema &schema)
{
  tuple_schema_ = schema;
}

RC SqlResult::init()
{
  if (nullptr == operator_) {
    return RC::INVALID_ARGUMENT;
  }

  Trx *trx = session_->current_trx();
  trx->start_if_need();
  return operator_->open(trx);
}

RC SqlResult::close()
{
  if (nullptr == operator_) {
    return RC::INVALID_ARGUMENT;
  }
  RC rc = operator_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close operator. rc=%s", strrc(rc));
  }

  operator_.reset();

  if (session_ && !session_->is_trx_multi_operation_mode()) {
    if (rc == RC::SUCCESS) {
      rc = session_->current_trx()->commit();
    } else {
      RC rc2 = session_->current_trx()->rollback();
      if (rc2 != RC::SUCCESS) {
        LOG_PANIC("rollback failed. rc=%s", strrc(rc2));
      }
    }
  }
  return rc;
}

RC SqlResult::next_tuple(Tuple *&tuple)
{
  RC rc = operator_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  tuple = operator_->current_tuple();
  return rc;
}

void SqlResult::set_operator(std::unique_ptr<PhysicalOperator> oper)
{
  ASSERT(operator_ == nullptr, "current operator is not null. Result is not closed?");
  operator_ = std::move(oper);
}
