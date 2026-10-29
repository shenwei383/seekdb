/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/timezone/ob_time_convert.h"
#include <limits.h>
#include <stdlib.h>
#include <cerrno>
#include <cstring>


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, has_correlated_expr_, file_csv_, file_csv_header_, file_csv_describe_, file_json_, file_parquet_);

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  csv_first_row_pending_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (MY_SPEC.file_csv_) {
    ret = open_csv();
  } else if (MY_SPEC.file_json_) {
    ret = open_json();
  } else if (MY_SPEC.file_parquet_) {
    ret = open_parquet();
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.file_csv_) {
      csv_row_.clear();
      if (FileCsvScan::Status::OK != csv_scan_.rewind()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (!MY_SPEC.file_csv_describe_ &&
          FileCsvScan::Status::OK != csv_scan_.next(csv_row_)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        csv_first_row_pending_ = !MY_SPEC.file_csv_describe_ && !MY_SPEC.file_csv_header_;
      }
    } else if (MY_SPEC.file_json_ && FileJsonScan::Status::OK != json_scan_.rewind()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (MY_SPEC.file_parquet_ && FileParquetScan::Status::OK != parquet_scan_.rewind()) {
      ret = OB_ERR_UNEXPECTED;
    }
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  csv_scan_.close();
  csv_row_.clear();
  csv_metadata_schema_ = FileCsvSchema{};
  json_scan_.close();
  json_schema_ = FileCsvSchema{};
  json_row_.clear();
  parquet_scan_.close();
  parquet_row_.clear();
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::open_csv()
{
  int ret = OB_SUCCESS;
  int64_t enabled = 0;
  ObString secure_root;
  ObDatum *path_datum = nullptr;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  char canonical[PATH_MAX + 1];
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (enabled == 0) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_SECURE_FILE_PRIV, secure_root))) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString path = path_datum->get_string();
    const std::string path_string(path.ptr(), path.length());
    if (nullptr == realpath(path_string.c_str(), canonical)) {
      ret = OB_FILE_NOT_EXIST;
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_root, ObString::make_string(canonical)))) {
    } else {
      const int fd = open_secure_csv_fd(
          std::string(secure_root.ptr(), secure_root.length()), canonical);
      if (fd < 0) {
        ret = errno == ENOSYS ? OB_NOT_SUPPORTED : OB_ERR_NO_PRIVILEGE;
      } else if (FileCsvScan::Status::OK != csv_scan_.open_fd(fd)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (MY_SPEC.file_csv_describe_) {
        if (MY_SPEC.column_exprs_.count() != 6 ||
            FileCsvScan::Status::OK != infer_csv_schema(csv_scan_, csv_metadata_schema_,
                                                        MY_SPEC.file_csv_header_)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv_metadata;
        }
      } else if (FileCsvScan::Status::OK != csv_scan_.next(csv_row_) ||
                 csv_row_.size() != static_cast<size_t>(MY_SPEC.column_exprs_.count())) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        csv_first_row_pending_ = !MY_SPEC.file_csv_header_;
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv;
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::open_json()
{
  int ret = OB_SUCCESS;
  int64_t enabled = 0;
  ObString secure_root;
  ObDatum *path_datum = nullptr;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  char canonical[PATH_MAX + 1];
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (enabled == 0) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_SECURE_FILE_PRIV, secure_root))) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString path = path_datum->get_string();
    const std::string path_string(path.ptr(), path.length());
    if (nullptr == realpath(path_string.c_str(), canonical)) {
      ret = OB_FILE_NOT_EXIST;
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_root,
                                                          ObString::make_string(canonical)))) {
    } else {
      const int fd = open_secure_csv_fd(
          std::string(secure_root.ptr(), secure_root.length()), canonical);
      if (fd < 0) {
        ret = errno == ENOSYS ? OB_NOT_SUPPORTED : OB_ERR_NO_PRIVILEGE;
      } else if (FileJsonScan::Status::OK != json_scan_.open_fd(fd)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (FileJsonScan::Status::OK != infer_json_schema(json_scan_, json_schema_) ||
                 FileJsonScan::Status::OK != json_scan_.rewind() ||
                 static_cast<size_t>(MY_SPEC.column_exprs_.count()) !=
                     (MY_SPEC.file_csv_describe_ ? 6 : json_schema_.names.size())) {
        ret = OB_INVALID_ARGUMENT;
      } else if (MY_SPEC.file_csv_describe_) {
        csv_metadata_schema_ = json_schema_;
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv_metadata;
      } else {
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_json;
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::open_parquet()
{
  int ret = OB_SUCCESS;
  int64_t enabled = 0;
  ObString secure_root;
  ObDatum *path_datum = nullptr;
  ObSQLSessionInfo *session = ctx_.get_my_session();
  char canonical[PATH_MAX + 1];
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enabled))) {
  } else if (enabled == 0) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_SECURE_FILE_PRIV, secure_root))) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString path = path_datum->get_string();
    const std::string path_string(path.ptr(), path.length());
    if (nullptr == realpath(path_string.c_str(), canonical)) {
      ret = OB_FILE_NOT_EXIST;
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_root,
                                                          ObString::make_string(canonical)))) {
    } else {
      const int fd = open_secure_csv_fd(
          std::string(secure_root.ptr(), secure_root.length()), canonical);
      if (fd < 0) {
        ret = errno == ENOSYS ? OB_NOT_SUPPORTED : OB_ERR_NO_PRIVILEGE;
      } else if (FileParquetScan::Status::OK != parquet_scan_.open_fd(fd) ||
                 static_cast<size_t>(MY_SPEC.column_exprs_.count()) !=
                     (MY_SPEC.file_csv_describe_ ? 6 : parquet_scan_.schema().names.size())) {
        ret = OB_INVALID_ARGUMENT;
      } else if (MY_SPEC.file_csv_describe_) {
        csv_metadata_schema_ = parquet_scan_.schema();
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_csv_metadata;
      } else {
        next_row_func_ = &ObFunctionTableOp::inner_get_next_row_parquet;
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_csv_metadata()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (node_idx_ >= static_cast<int64_t>(csv_metadata_schema_.names.size())) {
    ret = OB_ITER_END;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.column_exprs_.at(i);
    ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
    const char *value = nullptr;
    if (i == 0) {
      value = csv_metadata_schema_.names[node_idx_].c_str();
    } else if (i == 1) {
      switch (csv_metadata_schema_.kinds[node_idx_]) {
        case FileCsvSchema::Kind::BOOLEAN: value = "BOOLEAN"; break;
        case FileCsvSchema::Kind::BIGINT: value = "BIGINT"; break;
        case FileCsvSchema::Kind::DOUBLE: value = "DOUBLE"; break;
        case FileCsvSchema::Kind::DATE: value = "DATE"; break;
        case FileCsvSchema::Kind::DATETIME: value = "DATETIME"; break;
        default: value = "VARCHAR"; break;
      }
    } else if (i == 2) {
      value = csv_metadata_schema_.nullable[node_idx_] ? "YES" : "NO";
    }
    if (value == nullptr) {
      datum.set_null();
    } else {
      const size_t length = std::strlen(value);
      char *buffer = expr->get_str_res_mem(eval_ctx_, length);
      if (buffer == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        std::memcpy(buffer, value, length);
        datum.set_string(buffer, length);
      }
    }
    if (OB_SUCC(ret)) expr->set_evaluated_projected(eval_ctx_);
  }
  if (OB_SUCC(ret)) ++node_idx_;
  return ret;
}

int ObFunctionTableOp::write_file_value(ObExpr *expr, const std::string &field, bool is_null)
{
  int ret = OB_SUCCESS;
  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
  if (is_null) {
      datum.set_null();
  } else if (expr->datum_meta_.type_ == ObIntType) {
      char *end = nullptr;
      errno = 0;
      const int64_t value = std::strtoll(field.c_str(), &end, 10);
      if (errno != 0 || end != field.c_str() + field.size()) ret = OB_INVALID_ARGUMENT;
      else datum.set_int(value);
  } else if (expr->datum_meta_.type_ == ObDoubleType) {
      char *end = nullptr;
      errno = 0;
      const double value = std::strtod(field.c_str(), &end);
      if (errno != 0 || end != field.c_str() + field.size()) ret = OB_INVALID_ARGUMENT;
      else datum.set_double(value);
  } else if (expr->datum_meta_.type_ == ObTinyIntType) {
      if (field == "true") datum.set_int(1);
      else if (field == "false") datum.set_int(0);
      else ret = OB_INVALID_ARGUMENT;
  } else if (expr->datum_meta_.type_ == ObDateType) {
      int32_t value = 0;
      ObDateSqlMode mode;
      mode.allow_invalid_dates_ = false;
      const ObString text(static_cast<int32_t>(field.size()), field.data());
      if (OB_FAIL(ObTimeConverter::str_to_date(text, value, mode))) ret = OB_INVALID_ARGUMENT;
      else datum.set_date(value);
  } else if (expr->datum_meta_.type_ == ObDateTimeType) {
      std::string normalized = field;
      if (normalized.size() > 10 && normalized[10] == 'T') normalized[10] = ' ';
      const ObString text(static_cast<int32_t>(normalized.size()), normalized.data());
      ObDateSqlMode mode;
      mode.allow_invalid_dates_ = false;
      ObScale scale = 0;
      int64_t value = 0;
      ObTimeConvertCtx convert_ctx(nullptr, false);
      if (OB_FAIL(ObTimeConverter::str_to_datetime(text, convert_ctx, value, &scale, mode)))
        ret = OB_INVALID_ARGUMENT;
      else datum.set_datetime(value);
  } else if (field.empty()) {
      datum.set_string("", 0);
  } else {
      char *buffer = expr->get_str_res_mem(eval_ctx_, field.size());
      if (nullptr == buffer) ret = OB_ALLOCATE_MEMORY_FAILED;
      else { std::memcpy(buffer, field.data(), field.size()); datum.set_string(buffer, field.size()); }
  }
  if (OB_SUCC(ret)) expr->set_evaluated_projected(eval_ctx_);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_csv()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const FileCsvScan::Status status = csv_first_row_pending_
      ? FileCsvScan::Status::OK : csv_scan_.next(csv_row_);
  csv_first_row_pending_ = false;
  if (status == FileCsvScan::Status::END) {
    ret = OB_ITER_END;
  } else if (status != FileCsvScan::Status::OK ||
             csv_row_.size() != static_cast<size_t>(MY_SPEC.column_exprs_.count())) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    const std::string &field = csv_row_[i];
    ret = write_file_value(MY_SPEC.column_exprs_.at(i), field,
                           field.empty() || field == "NULL");
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_json()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const FileJsonScan::Status status = json_scan_.next(json_row_);
  if (status == FileJsonScan::Status::END) {
    ret = OB_ITER_END;
  } else if (status != FileJsonScan::Status::OK ||
             static_cast<size_t>(MY_SPEC.column_exprs_.count()) != json_schema_.names.size()) {
    ret = OB_INVALID_ARGUMENT;
  }
  const std::string empty;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    const std::string &name = json_schema_.names[i];
    const auto field = std::find_if(json_row_.begin(), json_row_.end(), [&name](const FileJsonField &item) {
      return item.name == name;
    });
    const bool is_null = field == json_row_.end() || field->cell.is_null;
    ret = write_file_value(MY_SPEC.column_exprs_.at(i),
                           is_null ? empty : field->cell.text, is_null);
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_parquet()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const FileParquetScan::Status status = parquet_scan_.next(parquet_row_);
  if (status == FileParquetScan::Status::END) {
    ret = OB_ITER_END;
  } else if (status != FileParquetScan::Status::OK ||
             static_cast<size_t>(MY_SPEC.column_exprs_.count()) != parquet_row_.size()) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
    ret = write_file_value(MY_SPEC.column_exprs_.at(i), parquet_row_[i].text,
                           parquet_row_[i].is_null);
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
