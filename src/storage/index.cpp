#include "duckdb/storage/index.hpp"

#include "duckdb/common/radix.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {

Index::Index(const string &name, const string &index_type, IndexConstraintType index_constraint_type,
             const vector<column_t> &column_ids, TableIOManager &table_io_manager,
             const vector<unique_ptr<Expression>> &unbound_expressions, AttachedDatabase &db)

    : name(name), index_type(index_type), index_constraint_type(index_constraint_type), column_ids(column_ids),
      table_io_manager(table_io_manager), db(db) {

	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("indexes are not supported on big endian architectures");
	}

	for (auto &expr : unbound_expressions) {
		types.push_back(expr->return_type.InternalType());
		logical_types.push_back(expr->return_type);
		auto unbound_expression = expr->Copy();
		bound_expressions.push_back(BindExpression(unbound_expression->Copy()));
		this->unbound_expressions.emplace_back(std::move(unbound_expression));
	}
	for (auto &bound_expr : bound_expressions) {
		executor.AddExpression(*bound_expr);
	}

	// create the column id set
	column_id_set.insert(column_ids.begin(), column_ids.end());
}

void Index::InitializeLock(IndexLock &state) {
	state.index_lock = unique_lock<mutex>(lock);
}
//Qihan Zhang, here is the entrance of update update type, and insert type
ErrorData Index::Append(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	return Append(state, entries, row_identifiers);
}
//Qihan Zhang here is the entrance of dropping an existing index
void Index::CommitDrop() {
	IndexLock index_lock;
	InitializeLock(index_lock);
	CommitDrop(index_lock);
}

void Index::Delete(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	Delete(state, entries, row_identifiers);
}

bool Index::MergeIndexes(Index &other_index) {
	IndexLock state;
	InitializeLock(state);
	return MergeIndexes(state, other_index);
}

string Index::VerifyAndToString(const bool only_verify) {
	IndexLock state;
	InitializeLock(state);
	return VerifyAndToString(state, only_verify);
}

void Index::Vacuum() {
	IndexLock state;
	InitializeLock(state);
	Vacuum(state);
}

idx_t Index::GetInMemorySize() {
	IndexLock state;
	InitializeLock(state);
	return GetInMemorySize(state);
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
		return make_uniq<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [this](unique_ptr<Expression> &expr) { expr = BindExpression(std::move(expr)); });
	return expr;
}

bool Index::IndexIsUpdated(const vector<PhysicalIndex> &column_ids_p) const {
	for (auto &column : column_ids_p) {
		if (column_id_set.find(column.index) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}

IndexStorageInfo Index::GetStorageInfo(const bool get_buffers) {
	throw NotImplementedException("The implementation of this index serialization does not exist.");
}

string Index::AppendRowError(DataChunk &input, idx_t index) {
	string error;
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (c > 0) {
			error += ", ";
		}
		error += input.GetValue(c, index).ToString();
	}
	return error;
}

} // namespace duckdb
