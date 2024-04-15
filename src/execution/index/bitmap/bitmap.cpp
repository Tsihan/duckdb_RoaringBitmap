#include "duckdb/execution/index/bitmap/bitmap.hpp"

#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"

namespace duckdb {

struct BITMAPIndexScanState : public IndexScanState {

	//! Scan predicates (single predicate scan or range scan)
	Value values[2];
	//! Expressions of the scan predicates
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs
	vector<row_t> result_ids;
	//Iterator iterator;
};

//===--------------------------------------------------------------------===//
// BITMAP
//===--------------------------------------------------------------------===//

BITMAP::BITMAP(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
         TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
         AttachedDatabase &db, const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr,
         const IndexStorageInfo &info)
    : Index(name, BITMAP::TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db),
      allocators(allocators_ptr), owns_data(false) {


	// deserialize lazily
	if (info.IsValid()) {

		if (!info.root_block_ptr.IsValid()) {
			InitAllocators(info);

		} else {
			// old storage file
			Deserialize(info.root_block_ptr);
		}
	}

	// validate the types of the key columns
	for (idx_t i = 0; i < types.size(); i++) {
		switch (types[i]) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::UINT128:
		case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for roaring bitmap index key.");
		}
	}
}

//===--------------------------------------------------------------------===//
// Initialize Predicate Scans
//===--------------------------------------------------------------------===//

//! Initialize a single predicate scan on the index with the given expression and column IDs
static unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                                const ExpressionType expression_type) {
	// initialize point lookup
	auto result = make_uniq<BITMAPIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

//! Initialize a two predicate scan on the index with the given expression and column IDs
static unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
                                                              const ExpressionType low_expression_type,
                                                              const Value &high_value,
                                                              const ExpressionType high_expression_type) {
	// initialize range lookup
	auto result = make_uniq<BITMAPIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> BITMAP::TryInitializeScan(const Transaction &transaction, const Expression &index_expr,
                                                  const Expression &filter_expr) {

	Value low_value, high_value, equal_value;
	ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;
	// try to find a matching index for any of the filter expressions

	// create a matcher for a comparison with a constant
	ComparisonExpressionMatcher matcher;
	// match on a comparison type
	matcher.expr_type = make_uniq<ComparisonExpressionTypeMatcher>();
	// match on a constant comparison with the indexed expression
	matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(const_cast<Expression &>(index_expr)));
	matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());

	matcher.policy = SetMatcher::Policy::UNORDERED;

	vector<reference<Expression>> bindings;
	if (matcher.Match(const_cast<Expression &>(filter_expr), bindings)) {
		// range or equality comparison with constant value
		// we can use our index here
		// bindings[0] = the expression
		// bindings[1] = the index expression
		// bindings[2] = the constant
		auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
		auto constant_value = bindings[2].get().Cast<BoundConstantExpression>().value;
		auto comparison_type = comparison.type;
		if (comparison.left->type == ExpressionType::VALUE_CONSTANT) {
			// the expression is on the right side, we flip them around
			comparison_type = FlipComparisonExpression(comparison_type);
		}
		if (comparison_type == ExpressionType::COMPARE_EQUAL) {
			// equality value
			// equality overrides any other bounds so we just break here
			equal_value = constant_value;
		} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
			// greater than means this is a lower bound
			low_value = constant_value;
			low_comparison_type = comparison_type;
		} else {
			// smaller than means this is an upper bound
			high_value = constant_value;
			high_comparison_type = comparison_type;
		}
	} else if (filter_expr.type == ExpressionType::COMPARE_BETWEEN) {
		// BETWEEN expression
		auto &between = filter_expr.Cast<BoundBetweenExpression>();
		if (!between.input->Equals(index_expr)) {
			// expression doesn't match the index expression
			return nullptr;
		}
		if (between.lower->type != ExpressionType::VALUE_CONSTANT ||
		    between.upper->type != ExpressionType::VALUE_CONSTANT) {
			// not a constant comparison
			return nullptr;
		}
		low_value = (between.lower->Cast<BoundConstantExpression>()).value;
		low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
		                                              : ExpressionType::COMPARE_GREATERTHAN;
		high_value = (between.upper->Cast<BoundConstantExpression>()).value;
		high_comparison_type =
		    between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}

	if (!equal_value.IsNull() || !low_value.IsNull() || !high_value.IsNull()) {
		// we can scan this index using this predicate: try a scan
		unique_ptr<IndexScanState> index_state;
		if (!equal_value.IsNull()) {
			// equality predicate
			index_state = InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
		} else if (!low_value.IsNull() && !high_value.IsNull()) {
			// two-sided predicate
			index_state = InitializeScanTwoPredicates(transaction, low_value, low_comparison_type, high_value,
			                                          high_comparison_type);
		} else if (!low_value.IsNull()) {
			// less than predicate
			index_state = InitializeScanSinglePredicate(transaction, low_value, low_comparison_type);
		} else {
			D_ASSERT(!high_value.IsNull());
			index_state = InitializeScanSinglePredicate(transaction, high_value, high_comparison_type);
		}
		return index_state;
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Keys
//===--------------------------------------------------------------------===//

template <class T>
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<BITMAPKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	D_ASSERT(keys.size() >= count);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			BITMAPKey::CreateBITMAPKey<T>(allocator, input.GetType(), keys[i], input_data[idx]);
		} else {
			// we need to possibly reset the former key value in the keys vector
			keys[i] = BITMAPKey();
		}
	}
}

template <class T>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<BITMAPKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		// key is not NULL (no previous column entry was NULL)
		if (!keys[i].Empty()) {
			if (!idata.validity.RowIsValid(idx)) {
				// this column entry is NULL, set whole key to NULL
				keys[i] = BITMAPKey();
			} else {
				auto other_key = BITMAPKey::CreateBITMAPKey<T>(allocator, input.GetType(), input_data[idx]);
				keys[i].ConcatenateBITMAPKey(allocator, other_key);
			}
		}
	}
}

void BITMAP::GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<BITMAPKey> &keys) {
	// generate keys for the first input column
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT128:
		TemplatedGenerateKeys<uhugeint_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t>(allocator, input.data[0], input.size(), keys);
		break;
	default:
		throw InternalException("Invalid type for  roaring bitmap index");
	}

	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		// for each of the remaining columns, concatenate
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT128:
			ConcatenateKeys<uhugeint_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t>(allocator, input.data[i], input.size(), keys);
			break;
		default:
			throw InternalException("Invalid type for  roaring bitmap index");
		}
	}
}




	

bool BITMAP::ConstructFromSorted(idx_t count, vector<BITMAPKey> &keys, Vector &row_identifiers) {

	// prepare the row_identifiers
	row_identifiers.Flatten(count);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	//TODO Qihan: pay attention to the following code
    




	return true;
}

//===--------------------------------------------------------------------===//
// Insert / Verification / Constraint Checking
//===--------------------------------------------------------------------===//
ErrorData BITMAP::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {

	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<BITMAPKey> keys(input.size());
	GenerateKeys(arena_allocator, input, keys);

	// get the corresponding row IDs
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	// now insert the elements into the index
	idx_t failed_index = DConstants::INVALID_INDEX;
	// for (idx_t i = 0; i < input.size(); i++) {
	// 	if (keys[i].Empty()) {
	// 		continue;
	// 	}

	// 	row_t row_id = row_identifiers[i];
	// 	if (!Insert(tree, keys[i], 0, row_id)) {
	// 		// failed to insert because of constraint violation
	// 		failed_index = i;
	// 		break;
	// 	}
	// }

	// failed to insert because of constraint violation: remove previously inserted entries
	// if (failed_index != DConstants::INVALID_INDEX) {
	// 	for (idx_t i = 0; i < failed_index; i++) {
	// 		if (keys[i].Empty()) {
	// 			continue;
	// 		}
	// 		row_t row_id = row_identifiers[i];
	// 		Erase(tree, keys[i], 0, row_id);
	// 	}
	// }

	if (failed_index != DConstants::INVALID_INDEX) {
		return ErrorData(ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicate key \"%s\"",
		                                     AppendRowError(input, failed_index)));
	}

#ifdef DEBUG
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
	}
#endif

	return ErrorData();
}

ErrorData BITMAP::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void BITMAP::VerifyAppend(DataChunk &chunk) {
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, chunk.size());
	CheckConstraintsForChunk(chunk, conflict_manager);
}

void BITMAP::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	D_ASSERT(conflict_manager.LookupType() == VerifyExistenceType::APPEND);
	CheckConstraintsForChunk(chunk, conflict_manager);
}







//===--------------------------------------------------------------------===//
// Drop and Delete
//===--------------------------------------------------------------------===//

void BITMAP::CommitDrop(IndexLock &index_lock) {
	for (auto &allocator : *allocators) {
		allocator->Reset();
	}
	tree.Clear();
}

void BITMAP::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {

	DataChunk expression;
	expression.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions
	ExecuteExpressions(input, expression);

	// then generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<BITMAPKey> keys(expression.size());
	GenerateKeys(arena_allocator, expression, keys);

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	// for (idx_t i = 0; i < input.size(); i++) {
	// 	if (keys[i].Empty()) {
	// 		continue;
	// 	}
	// 	Erase(tree, keys[i], 0, row_identifiers[i]);
	// }

#ifdef DEBUG
	// verify that we removed all row IDs
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		if (leaf) {
			D_ASSERT(!Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
		}
	}
#endif
}

void BITMAP::Erase(Node &node, const BITMAPKey &key, idx_t depth, const row_t &row_id) {

	if (!node.HasMetadata()) {
		return;
	}




		return;
	}



bool BITMAP::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
               vector<row_t> &result_ids) {

	auto &scan_state = state.Cast<BITMAPIndexScanState>();
	vector<row_t> row_ids;
	bool success;

	// FIXME: the key directly owning the data for a single key might be more efficient
	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	// auto key = CreateKey(arena_allocator, types[0], scan_state.values[0]);

	// if (scan_state.values[1].IsNull()) {

	// 	// single predicate
	// 	lock_guard<mutex> l(lock);
	// 	switch (scan_state.expressions[0]) {
	// 	case ExpressionType::COMPARE_EQUAL:
	// 		success = SearchEqual(key, max_count, row_ids);
	// 		break;
	// 	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	// 		success = SearchGreater(scan_state, key, true, max_count, row_ids);
	// 		break;
	// 	case ExpressionType::COMPARE_GREATERTHAN:
	// 		success = SearchGreater(scan_state, key, false, max_count, row_ids);
	// 		break;
	// 	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	// 		success = SearchLess(scan_state, key, true, max_count, row_ids);
	// 		break;
	// 	case ExpressionType::COMPARE_LESSTHAN:
	// 		success = SearchLess(scan_state, key, false, max_count, row_ids);
	// 		break;
	// 	default:
	// 		throw InternalException("Index scan type not implemented");
	// 	}

	// } else {

	// 	// two predicates
	// 	lock_guard<mutex> l(lock);

	// 	D_ASSERT(scan_state.values[1].type().InternalType() == types[0]);
	// 	auto upper_bound = CreateKey(arena_allocator, types[0], scan_state.values[1]);

	// 	bool left_equal = scan_state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
	// 	bool right_equal = scan_state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
	// 	success = SearchCloseRange(scan_state, key, upper_bound, left_equal, right_equal, max_count, row_ids);
	// }

	if (!success) {
		return false;
	}
	if (row_ids.empty()) {
		return true;
	}

	// sort the row ids
	sort(row_ids.begin(), row_ids.end());
	// duplicate eliminate the row ids and append them to the row ids of the state
	result_ids.reserve(row_ids.size());

	result_ids.push_back(row_ids[0]);
	for (idx_t i = 1; i < row_ids.size(); i++) {
		if (row_ids[i] != row_ids[i - 1]) {
			result_ids.push_back(row_ids[i]);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// More Verification / Constraint Checking
//===--------------------------------------------------------------------===//

string BITMAP::GenerateErrorKeyName(DataChunk &input, idx_t row) {

	// FIXME: why exactly can we not pass the expression_chunk as an argument to this
	// FIXME: function instead of re-executing?
	// re-executing the expressions is not very fast, but we're going to throw, so we don't care
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	string key_name;
	for (idx_t k = 0; k < expression_chunk.ColumnCount(); k++) {
		if (k > 0) {
			key_name += ", ";
		}
		key_name += unbound_expressions[k]->GetName() + ": " + expression_chunk.data[k].GetValue(row).ToString();
	}
	return key_name;
}

string BITMAP::GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name) {
	switch (verify_type) {
	case VerifyExistenceType::APPEND: {
		// APPEND to PK/UNIQUE table, but node/key already exists in PK/UNIQUE table
		string type = IsPrimary() ? "primary key" : "unique";
		return StringUtil::Format("Duplicate key \"%s\" violates %s constraint. "
		                          "If this is an unexpected constraint violation please double "
		                          "check with the known index limitations section in our documentation "
		                          "(https://duckdb.org/docs/sql/indexes).",
		                          key_name, type);
	}
	case VerifyExistenceType::APPEND_FK: {
		// APPEND_FK to FK table, node/key does not exist in PK/UNIQUE table
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" does not exist in the referenced table", key_name);
	}
	case VerifyExistenceType::DELETE_FK: {
		// DELETE_FK that still exists in a FK table, i.e., not a valid delete
		return StringUtil::Format("Violates foreign key constraint because key \"%s\" is still referenced by a foreign "
		                          "key in a different table",
		                          key_name);
	}
	default:
		throw NotImplementedException("Type not implemented for VerifyExistenceType");
	}
}

void BITMAP::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {

	// don't alter the index during constraint checking
	lock_guard<mutex> l(lock);

	// first resolve the expressions for the index
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<BITMAPKey> keys(expression_chunk.size());
	GenerateKeys(arena_allocator, expression_chunk, keys);

	idx_t found_conflict = DConstants::INVALID_INDEX;
	for (idx_t i = 0; found_conflict == DConstants::INVALID_INDEX && i < input.size(); i++) {

		// if (keys[i].Empty()) {
		// 	if (conflict_manager.AddNull(i)) {
		// 		found_conflict = i;
		// 	}
		// 	continue;
		// }

		auto leaf = Lookup(tree, keys[i], 0);
		if (!leaf) {
			if (conflict_manager.AddMiss(i)) {
				found_conflict = i;
			}
			continue;
		}

		// when we find a node, we need to update the 'matches' and 'row_ids'
		// NOTE: leaves can have more than one row_id, but for UNIQUE/PRIMARY KEY they will only have one
		D_ASSERT(leaf->GetType() == NType::LEAF_INLINED);
		if (conflict_manager.AddHit(i, leaf->GetRowId())) {
			found_conflict = i;
		}
	}

	conflict_manager.FinishLookup();

	if (found_conflict == DConstants::INVALID_INDEX) {
		return;
	}

	auto key_name = GenerateErrorKeyName(input, found_conflict);
	auto exception_msg = GenerateConstraintErrorMessage(conflict_manager.LookupType(), key_name);
	throw ConstraintException(exception_msg);
}

//===--------------------------------------------------------------------===//
// Helper functions for (de)serialization
//===--------------------------------------------------------------------===//

IndexStorageInfo BITMAP::GetStorageInfo(const bool get_buffers) {

	// set the name and root node
	IndexStorageInfo info;
	info.name = name;
	info.root = tree.Get();

	if (!get_buffers) {
		// store the data on disk as partial blocks and set the block ids
		WritePartialBlocks();

	} else {
		// set the correct allocation sizes and get the map containing all buffers
		for (const auto &allocator : *allocators) {
			info.buffers.push_back(allocator->InitSerializationToWAL());
		}
	}

	for (const auto &allocator : *allocators) {
		info.allocator_infos.push_back(allocator->GetInfo());
	}

	return info;
}

void BITMAP::WritePartialBlocks() {

	// use the partial block manager to serialize all allocator data
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(block_manager, CheckpointType::FULL_CHECKPOINT);

	for (auto &allocator : *allocators) {
		allocator->SerializeBuffers(partial_block_manager);
	}
	partial_block_manager.FlushPartialBlocks();
}

void BITMAP::InitAllocators(const IndexStorageInfo &info) {

	// set the root node
	tree.Set(info.root);

	// initialize the allocators
	D_ASSERT(info.allocator_infos.size() == ALLOCATOR_COUNT);
	for (idx_t i = 0; i < info.allocator_infos.size(); i++) {
		(*allocators)[i]->Init(info.allocator_infos[i]);
	}
}

void BITMAP::Deserialize(const BlockPointer &pointer) {

	D_ASSERT(pointer.IsValid());
	auto &metadata_manager = table_io_manager.GetMetadataManager();
	MetadataReader reader(metadata_manager, pointer);
	tree = reader.Read<Node>();

	for (idx_t i = 0; i < ALLOCATOR_COUNT; i++) {
		(*allocators)[i]->Deserialize(metadata_manager, reader.Read<BlockPointer>());
	}
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void BITMAP::InitializeVacuum(BITMAPFlags &flags) {

	flags.vacuum_flags.reserve(allocators->size());
	for (auto &allocator : *allocators) {
		flags.vacuum_flags.push_back(allocator->InitializeVacuum());
	}
}

void BITMAP::FinalizeVacuum(const BITMAPFlags &flags) {

	for (idx_t i = 0; i < allocators->size(); i++) {
		if (flags.vacuum_flags[i]) {
			(*allocators)[i]->FinalizeVacuum();
		}
	}
}

void BITMAP::Vacuum(IndexLock &state) {

	D_ASSERT(owns_data);

	if (!tree.HasMetadata()) {
		for (auto &allocator : *allocators) {
			allocator->Reset();
		}
		return;
	}

	// holds true, if an allocator needs a vacuum, and false otherwise
	BITMAPFlags flags;
	InitializeVacuum(flags);

	// skip vacuum if no allocators require it
	auto perform_vacuum = false;
	for (const auto &vacuum_flag : flags.vacuum_flags) {
		if (vacuum_flag) {
			perform_vacuum = true;
			break;
		}
	}
	if (!perform_vacuum) {
		return;
	}

	// traverse the allocated memory of the tree to perform a vacuum
	// tree.Vacuum(*this, flags);

	// finalize the vacuum operation
	FinalizeVacuum(flags);
}

//===--------------------------------------------------------------------===//
// Size
//===--------------------------------------------------------------------===//

idx_t BITMAP::GetInMemorySize(IndexLock &index_lock) {

	D_ASSERT(owns_data);

	idx_t in_memory_size = 0;
	for (auto &allocator : *allocators) {
		in_memory_size += allocator->GetInMemorySize();
	}
	return in_memory_size;
}





//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string BITMAP::VerifyAndToString(IndexLock &state, const bool only_verify) {
	// FIXME: this can be improved by counting the allocations of each node type,
	// FIXME: and by asserting that each fixed-size allocator lists an equal number of
	// FIXME: allocations of that type
	return VerifyAndToStringInternal(only_verify);
}

string BITMAP::VerifyAndToStringInternal(const bool only_verify) {
	// if (tree.HasMetadata()) {
	// 	return "BITMAP: " + tree.VerifyAndToString(*this, only_verify);
	// }
	return "[empty]";
}

string BITMAP::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) {
	auto key_name = GenerateErrorKeyName(input, failed_index);
	auto exception_msg = GenerateConstraintErrorMessage(verify_type, key_name);
	return exception_msg;
}

constexpr const char *BITMAP::TYPE_NAME;

} // namespace duckdb
