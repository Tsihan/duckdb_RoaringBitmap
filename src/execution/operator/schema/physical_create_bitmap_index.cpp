#include "duckdb/execution/operator/schema/physical_create_bitmap_index.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"

#include <roaring/roaring.hh>

namespace duckdb {

PhysicalCreateBITMAPIndex::PhysicalCreateBITMAPIndex(LogicalOperator &op, TableCatalogEntry &table_p,
                                               const vector<column_t> &column_ids, unique_ptr<CreateIndexInfo> info,
                                               vector<unique_ptr<Expression>> unbound_expressions,
                                               idx_t estimated_cardinality, const bool sorted)
    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()), info(std::move(info)), unbound_expressions(std::move(unbound_expressions)),
      sorted(sorted) {

	// convert virtual column ids to storage column ids
	for (auto &column_id : column_ids) {
		storage_ids.push_back(table.GetColumns().LogicalToPhysical(LogicalIndex(column_id)).index);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

class CreateBITMAPIndexGlobalSinkState : public GlobalSinkState {
public:
	//! Global index to be added to the table
	unique_ptr<Index> global_index;
};

class CreateBITMAPIndexLocalSinkState : public LocalSinkState {
public:
	explicit CreateBITMAPIndexLocalSinkState(ClientContext &context) : arena_allocator(Allocator::Get(context)) {};

	unique_ptr<Index> local_index;
	ArenaAllocator arena_allocator;

	DataChunk key_chunk;
	vector<column_t> key_column_ids;
};

unique_ptr<GlobalSinkState> PhysicalCreateBITMAPIndex::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateBITMAPIndexGlobalSinkState>();

	// create the global index
	auto &storage = table.GetStorage();
	// Qihan: todo
	return (std::move(state));
}

unique_ptr<LocalSinkState> PhysicalCreateBITMAPIndex::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CreateBITMAPIndexLocalSinkState>(context.client);

	// create the local index

	auto &storage = table.GetStorage();

	// Qihan: todo
	return std::move(state);
}

SinkResultType PhysicalCreateBITMAPIndex::SinkUnsorted(Vector &row_identifiers, OperatorSinkInput &input) const {

	auto &l_state = input.local_state.Cast<CreateBITMAPIndexLocalSinkState>();
	auto count = l_state.key_chunk.size();

	// get the corresponding row IDs
	row_identifiers.Flatten(count);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// insert the row IDs
	// Qihan: todo

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateBITMAPIndex::SinkSorted(Vector &row_identifiers, OperatorSinkInput &input) const {
	//Qihan: when creating a bitmap index, we don't need to sort the data

	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType PhysicalCreateBITMAPIndex::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {

	D_ASSERT(chunk.ColumnCount() >= 2);

	// generate the keys for the given input
	auto &l_state = input.local_state.Cast<CreateBITMAPIndexLocalSinkState>();
	l_state.key_chunk.ReferenceColumns(chunk, l_state.key_column_ids);
	l_state.arena_allocator.Reset();

	// insert the keys and their corresponding row IDs
	auto &row_identifiers = chunk.data[chunk.ColumnCount() - 1];
	if (sorted) {
		return SinkSorted(row_identifiers, input);
	}
	return SinkUnsorted(row_identifiers, input);
}

SinkCombineResultType PhysicalCreateBITMAPIndex::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
    // Qihan: In bitmap, you don't need to combine the local index to the global index

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateBITMAPIndex::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {

	// here, we set the resulting global index as the newly created index of the table
	auto &state = input.global_state.Cast<CreateBITMAPIndexGlobalSinkState>();

	// vacuum excess memory and verify
	state.global_index->Vacuum();
	D_ASSERT(!state.global_index->VerifyAndToString(true).empty());

	auto &storage = table.GetStorage();
	if (!storage.IsRoot()) {
		throw TransactionException("Transaction conflict: cannot add an index to a table that has been altered!");
	}

	auto &schema = table.schema;
	info->column_ids = storage_ids;
	auto index_entry = schema.CreateIndex(context, *info, table).get();
	if (!index_entry) {
		D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		// index already exists, but error ignored because of IF NOT EXISTS
		return SinkFinalizeType::READY;
	}
	auto &index = index_entry->Cast<DuckIndexEntry>();
	index.initial_index_size = state.global_index->GetInMemorySize();

	index.info = make_shared<IndexDataTableInfo>(storage.info, index.name);
	for (auto &parsed_expr : info->parsed_expressions) {
		index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// add index to storage
	storage.info->indexes.AddIndex(std::move(state.global_index));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalCreateBITMAPIndex::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

} // namespace duckdb
