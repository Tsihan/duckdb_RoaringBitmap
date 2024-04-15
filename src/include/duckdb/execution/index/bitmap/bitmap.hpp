//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/bitmap/bitmap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/index.hpp"

#include "duckdb/common/array.hpp"

namespace duckdb {

// classes
enum class VerifyExistenceType : uint8_t {
	APPEND = 0,    // appends to a table
	APPEND_FK = 1, // appends to a table that has a foreign key
	DELETE_FK = 2  // delete from a table that has a foreign key
};
class ConflictManager;
class BITMAPKey;
class FixedSizeAllocator;

// structs
struct BITMAPIndexScanState;
struct BITMAPFlags {
	vector<bool> vacuum_flags;
	vector<idx_t> merge_buffer_counts;
};

class BITMAP : public Index {
public:
	// Index type name for the BITMAP
	static constexpr const char *TYPE_NAME = "BITMAP";
	//! FixedSizeAllocator count of the BITMAP
	static constexpr uint8_t ALLOCATOR_COUNT = 6;

public:
	//! Constructs an BITMAP
	BITMAP(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
	    TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	    AttachedDatabase &db,
	    const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr = nullptr,
	    const IndexStorageInfo &info = IndexStorageInfo());

	//! Root of the tree
	Node tree = Node();
	//! Fixed-size allocators holding the BITMAP nodes
	shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> allocators;
	//! True, if the BITMAP owns its data
	bool owns_data;

	//! Try to initialize a scan on the index with the given expression and filter
	unique_ptr<IndexScanState> TryInitializeScan(const Transaction &transaction, const Expression &index_expr,
	                                             const Expression &filter_expr);

	//! Performs a lookup on the index, fetching up to max_count result IDs. Returns true if all row IDs were fetched,
	//! and false otherwise
	bool Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids);

public:
	//! Create a index instance of this type
	static unique_ptr<Index> Create(CreateIndexInput &input) {
		auto bitmap = make_uniq<BITMAP>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                          input.unbound_expressions, input.db, nullptr, input.storage_info);
		return std::move(bitmap);
	}

	//! Called when data is appended to the index. The lock obtained from InitializeLock must be held
	ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index without a constraint violation
	void VerifyAppend(DataChunk &chunk) override;
	//! Verify that data can be appended to the index without a constraint violation using the conflict manager
	void VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) override;
	//! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;
	//! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert a chunk of entries into the index
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	//! Construct an BITMAP from a vector of sorted keys
	bool ConstructFromSorted(idx_t count, vector<BITMAPKey> &keys, Vector &row_identifiers);

	//! Search equal values and fetches the row IDs
	bool SearchEqual(BITMAPKey &key, idx_t max_count, vector<row_t> &result_ids);
	//! Search equal values used for joins that do not need to fetch data
	void SearchEqualJoinNoFetch(BITMAPKey &key, idx_t &result_size);

	//! Returns all BITMAP storage information for serialization
	IndexStorageInfo GetStorageInfo(const bool get_buffers) override;

	//! Merge another index into this index. The lock obtained from InitializeLock must be held, and the other
	//! index must also be locked during the merge
	bool MergeIndexes(IndexLock &state, Index &other_index) override;

	//! Traverses an BITMAP and vacuums the qualifying nodes. The lock obtained from InitializeLock must be held
	void Vacuum(IndexLock &state) override;

	//! Returns the in-memory usage of the index. The lock obtained from InitializeLock must be held
	idx_t GetInMemorySize(IndexLock &index_lock) override;

	//! Generate BITMAP keys for an input chunk
	static void GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<BITMAPKey> &keys);

	//! Generate a string containing all the expressions and their respective values that violate a constraint
	string GenerateErrorKeyName(DataChunk &input, idx_t row);
	//! Generate the matching error message for a constraint violation
	string GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name);
	//! Performs constraint checking for a chunk of input data
	void CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) override;

	//! Returns the string representation of the BITMAP, or only traverses and verifies the index
	string VerifyAndToString(IndexLock &state, const bool only_verify) override;

	//! Find the node with a matching key, or return nullptr if not found
	optional_ptr<const Node> Lookup(const Node &node, const BITMAPKey &key, idx_t depth);


private:
	//! Insert a row ID into a leaf
	bool InsertToLeaf(Node &leaf, const row_t &row_id);
	//! Erase a key from the tree (if a leaf has more than one value) or erase the leaf itself
	void Erase(Node &node, const BITMAPKey &key, idx_t depth, const row_t &row_id);

	//! Returns all row IDs belonging to a key greater (or equal) than the search key
	bool SearchGreater(BITMAPIndexScanState &state, BITMAPKey &key, bool equal, idx_t max_count, vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key less (or equal) than the upper_bound
	bool SearchLess(BITMAPIndexScanState &state, BITMAPKey &upper_bound, bool equal, idx_t max_count,
	                vector<row_t> &result_ids);
	//! Returns all row IDs belonging to a key within the range of lower_bound and upper_bound
	bool SearchCloseRange(BITMAPIndexScanState &state, BITMAPKey &lower_bound, BITMAPKey &upper_bound, bool left_equal,
	                      bool right_equal, idx_t max_count, vector<row_t> &result_ids);

	//! Initializes a merge operation by returning a set containing the buffer count of each fixed-size allocator
	void InitializeMerge(BITMAPFlags &flags);

	//! Initializes a vacuum operation by calling the initialize operation of the respective
	//! node allocator, and returns a vector containing either true, if the allocator at
	//! the respective position qualifies, or false, if not
	void InitializeVacuum(BITMAPFlags &flags);
	//! Finalizes a vacuum operation by calling the finalize operation of all qualifying
	//! fixed size allocators
	void FinalizeVacuum(const BITMAPFlags &flags);

	//! Internal function to return the string representation of the BITMAP,
	//! or only traverses and verifies the index
	string VerifyAndToStringInternal(const bool only_verify);

	//! Initialize the allocators of the BITMAP
	void InitAllocators(const IndexStorageInfo &info);
	//! STABLE STORAGE NOTE: This is for old storage files, to deserialize the allocators of the BITMAP
	void Deserialize(const BlockPointer &pointer);
	//! Initializes the serialization of the index by combining the allocator data onto partial blocks
	void WritePartialBlocks();

	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) override;
};

} // namespace duckdb
