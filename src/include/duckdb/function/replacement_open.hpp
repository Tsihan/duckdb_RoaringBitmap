//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/replacement_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class DatabaseInstance;
struct DBConfig;

struct ReplacementOpenData {
	virtual ~ReplacementOpenData() {
	}
};

typedef unique_ptr<ReplacementOpenData> (*replacement_open_pre_t)(DBConfig &config);
typedef void (*replacement_open_post_t)(DatabaseInstance &instance, ReplacementOpenData *open_data);

struct ReplacementOpen {
	explicit ReplacementOpen(replacement_open_pre_t pre_func, replacement_open_post_t post_func)
	    : pre_func(pre_func), post_func(post_func), data(nullptr) {
	}

	replacement_open_pre_t pre_func;
	replacement_open_post_t post_func;

	unique_ptr<ReplacementOpenData> data;
};

} // namespace duckdb
