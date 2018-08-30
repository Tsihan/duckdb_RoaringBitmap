
#include "planner/logical_plan_generator.hpp"

#include "parser/expression/expression_list.hpp"
#include "parser/statement/copy_statement.hpp"
#include "parser/statement/insert_statement.hpp"
#include "parser/tableref/tableref_list.hpp"

#include "planner/operator/logical_list.hpp"

#include <map>

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::Visit(SelectStatement &statement) {
	for (auto &expr : statement.select_list) {
		expr->Accept(this);
	}

	if (statement.from_table) {
		// SELECT with FROM
		statement.from_table->Accept(this);
	} else {
		// SELECT without FROM, add empty GET
		root = make_unique<LogicalGet>();
	}

	if (statement.where_clause) {
		statement.where_clause->Accept(this);

		auto filter = make_unique<LogicalFilter>(move(statement.where_clause));
		filter->AddChild(move(root));
		root = move(filter);
	}

	if (statement.HasAggregation()) {
		auto aggregate =
		    make_unique<LogicalAggregate>(move(statement.select_list));
		if (statement.HasGroup()) {
			// have to add group by columns
			aggregate->groups = move(statement.groupby.groups);
		}
		aggregate->AddChild(move(root));
		root = move(aggregate);

		if (statement.HasHaving()) {
			statement.groupby.having->Accept(this);

			auto having =
			    make_unique<LogicalFilter>(move(statement.groupby.having));
			having->AddChild(move(root));
			root = move(having);
		}
	} else {
		auto projection =
		    make_unique<LogicalProjection>(move(statement.select_list));
		projection->AddChild(move(root));
		root = move(projection);
	}

	if (statement.select_distinct) {
		if (root->type != LogicalOperatorType::PROJECTION &&
		    root->type != LogicalOperatorType::AGGREGATE_AND_GROUP_BY) {
			throw Exception("DISTINCT can only apply to projection or group");
		}

		vector<unique_ptr<AbstractExpression>> expressions;
		vector<unique_ptr<AbstractExpression>> groups;

		for (size_t i = 0; i < root->expressions.size(); i++) {
			AbstractExpression *proj_ele = root->expressions[i].get();
			auto group_ref =
			    make_unique_base<AbstractExpression, GroupRefExpression>(
			        proj_ele->return_type, i);
			group_ref->alias = proj_ele->alias;
			expressions.push_back(move(group_ref));
			groups.push_back(
			    move(make_unique_base<AbstractExpression, ColumnRefExpression>(
			        proj_ele->return_type, i)));
		}
		// this aggregate is superflous if all grouping columns are in aggr
		// below
		auto aggregate = make_unique<LogicalAggregate>(move(expressions));
		aggregate->groups = move(groups);
		aggregate->AddChild(move(root));
		root = move(aggregate);
	}

	if (statement.HasOrder()) {
		auto order = make_unique<LogicalOrder>(move(statement.orderby));
		order->AddChild(move(root));
		root = move(order);
	}
	if (statement.HasLimit()) {
		auto limit = make_unique<LogicalLimit>(statement.limit.limit,
		                                       statement.limit.offset);
		limit->AddChild(move(root));
		root = move(limit);
	}

	if (statement.union_select) {
		auto top_select = move(root);
		statement.union_select->Accept(this);
		// TODO: LIMIT/ORDER BY with UNION? How does that work?
		auto bottom_select = move(root);
		auto union_op =
		    make_unique<LogicalUnion>(move(top_select), move(bottom_select));
		root = move(union_op);
	}
}

static void cast_children_to_equal_types(AbstractExpression &expr) {
	// first figure out the widest type
	TypeId max_type = TypeId::INVALID;
	for (size_t child_idx = 0; child_idx < expr.children.size(); child_idx++) {
		TypeId child_type = expr.children[child_idx]->return_type;
		if (child_type > max_type) {
			max_type = child_type;
		}
	}
	// now add casts where appropriate
	for (size_t child_idx = 0; child_idx < expr.children.size(); child_idx++) {
		TypeId child_type = expr.children[child_idx]->return_type;
		if (child_type != max_type) {
			auto cast = make_unique<CastExpression>(
			    max_type, move(expr.children[child_idx]));
			expr.children[child_idx] = move(cast);
		}
	}
}

void LogicalPlanGenerator::Visit(AggregateExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	// add cast if types don't match
	for (size_t i = 0; i < expr.children.size(); i++) {
		auto &child = expr.children[i];
		if (child->return_type != expr.return_type) {
			auto cast = make_unique<CastExpression>(expr.return_type,
			                                        move(expr.children[i]));
			expr.children[i] = move(cast);
		}
	}
}

void LogicalPlanGenerator::Visit(ComparisonExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	cast_children_to_equal_types(expr);
}

// TODO: this is ugly, generify functionality
void LogicalPlanGenerator::Visit(ConjunctionExpression &expr) {
	SQLNodeVisitor::Visit(expr);

	if (expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	}
	if (expr.children[1]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[1]));
		expr.children[1] = move(cast);
	}
}

void LogicalPlanGenerator::Visit(OperatorExpression &expr) {
	SQLNodeVisitor::Visit(expr);
	if (expr.type == ExpressionType::OPERATOR_NOT &&
	    expr.children[0]->return_type != TypeId::BOOLEAN) {
		auto cast = make_unique<CastExpression>(TypeId::BOOLEAN,
		                                        move(expr.children[0]));
		expr.children[0] = move(cast);
	} else {
		cast_children_to_equal_types(expr);
	}
}

void LogicalPlanGenerator::Visit(SubqueryExpression &expr) {
	LogicalPlanGenerator generator(context, *expr.context);
	expr.subquery->Accept(&generator);
	if (!generator.root) {
		throw Exception("Can't plan subquery");
	}
	expr.op = move(generator.root);
}

void LogicalPlanGenerator::Visit(BaseTableRef &expr) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(),
	                                         expr.schema_name, expr.table_name);
	auto alias = expr.alias.empty() ? expr.table_name : expr.alias;

	auto index = bind_context.GetTableIndex(alias);

	std::vector<size_t> column_ids;
	// look in the context for this table which columns are required
	for (auto &bound_column : bind_context.bound_columns[alias]) {
		column_ids.push_back(table->name_map[bound_column]);
	}
	if (column_ids.size() == 0) {
		// no column ids selected
		// the query is like SELECT COUNT(*) FROM table, or SELECT 42 FROM table
		// return just the first column
		column_ids.push_back(0);
	}

	auto get_table = make_unique<LogicalGet>(table, alias, index, column_ids);
	if (root)
		get_table->AddChild(move(root));
	root = move(get_table);
}

void LogicalPlanGenerator::Visit(CrossProductRef &expr) {
	auto cross_product = make_unique<LogicalCrossProduct>();

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	expr.left->Accept(this);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	expr.right->Accept(this);
	assert(root);
	cross_product->AddChild(move(root));
	root = nullptr;

	root = move(cross_product);
}

void LogicalPlanGenerator::Visit(JoinRef &expr) {
	auto join = make_unique<LogicalJoin>(expr.type);

	if (root) {
		throw Exception("Cross product cannot have children!");
	}

	expr.left->Accept(this);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	expr.right->Accept(this);
	assert(root);
	join->AddChild(move(root));
	root = nullptr;

	join->SetJoinCondition(move(expr.condition));

	root = move(join);
}

void LogicalPlanGenerator::Visit(SubqueryRef &expr) {
	throw NotImplementedException("Joins not implemented yet!");
}

void LogicalPlanGenerator::Visit(InsertStatement &statement) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(),
	                                         statement.schema, statement.table);
	std::vector<std::unique_ptr<AbstractExpression>> insert_val_list;

	if (statement.columns.size() == 0) {
		if (statement.values.size() != table->columns.size()) {
			throw Exception("Not enough values for insert");
		}
		for (size_t i = 0; i < statement.values.size(); i++) {
			insert_val_list.push_back(move(statement.values[i]));
		}
	} else {
		if (statement.values.size() != statement.columns.size()) {
			throw Exception("Column name/value mismatch");
		}
		map<std::string, unique_ptr<AbstractExpression>> insert_vals;
		for (size_t i = 0; i < statement.values.size(); i++) {
			insert_vals[statement.columns[i]] = move(statement.values[i]);
		}
		for (auto &col : table->columns) {
			if (insert_vals.count(col->name)) { // column value was specified
				insert_val_list.push_back(move(insert_vals[col->name]));
			} else {
				insert_val_list.push_back(std::unique_ptr<AbstractExpression>(
				    new ConstantExpression(col->default_value)));
			}
		}
	}

	auto insert = make_unique<LogicalInsert>(table, move(insert_val_list));
	root = move(insert);
}

void LogicalPlanGenerator::Visit(CopyStatement &statement) {
	if (statement.table[0] != '\0') {
		auto table = context.db.catalog.GetTable(
		    context.ActiveTransaction(), statement.schema, statement.table);
		auto copy = make_unique<LogicalCopy>(
		    table, move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape), move(statement.select_list));
		root = move(copy);
	} else {
		auto copy = make_unique<LogicalCopy>(
		    move(statement.file_path), move(statement.is_from),
		    move(statement.delimiter), move(statement.quote),
		    move(statement.escape));
		statement.select_stmt->Accept(this);
		copy->AddChild(move(root));
		root = move(copy);
	}
}
