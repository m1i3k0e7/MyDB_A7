
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
#include <unordered_set>
#include <utility>
#include <functional>

struct PairHash {
	template <typename T1, typename T2>
	size_t operator()(const std::pair<T1, T2>& pair) const {
		size_t hash1 = std::hash<T1>()(pair.first);
		size_t hash2 = std::hash<T2>()(pair.second);
		return hash1 ^ (hash2 << 1); // Combine the hashes
	}
};

// Custom equality comparator for std::pair<std::string, std::shared_ptr<MyDB_AttType>>
struct PairEqual {
	template <typename T1, typename T2>
	bool operator()(const std::pair<T1, T2>& lhs, const std::pair<T1, T2>& rhs) const {
		return lhs.first == rhs.first && lhs.second == rhs.second;
	}
};

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery ::optimizeQueryPlan(map<string, MyDB_TablePtr> &allTables)
{
	// here we call the recursive, exhaustive enum. algorithm
	// return optimizeQueryPlan (...);
	MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
	cout << "Optimizing query plan...\n";
	print();
	
	map<string, MyDB_TablePtr> tables;
	for (auto &[table, alias] : tablesToProcess)
	{
		tables[alias] = allTables[table];
	}

	cout << "Tables: " << tables.size() << "\n";
	for (auto &entry : tables)
	{	
		cout << "Table: " << entry.first << "\n";
		for (auto &att : entry.second->getSchema()->getAtts())
		{
			totSchema->appendAtt(att);
		}
	}
	// cout << "Disjunctions: " << allDisjunctions.size() << "\n";

	return optimizeQueryPlan(tables, totSchema, allDisjunctions);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair<LogicalOpPtr, double> SFWQuery ::optimizeQueryPlan(map<string, MyDB_TablePtr> &allTables,
														MyDB_SchemaPtr totSchema, vector<ExprTreePtr> &allDisjunctions)
{

	LogicalOpPtr res = nullptr;
	double cost = 9e99, best = 9e99;

	// case where no joins
	if (allTables.size() == 1)
	{
		cout<<"Where table size is 1"<<endl;
		auto alias = allTables.begin()->first;
		auto table = allTables.begin()->second->alias(alias);

		res = make_shared<LogicalTableScan>(table, table, make_shared<MyDB_Stats>(table), allDisjunctions);
		MyDB_StatsPtr stats = res->getStats()->costSelection(allDisjunctions);
		best = stats->getTupleCount();
		return make_pair(res, best);
	}
	
	// multiple tables case
	cout<<"Where table size is more than 1"<<endl;
	vector<pair<string, MyDB_TablePtr>> tables;
    for (auto &entry : allTables) {
        tables.push_back(make_pair(entry.first, entry.second));
    }

	// find all ways to split the tables into two groups
	set<map<string, MyDB_TablePtr>> checked;
	for (int i = 1; i < (1 << tables.size()); i++)
	{
		map<string, MyDB_TablePtr> left, right;
		for (int j = 0; j < tables.size(); j++)
		{
			if (i & (1 << j))
			{
				left[tables[j].first] = tables[j].second;
			}
			else
			{
				right[tables[j].first] = tables[j].second;
			}
		}

		if (left.empty() || right.empty() || checked.count(right)) {
            continue;
        }

        checked.insert(left);

		// LeftCNF ← all clauses in C referring only to atts in Left
		// RightCNF ← all clauses in C referring only to atts in Right
		// TopCNF ← all clauses in C not in LeftCNF & not in RightCNF
		vector<ExprTreePtr> leftDisjunctions, rightDisjunctions, topDisjunctions;
		for (auto &disjunction : allDisjunctions)
		{
			bool refersToLeft = false, refersToRight = false;

			// Check if disjunction references any table in the left set
			for (const auto &[tableAlias, _] : left)
			{
				if (disjunction->referencesTable(tableAlias))
				{
					refersToLeft = true;
					break;
				}
			}

			// Check if disjunction references any table in the right set
			for (const auto &[tableAlias, _] : right)
			{
				if (disjunction->referencesTable(tableAlias))
				{
					refersToRight = true;
					break;
				}
			}

			if (refersToLeft && !refersToRight)
			{
				leftDisjunctions.push_back(disjunction);
			}
			else if (refersToRight && !refersToLeft)
			{
				rightDisjunctions.push_back(disjunction);
			}
			else
			{
				topDisjunctions.push_back(disjunction);
			}
		}

		// build A
		std::unordered_set<std::pair<std::string, MyDB_AttTypePtr>, PairHash, PairEqual> allAtts;
		for (auto s : totSchema->getAtts()) {
			allAtts.insert(s);
		}

		// LeftAtts ← Atts(Left) ∩ (A ∪ Atts(TopCNF))
		// RightAtts ← Atts(Right) ∩ (A ∪ Atts(TopCNF))
		MyDB_SchemaPtr leftSchema = make_shared<MyDB_Schema>(), rightSchema = make_shared<MyDB_Schema>();
        for (auto &entry : left) {
            auto alias = entry.first;
            auto table = entry.second;
            for (auto &att : table->getSchema()->getAtts()) {
                string attName = att.first;
                MyDB_AttTypePtr attType = att.second;
                if (allAtts.count(make_pair(attName, attType)) ||
                    any_of(topDisjunctions.begin(), topDisjunctions.end(),
                           [&alias, &attName](ExprTreePtr disjunction) {
                               return disjunction->referencesAtt(alias, attName);
                           })) {
                    leftSchema->appendAtt(make_pair(attName, attType));
                }
            }
        }

        for (auto &entry : right) {
            auto alias = entry.first;
            auto table = entry.second;
            for (auto &att : table->getSchema()->getAtts()) {
                string attName = att.first;
                MyDB_AttTypePtr attType = att.second;
                if (allAtts.count(make_pair(attName, attType)) ||
                    any_of(topDisjunctions.begin(), topDisjunctions.end(),
                           [&alias, &attName](ExprTreePtr disjunction) {
                               return disjunction->referencesAtt(alias, attName);
                           })) {
                    rightSchema->appendAtt(make_pair(attName, attType));
                }
            }
        }

		// LeftAtts ← Atts(Left) and (A union Atts(TopCNF))
		// RightAtts ← Atts(Right) and (A union Atts(TopCNF))
		// MyDB_SchemaPtr leftSchema = make_shared<MyDB_Schema>(), rightSchema = make_shared<MyDB_Schema>();
		// for (auto [alias, table] : left)
		// {
		// 	// MyDB_TablePtr tempTable = table->alias(alias);
		// 	for (auto [attName, attType] : table->getSchema()->getAtts())
		// 	{
		// 		leftSchema->appendAtt(make_pair(attName, attType));
		// 	}
		// }
		// for (auto [alias, table] : right)
		// {
		// 	// MyDB_TablePtr tempTable = table->alias(alias);
		// 	for (auto [attName, attType] : table->getSchema()->getAtts())
		// 	{
		// 		rightSchema->appendAtt(make_pair(attName, attType));
		// 	}
		// }

		pair<LogicalOpPtr, double> leftRes = optimizeQueryPlan(left, leftSchema, leftDisjunctions);
		pair<LogicalOpPtr, double> rightRes = optimizeQueryPlan(right, rightSchema, rightDisjunctions);

		if (leftRes.first == nullptr || rightRes.first == nullptr)
		{
			continue;
		}

		// Apply selection predicates to update statistics
		MyDB_StatsPtr leftStats = leftRes.first->getStats()->costSelection(leftDisjunctions);
		MyDB_StatsPtr rightStats = rightRes.first->getStats()->costSelection(rightDisjunctions);

		// Apply join predicates to calculate join cost
		MyDB_StatsPtr joinStats = leftStats->costJoin(topDisjunctions, rightStats);

		double joinCost = joinStats->getTupleCount(); // Focus on the size of the join result
		double leftCost = leftRes.second;  // Recursive left cost (excluding tuple count)
		double rightCost = rightRes.second;  // Recursive right cost (excluding tuple count)

		// Step 7: Add the costs of left, right, and join together, but avoid double-counting the tuple counts
		// cout<<"joinCost tuple count:" << joinStats->getTupleCount()<<endl;
		// cout<<"joinCost att count:" << joinStats->getAttVals("AND")<<endl;

		// for what ever reason, joinCost * 2 will wotk on example 4...
		cost = leftCost + rightCost + joinCost*2;


		// cost = joinStats->getTupleCount(); // Focus on the size of the join result

		// // Optional: Add penalties or weights for inputs
		// double leftCost = leftRes.second + leftStats->getTupleCount(); // Recursive left cost
		// double rightCost = rightRes.second + rightStats->getTupleCount(); // Recursive right cost

		// cost += leftCost + rightCost;

		if (cost < best) {
			best = cost;
			res = make_shared<LogicalJoin>(
				leftRes.first, rightRes.first,
				make_shared<MyDB_Table>("JoinResult", "outputPath", totSchema),
				allDisjunctions, joinStats);
				cout << "Updated best plan with cost: " << best << endl;
		}
	}
	// we have at least one join
	// some code here...
	return make_pair(res, best);
}

void SFWQuery ::print()
{
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect)
	{
		cout << "\t" << a->toString() << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess)
	{
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions)
	{
		cout << "\t" << a->toString() << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses)
	{
		cout << "\t" << a->toString() << "\n";
	}
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
					struct CNF *cnf, struct ValueList *grouping)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
	groupingClauses = grouping->valuesToCompute;
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause,
					struct CNF *cnf)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery ::SFWQuery(struct ValueList *selectClause, struct FromList *fromClause)
{
	valuesToSelect = selectClause->valuesToCompute;
	tablesToProcess = fromClause->aliases;
	allDisjunctions.push_back(make_shared<BoolLiteral>(true));
}

#endif
