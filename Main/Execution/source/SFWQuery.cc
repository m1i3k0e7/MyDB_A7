
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
	
// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables) {

	// here we call the recursive, exhaustive enum. algorithm
	// return optimizeQueryPlan (...);
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
pair <LogicalOpPtr, double> SFWQuery :: optimizeQueryPlan (map <string, MyDB_TablePtr> &allTables, 
	MyDB_SchemaPtr totSchema, vector <ExprTreePtr> &allDisjunctions) {

	LogicalOpPtr res = nullptr;
	double cost = 9e99, best = 9e99;

	// case where no joins
	if (allTables.size () == 1) {
		res = make_shared<LogicalTableScan> (allTables.begin()->second, allTables.begin()->second, 
			make_shared <MyDB_Stats> (allTables.begin()->second), allDisjunctions);
		res->getStats()->costSelection (allDisjunctions);
		best = res->getStats()->getTupleCount();
	} else {
		vector<pair<string, MyDB_TablePtr>> tables;
		for (auto [alias, table] : allTables) {
			tables.push_back (make_pair (alias, table));
		}
		
		// find all ways to split the tables into two groups
		set<map<string, MyDB_TablePtr>> checked;
		for (int i = 1; i < (1 << tables.size ()); i++) {
			map<string, MyDB_TablePtr> left, right;
			for (int j = 0; j < tables.size (); j++) {
				if (i & (1 << j)) {
					left[tables[j].first] = tables[j].second;
				} else {
					right[tables[j].first] = tables[j].second;
				}
			}
			
			if (left.size () == 0 || right.size () == 0) {
				continue;
			}

			if (checked.find (left) != checked.end ()) {
				checked.insert (left);
				continue;
			}
			
			// LeftCNF ← all clauses in C referring only to atts in Left
			// RightCNF ← all clauses in C referring only to atts in Right
			// TopCNF ← all clauses in C not in LeftCNF & not in RightCNF
			vector<ExprTreePtr> leftDisjunctions, rightDisjunctions, topDisjunctions;
			for (auto d : allDisjunctions) {
				// if (a->referencesTable ()) {
				// 	leftDisjunctions.push_back (a);
				// } else if (a->referencesTable ()) {
				// 	rightDisjunctions.push_back (a);
				// } else {
				// 	topDisjunctions.push_back (a);
				// }
			}

			// LeftAtts ← Atts(Left) and (A union Atts(TopCNF))
			// RightAtts ← Atts(Right) and (A union Atts(TopCNF))
			MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> (), rightSchema = make_shared <MyDB_Schema> ();
			for (auto [alias, table] : left) {
				MyDB_TablePtr tempTable = table->alias(alias);
				for (auto [attName, attType] : tempTable->getSchema()->getAtts ()) {
					leftSchema->appendAtt (make_pair (attName, attType));
				}
			}
			for (auto [alias, table] : right) {
				MyDB_TablePtr tempTable = table->alias(alias);
				for (auto [attName, attType] : tempTable->getSchema()->getAtts ()) {
					rightSchema->appendAtt (make_pair (attName, attType));
				}
			}

			pair<LogicalOpPtr, double> leftRes = optimizeQueryPlan (left, leftSchema, leftDisjunctions);
			pair<LogicalOpPtr, double> rightRes = optimizeQueryPlan (right, rightSchema, rightDisjunctions);

			if (leftRes.first == nullptr || rightRes.first == nullptr) {
				continue;
			}
			
			leftRes.first->getStats()->costSelection (leftDisjunctions);
			rightRes.first->getStats()->costSelection (rightDisjunctions);
			leftRes.first->getStats()->costJoin(topDisjunctions, rightRes.first->getStats());
			cost = leftRes.second + rightRes.second;
			if (cost < best) {
				best = cost;
				res = make_shared<LogicalJoin> (leftRes.first, rightRes.first, totSchema, allDisjunctions, 
					make_shared <MyDB_Stats> (leftRes.first->getStats(), rightRes.first->getStats()));
			}
		}

		return make_pair (res, best);
	}

	// we have at least one join
	// some code here...
	return make_pair (res, best);
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
