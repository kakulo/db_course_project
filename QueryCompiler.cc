#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {	
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// SCAN operator for each table in the query along with push-down selections

	TableList* tables = _tables;
	
	while (tables != NULL)
	{
		DBFile db;
		Schema sch;
		string tab(tables->tableName);
		catalog->GetSchema(tab, sch);

		Scan scan(sch,db);
		scanz.insert (make_pair(tab,scan) );

		CNF cnf;
		Record rec;
		cnf.ExtractCNF (*_predicate, sch, rec);

		if (cnf.numAnds != 0)
		{
			Select sel(sch, cnf , rec ,(RelationalOp*) & scanz.at(tab));
			selectz.insert (make_pair(tab,sel) );
                }

		tables = tables->next;
	}

	// Optimizer to compute the join order

	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);
	OptimizationTree* rootCopy = root;
	
	// Creating join operators based on the optimal order computed by the optimizer

	RelationalOp* join = constTree(rootCopy, _predicate);

	// Creating the remaining operators based on the query

	Schema dupSch;	
	join->returnSchema(dupSch);	
	DuplicateRemoval* duplicateRemoval = new DuplicateRemoval(dupSch, join);

	if (_distinctAtts == 1)
	{
		join = (RelationalOp*) duplicateRemoval;
	}

	if (_finalFunction == NULL) 
	{
		Schema projSch;
		join->returnSchema(projSch);
		vector<Attribute> projAtts = projSch.GetAtts();	

		NameList* attsToSelect = _attsToSelect;
		int numAttsInput = projSch.GetNumAtts(), numAttsOutput = 0; 
		Schema projSchOut = projSch;
		vector<int> keepMe;

		while (attsToSelect != NULL)
		{
			string str(attsToSelect->name);
			keepMe.push_back(projSch.Index(str));
			attsToSelect = attsToSelect->next;
			numAttsOutput++;
		}
	
		projSchOut.Project(keepMe);
		Project* project = new Project (projSch, projSchOut, numAttsInput, numAttsOutput, &keepMe[0], join);
		
		join = (RelationalOp*) project;
	
	}
	else
	{
		if (_groupingAtts == NULL) 
		{
			Schema schIn, schIn_;
			join->returnSchema(schIn_);
			schIn = schIn_;

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schIn_);

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);
			Schema schOutSum(attributes, attributeTypes, distincts);
		
			Sum* sum = new Sum (schIn, schOutSum, compute, join);
			join = (RelationalOp*) sum;
		}

		else
		{
			Schema schIn, schIn_;
			join->returnSchema(schIn_);
			schIn = schIn_;

			NameList* grouping = _groupingAtts;
			int numAtts = 0; 
			vector<int> keepMe;

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);

			while (grouping != NULL)
			{
				string str(grouping->name);
				keepMe.push_back(schIn_.Index(str));
				attributes.push_back(str);

				Type type;
				type = schIn_.FindType(str);

				switch(type) 
				{
					case Integer: attributeTypes.push_back("INTEGER");	break;
					case Float:	attributeTypes.push_back("FLOAT");	break;
					case String: attributeTypes.push_back("STRING");	break;
					default: attributeTypes.push_back("UNKNOWN");	break;
				}
				
				distincts.push_back(schIn_.GetDistincts(str));
			
				grouping = grouping->next;
				numAtts++;
			}
			
			Schema schOut(attributes, attributeTypes, distincts);
			OrderMaker groupingAtts(schIn_, &keepMe[0], numAtts);

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schIn);
	
			GroupBy* groupBy = new GroupBy (schIn, schOut, groupingAtts, compute, join);	
			join = (RelationalOp*) groupBy;
		}	
	}
	
	Schema finalSchema;
	join->returnSchema(finalSchema);
	string outFile = "Output_File.txt";
	WriteOut* writeout = new WriteOut(finalSchema, outFile, join);
	join = (RelationalOp*) writeout;

	// Connecting everything in the query execution tree

	_queryTree.SetRoot(*join);	

	// free the memory occupied by the parse tree since it is not necessary anymore
}


RelationalOp* QueryCompiler::constTree(OptimizationTree* root, AndList* _predicate)
{
	if (root -> leftChild == NULL && root -> rightChild == NULL)
	{	
		cout<<"\nQuery has only one table\n\n";
		RelationalOp* op;
		auto it = selectz.find(root -> tables[0]);
		if(it != selectz.end())	op = (RelationalOp*) & it->second;
		else op = (RelationalOp*) & scanz.at(it->first);

		return op;
	}

	if (root -> leftChild -> tables.size() == 1  && root -> rightChild -> tables.size() == 1) 
	{
		string left = root -> leftChild -> tables[0];
		string right = root -> rightChild -> tables[0];

		CNF cnf;
		Schema sch1, sch2;
		RelationalOp *lop, *rop;

		auto it = selectz.find(left);
		if(it != selectz.end())	lop = (RelationalOp*) & it->second;
		else lop = (RelationalOp*) & scanz.at(left);

		it = selectz.find(right);
		if(it != selectz.end()) rop = (RelationalOp*) & it->second;
		else rop = (RelationalOp*) & scanz.at(right);
	
		lop->returnSchema(sch1);
		rop->returnSchema(sch2);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	
	}

	if (root -> leftChild -> tables.size() == 1)
	{	
		string left = root -> leftChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;		
		RelationalOp* lop;

		auto it = selectz.find(left);
		if(it != selectz.end())	lop = (RelationalOp*) & it->second;
		else lop = (RelationalOp*) & scanz.at(left);

		lop->returnSchema(sch1);
		RelationalOp* rop = constTree(root -> rightChild, _predicate);
		rop->returnSchema(sch2);

		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	if (root -> rightChild -> tables.size() == 1)
	{	
		string right = root -> rightChild -> tables[0];
		Schema sch1,sch2;
		CNF cnf;
		RelationalOp* rop;

		auto it = selectz.find(right);
		if(it != selectz.end()) rop = (RelationalOp*) & it->second;
		else rop = (RelationalOp*) & scanz.at(right);

		rop->returnSchema(sch2);
		RelationalOp* lop = constTree(root -> leftChild, _predicate);
		lop->returnSchema(sch1);
		
		cnf.ExtractCNF (*_predicate, sch1, sch2);
		Schema schout = sch1;
		schout.Append(sch2);
		Join* join = new Join(sch1, sch2, schout, cnf, lop , rop);
		return ((RelationalOp*) join);
	}

	Schema sch1,sch2;
	CNF cnf;
	RelationalOp *lopp = constTree(root->leftChild, _predicate);
	RelationalOp *ropp = constTree(root->rightChild, _predicate);

	lopp->returnSchema(sch1);
	ropp->returnSchema(sch2);

	cnf.ExtractCNF (*_predicate, sch1, sch2);
	Schema schout = sch1;
	schout.Append(sch2);
	Join* join = new Join(sch1, sch2, schout, cnf, lopp , ropp);
	return ((RelationalOp*) join);

}




