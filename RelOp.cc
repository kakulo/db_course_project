#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;

}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	//return _os << "(SELECT <- " << *producer << ")";
	for (int i = 0; i < predicate.numAnds; i++) {
			if (i == 0) _os << "("; else _os << " (";
			Comparison c = predicate.andList[i];
			vector<Attribute> attList = schema.GetAtts();
			if (c.operand1 == Left){
				int pos = c.whichAtt1;
				_os << attList.at(pos);
			}
			if (c.op == LessThan) _os << " < ";
				else if (c.op == GreaterThan) _os << " > ";
				else _os << " = ";

			if (c.operand2 == Left){
				int pos = c.whichAtt2;
				_os << attList.at(pos);
			}
			else if (c.operand2 == Literal){

			}

			if (i < predicate.numAnds-1) _os << ") AND";
			else _os << ")";
		}

		_os << "]";

		return _os;

}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
	
}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT (" << *producer << ")";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

	schemaLeft = _schemaLeft; 	
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN (" << *left << " & " << *right <<  ")";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	
	schema = _schema;
	producer = _producer;

}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT (" << *producer << ")";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut; 
	compute = _compute;
	producer = _producer;

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM (" << *producer << ")";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

	schemaIn =  _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;	
	producer = _producer;

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY (" << *producer << ")";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT:\n{\n\t" << *producer <<"\n}\n";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	if (_op.root != NULL)
	return _os << "QUERY EXECUTION TREE\n"	<< *_op.root ;
}