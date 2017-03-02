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
	return _os << "  <<--< SCAN" ;

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
	_os << "(SELECT | " ;

	for (int i = 0; i < predicate.numAnds; i++) {
			if (i == 0) _os << "("; else _os << " (";
			Comparison c = predicate.andList[i];
			vector<Attribute> attList = schema.GetAtts();
			//first operand

			if (c.operand1 == Left){
				int pos = c.whichAtt1;
				_os << attList[pos].name;
			}
			else if (c.operand1 == Literal)
				if(c.attType==Integer){
					int *tempInt = (int *)constants.GetColumn(c.whichAtt1);
					_os << *tempInt;
				}
				else if(c.attType==Float){
					float *tempDouble = (float *)constants.GetColumn(c.whichAtt1);
					_os << *tempDouble;
				}
				else if(c.attType==String){
					_os << (char *)constants.GetColumn(c.whichAtt1);
				}

			//cop
			if (c.op == LessThan) _os << " < ";
				else if (c.op == GreaterThan) _os << " > ";
				else _os << " = ";

			//second operand
			if (c.operand2 == Right){
				int pos = c.whichAtt2;
				_os << attList[pos].name;
			}
			else if (c.operand2 == Literal)
				if(c.attType==Integer){
					int *tempInt = (int *)constants.GetColumn(c.whichAtt2);
					_os << *tempInt;
				}
				else if(c.attType==Float){
					float *tempDouble = (float *)constants.GetColumn(c.whichAtt2);
					_os << *tempDouble;
				}
				else if(c.attType==String){
					_os << (char *)constants.GetColumn(c.whichAtt2);
				}

			if (i < predicate.numAnds-1) _os << ") AND";
			else _os << ")";
		}

		_os  << " |" << *producer;

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
	return _os << "PROJECT {" <<  schemaOut << "} "  << *producer  ;
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
	//return _os << "JOIN (" << *left << " & " << *right <<  ")";
	_os << "JOIN " ;
	for (int i = 0; i < predicate.numAnds; i++) {
			if (i == 0) _os << "("; else _os << " (";

			// print the comparison
			Comparison c = predicate.andList[i];
			vector<Attribute> attListLeft = schemaLeft.GetAtts();
			vector<Attribute> attListRight = schemaRight.GetAtts();
			if (c.operand1 == Left){
				int pos = c.whichAtt1;
				_os << attListLeft[pos].name;
			}
			else if (c.operand1 == Right) {
				int pos = c.whichAtt1;
				_os << attListRight[pos].name;;
			}

			if (c.op == LessThan) _os << " < ";
			else if (c.op == GreaterThan) _os << " > ";
			else _os << " = ";

			if (c.operand2 == Left){
				int pos = c.whichAtt2;
				_os << attListLeft[pos].name;
			}
			else if (c.operand2 == Right) {
				int pos = c.whichAtt2;
				_os << attListRight[pos].name;;
			}

			if (i < predicate.numAnds-1) _os << ") AND"; else _os << ")";
		}
	_os  << " < "<< *left << " + " << *right << " > "  ;
	return _os;
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

			return _os << "SUM() <<--< " << *producer;
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
	//return _os << "GROUP BY (" << *producer << ")";
	_os << "GROUP BY (";
			int pos = groupingAtts.whichAtts[0];
			vector<Attribute> attList = schemaIn.GetAtts();
		_os << attList[pos].name;
		_os << ") " << *producer;
	return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << endl << endl << endl << *producer << endl;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	if (_op.root != NULL)
	return _os << "QUERY EXECUTION TREE" << *_op.root ;
}
