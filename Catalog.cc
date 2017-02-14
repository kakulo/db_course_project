#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;


Catalog::Catalog(string& _fileName) {
	//get table metadata
	sqlite3* db;
	sqlite3_open("_fileName", &db);

	sqlite3_stmt* stmt;
	sqlite3_prepare_v2(db, "select * from tableNames", -1, &stmt, NULL);
	tableStruct tables;
	   int i = 4;
	while (sqlite3_step(stmt) == SQLITE_ROW) {
		tables.tableName = (char*)sqlite3_column_text(stmt, i);
		tables.numTuples = sqlite3_column_int(stmt, i+1);
		tables.fileLoc = (char*)sqlite3_column_text(stmt, i+2);
		tableData.insert(pair<string,tableStruct>(tables.tableName,tables));
		i++;
		}
	sqlite3_finalize(stmt);

	//get attribute data
	sqlite3_stmt* stmt2;
		sqlite3_prepare_v2(db, "select * from attributes", -1, &stmt2, NULL);
		//Schema sc;
		   int j = 5;
		while (sqlite3_step(stmt2) == SQLITE_ROW) {
			attStruct atts;
			atts.attid = sqlite3_column_int(stmt2, j);
			atts.attType = (char*)sqlite3_column_text(stmt2, j+1);
			atts.attName = (char*)sqlite3_column_text(stmt2, j+2);
			atts.numDist = sqlite3_column_int(stmt2, j+3);
			atts.tableName = (char*)sqlite3_column_text(stmt2, j+4);
			attv.push_back(atts);
			attributeData.insert(pair<string,vector<attStruct>>(atts.tableName,attv));
			j++;
			}
		sqlite3_finalize(stmt2);
	sqlite3_close(db);
}

Catalog::~Catalog() {
}

bool Catalog::Save() {

	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

	if(tableData.find(_table)==tableData.end()) {return false;}
	_noTuples = tableData.at(_table).numTuples;
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	//if(tableData.find(_table)==tableData.end()) {return false;}
	tableData.at(_table).numTuples = _noTuples;
	//return true;

}

bool Catalog::GetDataFile(string& _table, string& _path) {
	if(tableData.find(_table)==tableData.end()) {return false;}
	_path=tableData.at(_table).fileLoc;
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	tableData.at(_table).fileLoc=_path;
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	if(tableData.find(_table)==tableData.end()) {return false;}
	_noDistinct = attributeData.at(_table).data()->numDist;
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	 attributeData.at(_table).data()->numDist = _noDistinct;
}

void Catalog::GetTables(vector<string>& _tables) {
	for(map<string, tableStruct>::iterator it = tableData.begin(); it != tableData.end(); ++it)
	  _tables.push_back(it->first);
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	//look for table name in the map return false for not found
	if(tableData.find(_table)==tableData.end()) {return false;}

	for(map<string, vector<attStruct>>::iterator it = attributeData.begin(); it != attributeData.end(); ++it){
	_attributes.push_back(attributeData.at(_table).data()->attName);
	}
		return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	if(tableData.find(_table)==tableData.end()) {return false;}
	vector<string> _att; vector<string> _attTypes; vector<unsigned int> _dist ;
	for(map<string, vector<attStruct>>::iterator it = attributeData.begin(); it != attributeData.end(); ++it){
		_att.push_back(attributeData.at(_table).data()->attName);
		_attTypes.push_back(attributeData.at(_table).data()->attType);
		_dist.push_back(attributeData.at(_table).data()->numDist);
	}
	Schema sc(_att,_attTypes,_dist);
	_schema=sc;
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

		//check if table already exists
		//if(attributeData.find(_table)==attributeData.end()) {return false;}

		//update Catalog attributeData
		for (int i=0; i<_attributes.size(); i++){
			attStruct atts;
			atts.attName = _attributes.at(i);
			atts.attType = _attributeTypes.at(i);
			//if( (atts.attType != "Integer") || (atts.attType!="String") || (atts.attType!="Float") ){return false;}
			atts.attid = i+1;
			atts.numDist = 0;
			atts.tableName = _table;
			attv.push_back(atts);
		}
		attributeData.insert(pair<string,vector<attStruct>>(_table,attv));

		//update Catalog tableData
		tableStruct tablex;
		tablex.tableName = _table;
		tablex.numTuples = 0;
		tablex.fileLoc = "";
		tableData.insert(pair<string,tableStruct>(_table,tablex));
		return true;
}

bool Catalog::DropTable(string& _table) {
	//look for table name in the map return false for not found
	if(tableData.find(_table)==tableData.end()) {return false;}
	//delete by key i.e table name
	attributeData.erase (_table);
	tableData.erase(_table);
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
