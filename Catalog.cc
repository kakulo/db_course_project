#include <iostream>
#include<sstream>
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
	   int i = 3;
	while (sqlite3_step(stmt) == SQLITE_ROW) {
		tables.tableName = (char*)sqlite3_column_text(stmt, i);
		tables.numTuples = sqlite3_column_int(stmt, i+1);
		tables.fileLoc = (char*)sqlite3_column_text(stmt, i+2);
		tableData.insert(pair<string,tableStruct>(tables.tableName,tables));
		i=i+3;
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
			j=j+5;
			}
		sqlite3_finalize(stmt2);
	sqlite3_close(db);
}

Catalog::~Catalog() {
	Save();

}

bool Catalog::Save() {
		sqlite3 *db;
		sqlite3_stmt *stmt, *stmt2;

		if (sqlite3_open("_fileName", &db) == SQLITE_OK){
			//write into tableNames
			for(map<string, tableStruct>::iterator it = tableData.begin(); it != tableData.end(); it++){
					string tableName = tableData.at(it->first).tableName;
					unsigned int numTuples=tableData.at(it->first).numTuples;
					string fileLoc=tableData.at(it->first).fileLoc;
					ostringstream oss;
					oss << "insert into tableNames values (" << "'"<< tableName << "'"<<"," << numTuples << "," << "'"<< fileLoc << "'"<< ")";
					sqlite3_prepare_v2( db, oss.str().c_str(), -1, &stmt, NULL );//preparing the statement
					sqlite3_step( stmt );//executing the statement
			}
			//write into attributes
			for(auto it = attributeData.begin(); it != attributeData.end(); it++){
				int j=0;
				for(auto i= attributeData.at(it->first).begin(); i!= attributeData.at(it->first).end(); i++){
					int attributeId = attributeData.at(it->first).at(j).attid;
					string attributeType = attributeData.at(it->first).at(j).attType;
					string attributeName = attributeData.at(it->first).at(j).attName;
					unsigned int numberOfDist = attributeData.at(it->first).at(j).numDist;
					string attributeTable = attributeData.at(it->first).at(j).tableName;
					ostringstream oss2;
					oss2 << "insert into attributes values (" <<attributeId << ","
						<< "'"	<< attributeType << "'"<< "," << "'" << attributeName << "'" <<  numberOfDist << "'" << attributeTable << "'" <<")";
					sqlite3_prepare_v2( db, oss2.str().c_str(), -1, &stmt2, NULL );//preparing the statement
					sqlite3_step( stmt2 );//executing the statement
					j++;
					}
			}
		}
	    else{
	        return false;
	    }
	    sqlite3_finalize(stmt);
	    sqlite3_finalize(stmt2);
	    sqlite3_close(db);
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
	int j=0;
	for(auto i= attributeData.at(_table).begin(); i!= attributeData.at(_table).end(); i++){
				if((attributeData.at(_table).at(j).attName) == _attribute)
				_noDistinct = attributeData.at(_table).at(j).numDist;
				else j++;
				}
	return true;
}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	int j=0;
	for(auto i= attributeData.at(_table).begin(); i!= attributeData.at(_table).end(); i++){
			if((attributeData.at(_table).at(j).attName) == _attribute)
					attributeData.at(_table).at(j).numDist=_noDistinct;
			else j++;
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	for(map<string, tableStruct>::iterator it = tableData.begin(); it != tableData.end(); it++)
	  _tables.push_back(it->first);
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	//look for table name in the map return false for not found
	if(tableData.find(_table)==tableData.end()) {return false;}
	int j=0;
	for(auto i= attributeData.at(_table).begin(); i!= attributeData.at(_table).end(); i++){
				_attributes.push_back(attributeData.at(_table).at(j).attName) ;
				j++;
		}
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	if(tableData.find(_table)==tableData.end()) {return false;}
	vector<string> _att; vector<string> _attTypes; vector<unsigned int> _dist ;
	for(map<string, vector<attStruct>>::iterator it = attributeData.begin(); it != attributeData.end(); it++){
		int j=0;
		for(auto i= attributeData.at(it->first).begin(); i!= attributeData.at(it->first).end(); i++){
			_att.push_back(attributeData.at(it->first).at(j).attName) ;
			_attTypes.push_back(attributeData.at(it->first).at(j).attType);
			_dist.push_back(attributeData.at(it->first).at(j).numDist);
			j++;
			}
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
	for(auto it = _c.attributeData.begin(); it != _c.attributeData.end(); it++){
		_os << _c.attributeData.at(it->first).data()->tableName  << " ( " ;
		int j=0;
		for(auto i=_c.attributeData.at(it->first).begin(); i!=_c.attributeData.at(it->first).end(); i++){
				_os << _c.attributeData.at(it->first).at(j).attName << " ";
				_os << _c.attributeData.at(it->first).at(j).attType << ", ";
				j++;
				}
		_os << ")" ;

	}
	return _os;
}
