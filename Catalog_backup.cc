#include <iostream>
#include <set>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "sqlite3.h"
#include "Schema.h"
#include "Catalog.h"
#include "InefficientMap.h"
#include "Keyify.h"
#include "TwoWayList.h"
#include "EfficientMap.h"

#include "InefficientMap.cc"
#include "Keyify.cc"
#include "TwoWayList.cc"
#include "EfficientMap.cc"
using namespace std;

char * databasefile;
sqlite3 *db;
int rc;
char * sql;
struct TableEntry{
	int num_tuples;
	string path;
	void TableEntryInit(int num_tuples, string path){
		this->num_tuples = num_tuples;
		this->path = path;
	}
};

ostream& operator<<(ostream& output,const TableEntry& now) {
	output << now.num_tuples << ", " << now.path;
	return output;
}

struct AttributeEntry{
	string aname;
	string atype;
	int num_distinct;
	void AttributeEntryInit(string aname, string atype, int num_distinct){
		this->aname = aname;
		this->atype = atype;
		this->num_distinct = num_distinct;
	}
};
ostream& operator<<(ostream& output, const AttributeEntry& now){
	output << now.aname << ", " << now.atype << ", " << now.num_distinct;
	return output;
}
InefficientMap <KeyString,Swapify<TableEntry> > mmaster2;
EfficientMap <KeyString, Swapify<AttributeEntry> > mattribute;
template<class Key, class Data>
static void fill_table(sqlite3* db, char* sql, InefficientMap < Key,  Data>& table){
	sqlite3_stmt *stat;
	string tname, tpath;
	int tnumoftuples;
	TableEntry te;
	KeyString a;
	if(sqlite3_prepare(db, sql, -1, &stat, 0) == SQLITE_OK){
		//int ctotal = sqlite3_column_count(stat);
		int res = 0;
		while(1){
			res = sqlite3_step(stat);
			if(res == SQLITE_ROW){
				tname = (char*)sqlite3_column_text(stat, 0);
				tnumoftuples = atoi((char*)sqlite3_column_text(stat, 1));
				tpath = (char*)sqlite3_column_text(stat, 2);
				te.TableEntryInit(tnumoftuples, tpath);
				Swapify<TableEntry> tmp(te);
				a = tname;
				table.Insert(a, tmp);
			}else if(res == SQLITE_DONE){
				break;
			}
		}
	}
}
template<class Key, class Data>
static void fill_attribute(sqlite3* db, char* sql, EfficientMap < Key,  Data>& table){
	sqlite3_stmt *stat;
	string tname, aname, atype;
	int num_distinct;
	AttributeEntry te; 
	KeyString a;
	if(sqlite3_prepare(db, sql, -1, &stat, 0) == SQLITE_OK){
		//int ctotal = sqlite3_column_count(stat);
		int res = 0;
		while(1){
			res = sqlite3_step(stat);
			if(res == SQLITE_ROW){
				tname= (char*)sqlite3_column_text(stat, 0);
				aname = (char*)sqlite3_column_text(stat, 1);
				atype = (char*)sqlite3_column_text(stat, 2);
				num_distinct = atoi((char*)sqlite3_column_text(stat, 3));
				te.AttributeEntryInit(aname, atype, num_distinct);
				Swapify<AttributeEntry> tmp(te);
				a = tname;
				table.Insert(a, tmp);
			}else if(res == SQLITE_DONE){
				break;
			}
		}
	}
}
Catalog::Catalog(string& _fileName) {
	 databasefile = new char[_fileName.length()+1];
	 strcpy(databasefile, _fileName.c_str());
	 rc = sqlite3_open(databasefile, &db);
	if(rc){
	 	exit(EXIT_FAILURE);
	 }else{
	 	char * sql = "Select * from master_table";
		fill_table(db, sql, mmaster2);
	 	char * sql2 = "Select * from master_attribute";
	 	fill_attribute(db, sql2, mattribute);
	 }
}

Catalog::~Catalog() {
	string sql = "Delete from master_table;", tmp;
	sqlite3_stmt *stat;
	sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
	sqlite3_step(stat);
	sql = "Delete from master_attribute;";
	sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
	sqlite3_step(stat);
	mmaster2.MoveToStart();
	while(!mmaster2.AtEnd()){
		tmp = mmaster2.CurrentKey();
		sql = string("Insert Into master_table (tname, number_of_tuples, path) ") + "Values ('"+ tmp + "', " + to_string(mmaster2.CurrentData().test().num_tuples) + ", '" + mmaster2.CurrentData().test().path +"');";
		sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
		sqlite3_step(stat);
		mmaster2.Advance();
	}
	mattribute.MoveToStart();
	while(!mattribute.AtEnd()){
		tmp = mattribute.CurrentKey();
		sql = string("Insert Into master_attribute (tname, attribute_name, attribute_type, num_distinct) ") + "Values ('"+ tmp + "', '" + mattribute.CurrentData().test().aname + "', '" + mattribute.CurrentData().test().atype +"', " + to_string(mattribute.CurrentData().test().num_distinct)+" );";
		sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
		sqlite3_step(stat);
		mattribute.Advance();
	}
	sqlite3_close(db);
}

bool Catalog::Save() {
	string sql = "Delete from master_table;", tmp;
	sqlite3_stmt *stat;
	sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
	sqlite3_step(stat);
	sql = "Delete from master_attribute;";
	sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
	sqlite3_step(stat);
	mmaster2.MoveToStart();
	while(!mmaster2.AtEnd()){
		tmp = mmaster2.CurrentKey();
		sql = string("Insert Into master_table (tname, number_of_tuples, path) ") + "Values ('"+ tmp + "', " + to_string(mmaster2.CurrentData().test().num_tuples) + ", '" + mmaster2.CurrentData().test().path +"');";
		sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
		sqlite3_step(stat);
		mmaster2.Advance();
	}
	mattribute.MoveToStart();
	while(!mattribute.AtEnd()){
		tmp = mattribute.CurrentKey();
		sql = string("Insert Into master_attribute (tname, attribute_name, attribute_type, num_distinct) ") + "Values ('"+ tmp + "', '" + mattribute.CurrentData().test().aname + "', '" + mattribute.CurrentData().test().atype +"', " + to_string(mattribute.CurrentData().test().num_distinct)+" );";
		sqlite3_prepare(db, sql.c_str(), -1, &stat, 0);
		sqlite3_step(stat);
		mattribute.Advance();
	}
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
		KeyString a(_table);
		if(mmaster2.IsThere(a)){
			_noTuples = mmaster2.Find(a).test().num_tuples;
			return true;
		}else{
			return false;
		}
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	KeyString a(_table);
	if(mmaster2.IsThere(a)){
		 mmaster2.Find(a).setnotuples(_noTuples);
	}
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	KeyString a(_table);
	if(mmaster2.IsThere(a)){
		 _path = mmaster2.Find(a).test().path;
		return true;
	}else{
		return false;
	}
}

void Catalog::SetDataFile(string& _table, string& _path) {
	KeyString a(_table);
	if(mmaster2.IsThere(a)){
		 mmaster2.Find(a).setdatafile(_path);// = _noTuples;
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString a(_table);
	if(mmaster2.IsThere(a) && mattribute.IsThere(a)){
		while(!mattribute.AtEnd()){
			if(mattribute.CurrentData().test().aname == _attribute){
					break;
			}
			mattribute.Advance();
		}
		if(mattribute.CurrentData().test().aname == _attribute){
			_noDistinct = mattribute.CurrentData().test().num_distinct;
			return true;
		}else{
			return false;
		}
		/*if(mattribute.AtEnd() && mattribute.CurrentData().test().aname != _attribute){
			return false;
		}*/
	}else{

		return false;
	}
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	KeyString a(_table);
	if(mmaster2.IsThere(a)){
		bool found = false;
		while(!mattribute.AtEnd()){
			if(mattribute.CurrentData().test().aname == _attribute){
					found = true;
					break;
			}
			mattribute.Advance();
		}
		if(!found)
			return;
		if(mattribute.CurrentData().test().aname == _attribute){
			mattribute.CurrentData().setnodistinct(_noDistinct);
		}
		/*if(mattribute.AtEnd() && mattribute.CurrentData().test().aname != _attribute){
			//cout << "Unsuccessful" <<endl;
		}else{
			mattribute.CurrentData().setnodistinct(_noDistinct);
		//cout << "Successful" <<endl;
		}*/
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	for (mmaster2.MoveToStart(); !mmaster2.AtEnd(); mmaster2.Advance()) {
		_tables.push_back(mmaster2.CurrentKey());
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	KeyString a(_table);
	if(mmaster2.IsThere(a) && mattribute.IsThere(a)){
		while(!mattribute.CurrentKey().IsEqual(a)){
			mattribute.Advance();
		}
		while(!mattribute.AtEnd() && mattribute.CurrentKey().IsEqual(a)){
			_attributes.push_back(mattribute.CurrentData().test().aname);
			mattribute.Advance();
		}
		return true;
	}else{

		return false;
	}
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	KeyString a(_table);
	if(mmaster2.IsThere(a) && mattribute.IsThere(a)){
		while(!mattribute.CurrentKey().IsEqual(a)){
			mattribute.Advance();
		}
		vector<string> attributes, types;
		vector<unsigned int> distincts;
		while(!mattribute.AtEnd() && mattribute.CurrentKey().IsEqual(a)){
			attributes.push_back(mattribute.CurrentData().test().aname);
			types.push_back(mattribute.CurrentData().test().atype);
			distincts.push_back(mattribute.CurrentData().test().num_distinct);
			mattribute.Advance();
		}
		Schema s(attributes, types, distincts);
		_schema = s;
		return true;
	}else{

		return false;
	}
}
template <class T>
bool is_unique(vector<T> X){
	set<T> Y(X.begin(), X.end());
	return X.size() == Y.size();
}
bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	KeyString a(_table);
	//#pragma omp parallel for
	for (mmaster2.MoveToStart(); !mmaster2.AtEnd(); mmaster2.Advance()) {
		if(mmaster2.CurrentKey().IsEqual(a)){
			return false;
		}
	}
	//if(!is_unique(_attributes))
	int size = 1;
	vector<string> masteratt;
	mattribute.MoveToStart();
	while(!mattribute.AtEnd()){
		if(mattribute.CurrentKey().IsEqual(a)){
			masteratt.push_back(mattribute.CurrentData().test().aname);
			size++;
		}
		mattribute.Advance();
	}
	int i;
	//#pragma omp parallel for private(i)
	for(i = 0; i < _attributes.size(); i++){
		masteratt.push_back(_attributes[i]);
	}
	if(!is_unique(masteratt)){
		return false;
	}
	string tmpp;
	AttributeEntry te;	
	for (i = 0; i<_attributeTypes.size(); i++){
		tmpp = _attributeTypes[i];
		if(tmpp == "INTEGER" || tmpp == "FLOAT" || tmpp == "STRING"){
			te.AttributeEntryInit(_attributes[i], tmpp, 0);
			Swapify<AttributeEntry> tmp(te);
			KeyString c(_table);
			mattribute.Insert(c, tmp);
		}else{
			return false;
		}
	}
	TableEntry te1;
	te1.TableEntryInit(0, "");
	Swapify<TableEntry> tmp(te1);
	KeyString b(_table);
	mmaster2.Insert(b, tmp);
	return true;
}

bool Catalog::DropTable(string& _table) {
	KeyString a(_table);
	if(mmaster2.IsThere(a)){
		mmaster2.Remove(a, a, mmaster2.Find(a));
		mattribute.MoveToStart();
			int size = 1;
			while(!mattribute.AtEnd()){
				if(mattribute.CurrentKey().IsEqual(a)){
					size++;
				}
				mattribute.Advance();
			}
			while(size>1){
				mattribute.Remove(a, a, mattribute.Find(a));
				size--;
			}
		return true;
	}else{
		return false;
	}
}

ostream& operator<<(ostream& _os, Catalog& _c) {

	 return _os << mmaster2 <<endl << mattribute;

}
