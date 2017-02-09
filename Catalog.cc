#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"
#include "EfficientMap.h"

using namespace std;

int Catalog::callback(void *data, int argc, char **argv, char **azColName){
	   	   int i;
	   	   fprintf(stderr, "%s: ", (const char*)data);

	   	   for(i=0; i<argc; i++){
	   	      //sprintf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
	   		  // KeyString ks;
	   		  // ks.Keyify(azColName[i]);
	   		  // tableNames.Insert(ks,argv[i]);

	   		   vector<data> dataList;
	   		   struct data dataPiece;
	   		   dataPiece.tabName = "";
	   		   dataPiece.numTuples =
	   		   dataPiece.nameList["argv[i]"] =
	   	   }
	   	   printf("\n");
	   	   return 0;
	   	}



Catalog::Catalog(string& _fileName) {

	char *zErrMsg = 0;
	int rc;
	sqlite3 *db;
	rc = sqlite3_open("_filename", &db);
	if( rc ){
	      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	   }else{
	      fprintf(stderr, "Opened database successfully\n");
	 }
	sqlite3_stmt *statement;
	char *sql = "select * from tables";
	const char* data = "Callback function called";
	 /* Execute SQL statement */
	   rc = sqlite3_exec(db, sql, callback, (void*)data, &zErrMsg);
	   if( rc != SQLITE_OK ){
	      fprintf(stderr, "SQL error: %s\n", zErrMsg);
	      sqlite3_free(zErrMsg);
	   }else{
	      fprintf(stdout, "Operation done successfully\n");
	   }
	   sqlite3_close(db);


}

Catalog::~Catalog() {
}

bool Catalog::Save() {
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
