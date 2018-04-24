
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

const int DEBUG = 0;

#define TABLES_TABLE_NAME "Tables"
#define COLUMNS_TABLE_NAME "Columns"
#define INDEXES_TABLE_NAME "Indexes"

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator
{
public:
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC open(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute, const CompOp compOp,
			const void * value, const vector<string> &attributeNames);

  RC getNextTuple(RID &rid, void *data);

  RC close();

  RBFM_ScanIterator rbfm_ScanIterator;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator{
 public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(); 	// Destructor

  RC initParam(
  		const Attribute attr, const void* lowkey, const void* highKey,
  		bool lowKeyInclusive, bool highKeyInclusive);

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			        // Terminate index scan


  IX_ScanIterator ix_ScanIterator;
  IXFileHandle ixfileHandle;
};

// new class to store tables and columns data
class SingleColumn
{
public:
	int table_id;
	string column_name;
	int column_type;
	int column_length;
	int column_position;
	RID column_rid;

	SingleColumn(int SCtable_id,
				string SCcolumn_name,
				int SCcolumn_type,
				int SCcolumn_length,
				int SCcolumn_position,
				RID SCcolumn_rid)
	{
		this->table_id = SCtable_id;
		this->column_name = SCcolumn_name;
		this->column_type = SCcolumn_type;
		this->column_length = SCcolumn_length;
		this->column_position = SCcolumn_position;
		this->column_rid = SCcolumn_rid;
	}
};

class SingleIndex
{
public:
	int table_id;
	string index_table_name;
	string index_attr_name;
	string index_file_name;
	RID index_rid;

	SingleIndex(){

	}

	SingleIndex(int SItable_id,
				string SItable_name,
				string SIattr_name,
				string SIfile_name,
				RID SItable_rid)
	{
		this->table_id = SItable_id;
		this->index_table_name = SItable_name;
		this->index_attr_name = SIattr_name;
		this->index_file_name = SIfile_name;
		this->index_rid = SItable_rid;
	}
};

class SingleTable
{
public:
	int table_id;
	string table_name;
	string file_name;
	RID table_rid;
	vector<SingleColumn> columns;
	vector<SingleIndex> indexes;

	SingleTable(int STtable_id,
				string STtable_name,
				string STfile_name,
				RID STtable_rid,
				vector<SingleColumn> STcolumns,
				vector<SingleIndex> STindexes)
	{
		this->table_id = STtable_id;
		this->table_name = STtable_name;
		this->file_name = STfile_name;
		this->table_rid = STtable_rid;
		this->columns = STcolumns;
		this->indexes = STindexes;
	}
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  // Added by Xingyu Wu for Index operations
  RC getAttribute(const string &tableName, const string index_attr_name,
		  Attribute &attr);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


// ------------------- New functions (Xingyu Wu) ----------------------

private:
  /* -------------------- File Handle Related Functions -------------------- */
  // Sys Table File IO
  RC openOrCreateAndOpenSysTables(const string &operation);

  // Minimize sys call of Data Table File IO
  RC switchDataFileHandle(const string &tableName);

  RC switchIndexFileHandle(const string &tableName);

  // close and destroy data or catalog files
  RC closeAndDestroyTableFile(const string &tableName);

  /* -------------------- Commonly Used Functions -------------------- */
  // Some operations (deleteTable, deleteTuple, ...) should not be
  // operated on system tables ("Tables", "Columns")
  RC checkIfSystemTable(const string &tableName);

  // Check if table exists
  RC checkIfTableExist(const string &tableName);

  // Check if table exists
  // if exist, get file_name, index_attr_name and index_data_file
  RC checkTableAndFetchInfo(const string &tableName,
		  string &data_file,
		  vector<string> &index_attr_name,
		  vector<string> &index_data_file);

  // save recordDescriptor for table "Tables" in recordDescriptor
  void createVectorDataForTables(vector<Attribute> &recordDescriptor);

  // save recordDescriptor for table "Columns" in recordDescriptor
  void createVectorDataForColumns(vector<Attribute> &recordDescriptor);

  // save recordDescriptor for table "Indexes" in recordDescriptor
  void createVectorDataForIndexes(vector<Attribute> &recordDescriptor);

  // convert a SingleTable object to rbfm record data format
  void prepareTablesRecord(SingleTable &table, void* data);

  // convert a SingleColumn object to rbfm record data format
  void prepareColumnsRecord(SingleColumn &column, void* data);

  // convert a SingleIndex object to rbfm record data format
  void prepareIndexesRecord(SingleIndex &index, void* data);

  // 1. append info of a column to vector ColumnsColumns<SingleColumn>
  // 2. prepareColumnsRecord()
  // 3. insert record into table "Columns"
  RC putColumnsMetaInfo(FileHandle &fileHandle,
		  	  	  	   const int table_id,
		  	  	  	   const string column_name,
		  	  	  	   const AttrType column_type,
		  	  	  	   const int column_length,
		  	  	  	   const int column_position,
		  	  	  	   const vector<Attribute> columnsAttributesVector,
		  	  	  	   vector<SingleColumn> &ColumnsColumns);

  // 1. append info of a index to vector IndexesColumns<SingleIndex>
  // 2. prepareIndexesRecord()
  // 3. insert record into table "Indexes"
  RC putIndexesMetaInfo(FileHandle &fileHandle,
  		  	  	  	  const int table_id,
  		  	  	  	  const string table_name,
  		  	  	  	  const string attr_name,
  		  	  	  	  const string file_name,
  		  	  	  	  const vector<Attribute> indexesAttributesVector,
  		  	  	  	  SingleIndex &IndexesTable);

  // 1. append info of a table to vector this->AllTables
  // 2. prepareTablesRecord()
  // 3. insert record into table "Tables"
  RC putTablesMetaInfo(FileHandle &fileHandle,
		  	  	  	  const int table_id,
		  	  	  	  const string table_name,
		  	  	  	  const string file_name,
		  	  	  	  const vector<SingleColumn> table_columns,
		  	  	  	  const vector<SingleIndex> table_indexes,
		  	  	  	  const vector<Attribute> tablesAttributesVector);

  /* -------------------- Catalog Init Functions -------------------- */
  // Init after createCatalog(), read and parse "Tables" and "Columns"
  // assort data into this->AllTables
  RC getTablesColumnsMetaInfo();

  // Should only be called from getTablesColumnsMetaInfo()
  RC scanInit(const string &tableName,
	      	   const string &conditionAttribute,
	      	   const CompOp compOp,                  // comparison type such as "<" and "="
	      	   const void *value,                    // used in the comparison
	      	   const vector<string> &attributeNames, // a list of projected attributes
	      	   RM_ScanIterator &rm_ScanIterator);

  // should only be called via getTablesColumnsMetaInfo()
  RC getSingleTable(const void* data,
		  	  	  	SingleTable &thisTable);

  // should only be called via getTablesColumnsMetaInfo()
  RC getSingleColumn(const void* data,
		  	  	  	 SingleColumn &thisColumn);

  // should only be called via getTablesColumnsMetaInfo()
  RC getSingleIndex(const void* data,
		  	  	  	 SingleIndex &thisIndex);

  /* -------------------- Other Sys Table Functions -------------------- */
  // deleteSystemTableTuple() should only be called via deleteTable()
  RC deleteSystemTableTuple(const string &tableName, const RID &rid);

  /* ------------------------- Debug Functions ------------------------- */
  void printAllTables();

protected:
  RelationManager();
  ~RelationManager();

// -------------------- New objects (Xingyu Wu) -----------------------

public:
  RecordBasedFileManager* _rbfm;

//  IndexManager* _ix;

  vector<SingleTable> AllTables;

  FileHandle TablesFileHandle;

  FileHandle ColumnsFileHandle;

  FileHandle IndexesFileHandle;

};

#endif
