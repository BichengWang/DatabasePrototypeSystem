#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm; 
    return &_rm;
}

// createCatalog() is called from upper level
// if catalog exists, initialize at rm level
RelationManager::RelationManager()
{
    if(!_rbfm)
    {
        _rbfm = RecordBasedFileManager::instance();
    }
//    if(!_ix)
//    {
//        _ix = IndexManager::instance();
//    }
    FileHandle probeFileHandle;
    if(_rbfm->openFile(TABLES_TABLE_NAME, probeFileHandle) == 0)
    {
    	_rbfm->closeFile(probeFileHandle);
    	getTablesColumnsMetaInfo();
    }
}

// note that IndexDataFileHandle is member of IXFileHandle
// but IndexesFileHandle is just an ordinary FileHandle
RelationManager::~RelationManager()
{
	_rbfm->closeFile(TablesFileHandle);
	_rbfm->closeFile(ColumnsFileHandle);
	_rbfm->closeFile(IndexesFileHandle);
}

/** (Xingyu Wu)
 * 1. Create three files named Tables, Columns amd Indexes
 * 2. Create meta Vector<Attr> info for all three tables
 * 3. Create all entries for all three tables
 * 4. Append all entries to Tables and Columns, leave Indexes blank
 * 5. Write meta info to AllTables for future reference
 *
 * nullsIndicator is always a byte and valued 0 for tables and columns,
 *     since we assume there will be no NULL attributes and values for the meta data.
 */
RC RelationManager::createCatalog()
{
	string MODE = "CreateAndOpen";
	if(this->openOrCreateAndOpenSysTables(MODE) != 0)
	{
		cerr << "In function createCatalog():" << endl;
		cerr << "Create and Open FileHandle error!" << endl;
		return -1;
	}

    vector<Attribute> tablesAttributesVector;
    createVectorDataForTables(tablesAttributesVector);
    vector<Attribute> columnsAttributesVector;
    createVectorDataForColumns(columnsAttributesVector);
    vector<Attribute> indexesAttributesVector;
    createVectorDataForIndexes(indexesAttributesVector);
    
    // Columns from file "Tables", save in file "Columns"
    vector<SingleColumn> TablesColumns;
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 1, "table-id", TypeInt, 4, 1,
    		           columnsAttributesVector, TablesColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 1, "table-name", TypeVarChar, 50, 2,
    		           columnsAttributesVector, TablesColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 1, "file-name", TypeVarChar, 50, 3,
        		       columnsAttributesVector, TablesColumns);

    // Columns from file "Columns", save in file "Columns"
    vector<SingleColumn> ColumnsColumns;
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 2, "table-id", TypeInt, 4, 1,
    		           columnsAttributesVector, ColumnsColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 2, "column-name", TypeVarChar, 50, 2,
    		           columnsAttributesVector, ColumnsColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 2, "column-type", TypeInt, 4, 3,
        		       columnsAttributesVector, ColumnsColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 2, "column-length", TypeInt, 4, 4,
    		           columnsAttributesVector, ColumnsColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 2, "column-position", TypeInt, 4, 5,
    		           columnsAttributesVector, ColumnsColumns);

    // Columns from file "Indexes", save in file "Columns"
    vector<SingleColumn> IndexColumns;
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 3, "table-id", TypeInt, 4, 1,
    				   columnsAttributesVector, IndexColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 3, "index-table-name", TypeVarChar, 50, 2,
    				   columnsAttributesVector, IndexColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 3, "index-attr-name", TypeVarChar, 50, 3,
    				   columnsAttributesVector, IndexColumns);
    this->putColumnsMetaInfo(this->ColumnsFileHandle, 3, "index-file-name", TypeVarChar, 50, 4,
    				   columnsAttributesVector, IndexColumns);

    // Info of file "Tables", save in file "Tables"
    // Info of file "Columns", save in file "Tables"
    // Info of file "Indexes", save in file "Tables"
    // this->AllTables is constructed here
    vector<SingleIndex> emptyIndex;
    this->putTablesMetaInfo(this->TablesFileHandle, 1, "Tables", "Tables",
    				  TablesColumns, emptyIndex, tablesAttributesVector);
    this->putTablesMetaInfo(this->TablesFileHandle, 2, "Columns", "Columns",
    				  ColumnsColumns, emptyIndex, tablesAttributesVector);
    this->putTablesMetaInfo(this->TablesFileHandle, 3, "Indexes", "Indexes",
    				  IndexColumns, emptyIndex, tablesAttributesVector);

    return 0;
}

RC RelationManager::deleteCatalog()
{
	// delete catalog files ONLY, see rmtest_delete_tables.cc
	int ret;
	ret = this->closeAndDestroyTableFile(TABLES_TABLE_NAME);
	ret = this->closeAndDestroyTableFile(COLUMNS_TABLE_NAME);
	ret = this->closeAndDestroyTableFile(INDEXES_TABLE_NAME);
	if(ret != 0)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}

RC RelationManager::createTable(const string &tableName,
								const vector<Attribute> &attrs)
{
    if(_rbfm->createFile(tableName))
    {
		cerr << "In function RelationManager::createTable()." << endl;
		cerr << "Table already exists: " << tableName << endl;
		return -1;
    }
    FileHandle new_tableFileHandle;
    _rbfm->openFile(tableName, new_tableFileHandle);

    vector<Attribute> tablesAttributesVector;
    createVectorDataForTables(tablesAttributesVector);
    vector<Attribute> columnsAttributesVector;
    createVectorDataForColumns(columnsAttributesVector);

    vector<SingleColumn> new_ColumnsColumns;
    vector<SingleIndex> new_Indexes;

    int new_table_id = this->AllTables.back().table_id + 1;
    int pos = 1;
    vector<Attribute>::const_iterator it;
    for(it = attrs.begin(); it != attrs.end(); it++, pos++)
    {
    	Attribute attr = *it;
    	this->putColumnsMetaInfo(this->ColumnsFileHandle,
        				   new_table_id, attr.name, attr.type, attr.length, pos,
        		           columnsAttributesVector, new_ColumnsColumns);
    }
    this->putTablesMetaInfo(this->TablesFileHandle,
    				  new_table_id, tableName, tableName,
    				  new_ColumnsColumns, new_Indexes,
    				  tablesAttributesVector);

    _rbfm->closeFile(new_tableFileHandle);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(this->checkIfSystemTable(tableName) != 0)
	{
		cerr << "In function RelationManager::deleteTable()." << endl;
		cerr << "Illegal to delete system table." << endl;
		return -1;
	}
	string table_filename;
	string index_file_name;
	RID table_rid;
	vector<SingleColumn> table_columns;
	vector<SingleIndex> table_indexes;
	int table_pos_in_vector;
	int pos = 0;
	vector<SingleTable>::const_iterator it_st;
	vector<SingleColumn>::const_iterator it_sc;
	vector<SingleIndex>::const_iterator it_si;
	for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, pos++)
	{
		SingleTable st = *it_st;
		if(st.table_name == tableName)
		{
			table_filename = st.file_name;
			table_rid = st.table_rid;
			table_columns = st.columns;
			table_indexes = st.indexes;
			table_pos_in_vector = pos;
		}
	}
	// delete in file Columns
	for(it_sc = table_columns.begin(); it_sc != table_columns.end(); it_sc++)
	{
		SingleColumn sc = *it_sc;
		this->deleteSystemTableTuple(COLUMNS_TABLE_NAME, sc.column_rid);
	}
	// delete in file Indexes, and the corresponding index files
	for(it_si = table_indexes.begin(); it_si != table_indexes.end(); it_si++)
	{
		SingleIndex si = *it_si;
		this->deleteSystemTableTuple(INDEXES_TABLE_NAME, si.index_rid);
		index_file_name = si.index_file_name;
		if(this->closeAndDestroyTableFile(index_file_name) != 0)
		{
			return -1;
		}
	}
	// delete in file Tables, and the corresponding element in this->AllTables
	this->deleteSystemTableTuple(TABLES_TABLE_NAME, table_rid);
	this->AllTables.erase(this->AllTables.begin() + table_pos_in_vector);
	if (this->closeAndDestroyTableFile(table_filename) != 0)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}

RC RelationManager::getAttribute(const string &tableName,
								 const string index_attr_name,
								 Attribute &attr)
{
	vector<SingleTable>::const_iterator it_st;
	vector<SingleColumn>::const_iterator it_sc;
	int i = 0;
//	this->printAllTables();
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, i++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    		int j = 0;
    	    for(it_sc = st.columns.begin(); it_sc != st.columns.end(); it_sc++, j++)
    	    {
    	    	SingleColumn sc = *it_sc;
    	    	if(sc.column_name == index_attr_name)
    	    	{
					attr.name = sc.column_name;
					attr.type = static_cast<AttrType>(sc.column_type);
					attr.length = sc.column_length;
					return 0;
    	    	}
    	    }
    	}
    }
	cerr << "Cannot find table or column: " << tableName << endl;
    return -1;
}

RC RelationManager::getAttributes(const string &tableName,
								  vector<Attribute> &attrs)
{
	vector<SingleTable>::const_iterator it_st;
	vector<SingleColumn>::const_iterator it_sc;
	int i = 0;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, i++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    		int j = 0;
    	    for(it_sc = st.columns.begin(); it_sc != st.columns.end(); it_sc++, j++)
    	    {
    	    	SingleColumn sc = *it_sc;
    	    	Attribute a;
    	    	a.name = sc.column_name;
    	    	a.type = static_cast<AttrType>(sc.column_type);
    	    	a.length = sc.column_length;
    	    	attrs.push_back(a);
    	    }
    	    return 0;
    	}
    }
	cerr << "Cannot find table file: " << tableName << endl;
    return -1;
}

/* insertTuple():
 * 1. insert on _rbfm first
 * 2. then insert on _ix
 */
RC RelationManager::insertTuple(const string &tableName,
								const void *data, RID &rid)
{
	IndexManager * _ix = IndexManager::instance();
	if(this->checkIfSystemTable(tableName) != 0)
	{
		cerr << "In function RelationManager::insertTuple()." << endl;
		return -1;
	}
	string data_file;
	vector<string> index_attr_name;
	vector<string> index_data_file;
    if(this->checkTableAndFetchInfo(tableName, data_file,
    		index_attr_name, index_data_file) != 0)
    {
		cerr << "In function RelationManager::insertTuple()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::insertTuple()." << endl;
		return -1;
	}
	FileHandle data_filehandle = FileHandle();
	_rbfm->openFile(tableName, data_filehandle);
    if(_rbfm->insertRecord(data_filehandle, data_Attr, data, rid) != 0)
    {
    	_rbfm->closeFile(data_filehandle);
    	return -1;
    }
    _rbfm->closeFile(data_filehandle);
	unsigned int i;
	string index_attr;
	string index_file;
	Attribute attr;
    for(i = 0; i < index_attr_name.size(); i++)
    {
    	index_attr = index_attr_name[i];
    	index_file = index_data_file[i];
    	if(DEBUG)
    	{
//    		cout << "inserting: " << index_attr << endl;
    	}
    	IXFileHandle ixFileHandle;
    	_ix->openFile(index_file, ixFileHandle);
    	// get attr for _ix->insertEntry
    	this->getAttribute(tableName, index_attr, attr);
    	// get key for _ix->insertEntry
    	void * index_data = malloc(PAGE_SIZE);
    	this->readAttribute(tableName, rid, index_attr, index_data);
//    	cout << "--------------------------------------" << endl;
//    	cout << *(float *) index_data << endl;
//    	cout << *(float *) (index_data + 1) << endl;
//    	cout << "--------------------------------------" << endl;
    	// _ix->insertEntry, free data
    	_ix->insertEntry(ixFileHandle, attr, (byte*) index_data + 1, rid);
    	if(DEBUG)
    	{
//    		cout << "inserted rid: (" << rid.pageNum << ", " << rid.slotNum << ")" << endl;
    	}
    	free(index_data);
    	_ix->closeFile(ixFileHandle);
    }
    return 0;
}

/* deleteTuple():
 * 1. delete on _ix first
 * 2. then delete on _rbfm
 */
RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	IndexManager * _ix = IndexManager::instance();
	if(this->checkIfSystemTable(tableName) != 0)
	{
		cerr << "In function RelationManager::deleteTuple()." << endl;
		return -1;
	}
	string data_file;
	vector<string> index_attr_name;
	vector<string> index_data_file;
    if(this->checkTableAndFetchInfo(tableName, data_file,
    		index_attr_name, index_data_file) != 0)
    {
		cerr << "In function RelationManager::deleteTuple()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::deleteTuple()." << endl;
		return -1;
	}
	unsigned int i;
	string index_attr;
	string index_file;
	Attribute attr;
    for(i = 0; i < index_attr_name.size(); i++)
    {
    	index_attr = index_attr_name[i];
    	index_file = index_data_file[i];
    	IXFileHandle ixFileHandle;
    	_ix->openFile(index_file, ixFileHandle);
    	// get key for _ix->deleteEntry
    	void * data = malloc(PAGE_SIZE);
    	this->readAttribute(tableName, rid, index_attr, data);
    	// get attr for _ix->deleteEntry
    	this->getAttribute(tableName, index_attr, attr);
    	// _ix->deleteEntry, free data
    	_ix->deleteEntry(ixFileHandle, attr, (byte*) data + 1, rid);
    	free(data);
    	_ix->closeFile(ixFileHandle);
    }
    FileHandle data_filehandle;
    _rbfm->openFile(data_file, data_filehandle);
    if(_rbfm->deleteRecord(data_filehandle, data_Attr, rid))
    {
    	_rbfm->closeFile(data_filehandle);
    	return -1;
    }
    _rbfm->closeFile(data_filehandle);
    return 0;
}
//RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
//{
//	IndexManager * _ix = IndexManager::instance();
//	if(this->checkIfSystemTable(tableName) != 0)
//	{
//		cerr << "In function RelationManager::deleteTuple()." << endl;
//		return -1;
//	}
//	string data_file;
//	vector<string> index_attr_name;
//	vector<string> index_data_file;
//    if(this->checkTableAndFetchInfo(tableName, data_file,
//    		index_attr_name, index_data_file) != 0)
//    {
//		cerr << "In function RelationManager::deleteTuple()." << endl;
//    	return -1;
//    }
//	vector<Attribute> data_Attr;
//	if(this->getAttributes(tableName, data_Attr) != 0)
//	{
//		cerr << "In function RelationManager::deleteTuple()." << endl;
//		return -1;
//	}
//	unsigned int i;
//	string index_attr;
//	string index_file;
//	Attribute attr;
//    for(i = 0; i < index_attr_name.size(); i++)
//    {
//    	index_attr = index_attr_name[i];
//    	index_file = index_data_file[i];
//    	IXFileHandle ixFileHandle;
//    	_ix->openFile(index_file, ixFileHandle);
//    	// get key for _ix->deleteEntry
//    	void * data = malloc(PAGE_SIZE);
//    	this->readAttribute(tableName, rid, index_attr, data);
//    	// get attr for _ix->deleteEntry
//    	this->getAttribute(tableName, index_attr, attr);
//    	// _ix->deleteEntry, free data
//    	_ix->deleteEntry(ixFileHandle, attr, data, rid);
//    	free(data);
//    	_ix->closeFile(ixFileHandle);
//    }
//    FileHandle data_filehandle;
//    _rbfm->openFile(data_file, data_filehandle);
//    if(_rbfm->deleteRecord(data_filehandle, data_Attr, rid))
//    {
//    	_rbfm->closeFile(data_filehandle);
//    	return -1;
//    }
//    _rbfm->closeFile(data_filehandle);
//    return 0;
//}

/* updateTuple():
 * 1. delete on _ix first
 * 2. then update on _rbfm
 * 3. finally, insert on _ix
 */
//RC RelationManager::updateTuple(const string &tableName,
//								const void *data, const RID &rid)
//{
//	IndexManager * _ix = IndexManager::instance();
//	cout << "In updateTuple()" << endl;
//	if(this->checkIfSystemTable(tableName) != 0)
//	{
//		cerr << "In function RelationManager::updateTuple()." << endl;
//		return -1;
//	}
//	string data_file;
//	vector<string> index_attr_name;
//	vector<string> index_data_file;
//    if(checkTableAndFetchInfo(tableName, data_file,
//    		index_attr_name, index_data_file) != 0)
//    {
//		cerr << "In function RelationManager::updateTuple()." << endl;
//    	return -1;
//    }
//	vector<Attribute> data_Attr;
//	if(getAttributes(tableName, data_Attr) != 0)
//	{
//		cerr << "In function RelationManager::updateTuple()." << endl;
//		return -1;
//	}
//	// we have to get, and delete the original index entry here
//	unsigned int i;
//	string index_attr;
//	string index_file;
//	Attribute attr;
//    for(i = 0; i < index_attr_name.size(); i++)
//    {
//    	index_attr = index_attr_name[i];
//    	index_file = index_data_file[i];
//    	cout << "Opening " << index_file << endl;
//    	IXFileHandle ixFileHandle;
//    	_ix->openFile(index_file, ixFileHandle);
//    	// get key for _ix->deleteEntry
//    	void * index_data = malloc(PAGE_SIZE);
//    	if(this->readAttribute(tableName, rid, index_attr, index_data)){
//    		cerr << "In function RelationManager::updateTuple()." << endl;
//    		cerr << "Cannot read index element from heap file (delete)." << endl;
//    		return -1;
//    	}
//    	// get attr for _ix->deleteEntry
//    	this->getAttribute(tableName, index_attr, attr);
//    	// _ix->deleteEntry, free data
//    	cout << "Before deleteEntry()" << endl;
//    	if(_ix->deleteEntry(ixFileHandle, attr, index_data, rid) != 0)
//    	{
//    		if(DEBUG)
//    		{
//				cout << "index_file = " << index_file << endl;
//				cout << "attr name = " << attr.name << " (of type " << attr.type << ")" << endl;
//				cout << "update rid = (" << rid.pageNum << ", " << rid.slotNum << ")" << endl;
//    		}
//    		cerr << "In function RelationManager::updateTuple()." << endl;
//    		cerr << "Error in update->deletion." << endl;
//    		_ix->closeFile(ixFileHandle);
//    		return -1;
//    	}
//    	free(index_data);
//    	_ix->closeFile(ixFileHandle);
//    }
//    FileHandle data_filehandle;
//    _rbfm->openFile(data_file, data_filehandle);
//    if(_rbfm->updateRecord(data_filehandle, data_Attr, data, rid) != 0)
//    {
//    	_rbfm->closeFile(data_filehandle);
//    	cerr << "In function RelationManager::updateTuple()." << endl;
//    	return -1;
//    }
//    _rbfm->closeFile(data_filehandle);
//    // then insert again
//    for(i = 0; i < index_attr_name.size(); i++)
//    {
//    	index_attr = index_attr_name[i];
//    	index_file = index_data_file[i];
//    	IXFileHandle ixFileHandle;
//    	_ix->openFile(index_file, ixFileHandle);
//    	// get key for _ix->insertEntry
//    	void * index_data2 = malloc(PAGE_SIZE);
//    	if(this->readAttribute(tableName, rid, index_attr, index_data2) !=0 ){
//    		cerr << "In function RelationManager::updateTuple()." << endl;
//    		cerr << "Cannot read index element from heap file (insert)." << endl;
//    		return -1;
//    	}
//    	// get attr for _ix->insertEntry
//    	this->getAttribute(tableName, index_attr, attr);
//    	// _ix->insertEntry, free data
//    	if(_ix->insertEntry(ixFileHandle, attr, index_data2, rid))
//    	{
//    		cerr << "In function RelationManager::updateTuple()." << endl;
//    		cerr << "Error in update->insertion." << endl;
//    		_ix->closeFile(ixFileHandle);
//    		return -1;
//    	}
//    	free(index_data2);
//    	_ix->closeFile(ixFileHandle);
//    }
//    return 0;
//}
RC RelationManager::updateTuple(const string &tableName,
								const void *data, const RID &rid)
{
	IndexManager * _ix = IndexManager::instance();
//	cout << "In updateTuple()" << endl;
	if(this->checkIfSystemTable(tableName) != 0)
	{
		cerr << "In function RelationManager::updateTuple()." << endl;
		return -1;
	}
	string data_file;
	vector<string> index_attr_name;
	vector<string> index_data_file;
    if(checkTableAndFetchInfo(tableName, data_file,
    		index_attr_name, index_data_file) != 0)
    {
		cerr << "In function RelationManager::updateTuple()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::updateTuple()." << endl;
		return -1;
	}
	// we have to get, and delete the original index entry here
	unsigned int i;
	string index_attr;
	string index_file;
	Attribute attr;
    for(i = 0; i < index_attr_name.size(); i++)
    {
    	index_attr = index_attr_name[i];
    	index_file = index_data_file[i];
//    	cout << "Opening " << index_file << endl;
    	IXFileHandle ixFileHandle;
    	_ix->openFile(index_file, ixFileHandle);
    	// get key for _ix->deleteEntry
    	void * index_data = malloc(PAGE_SIZE);
    	if(this->readAttribute(tableName, rid, index_attr, index_data)){
    		cerr << "In function RelationManager::updateTuple()." << endl;
    		cerr << "Cannot read index element from heap file (delete)." << endl;
    		return -1;
    	}
    	// get attr for _ix->deleteEntry
    	this->getAttribute(tableName, index_attr, attr);
    	// _ix->deleteEntry, free data
//    	cout << "Before deleteEntry()" << endl;
    	if(_ix->deleteEntry(ixFileHandle, attr, (byte*) index_data + 1, rid) != 0)
    	{
    		if(DEBUG)
    		{
				cout << "index_file = " << index_file << endl;
				cout << "attr name = " << attr.name << " (of type " << attr.type << ")" << endl;
				cout << "update rid = (" << rid.pageNum << ", " << rid.slotNum << ")" << endl;
    		}
    		cerr << "In function RelationManager::updateTuple()." << endl;
    		cerr << "Error in update->deletion." << endl;
    		_ix->closeFile(ixFileHandle);
    		return -1;
    	}
    	free(index_data);
    	_ix->closeFile(ixFileHandle);
    }
    FileHandle data_filehandle;
    _rbfm->openFile(data_file, data_filehandle);
    if(_rbfm->updateRecord(data_filehandle, data_Attr, data, rid) != 0)
    {
    	_rbfm->closeFile(data_filehandle);
    	cerr << "In function RelationManager::updateTuple()." << endl;
    	return -1;
    }
    _rbfm->closeFile(data_filehandle);
    // then insert again
    for(i = 0; i < index_attr_name.size(); i++)
    {
    	index_attr = index_attr_name[i];
    	index_file = index_data_file[i];
    	IXFileHandle ixFileHandle;
    	_ix->openFile(index_file, ixFileHandle);
    	// get key for _ix->insertEntry
    	void * index_data2 = malloc(PAGE_SIZE);
    	if(this->readAttribute(tableName, rid, index_attr, index_data2) !=0 ){
    		cerr << "In function RelationManager::updateTuple()." << endl;
    		cerr << "Cannot read index element from heap file (insert)." << endl;
    		return -1;
    	}
    	// get attr for _ix->insertEntry
    	this->getAttribute(tableName, index_attr, attr);
    	// _ix->insertEntry, free data
    	if(_ix->insertEntry(ixFileHandle, attr, (byte*) index_data2 + 1, rid))
    	{
    		cerr << "In function RelationManager::updateTuple()." << endl;
    		cerr << "Error in update->insertion." << endl;
    		_ix->closeFile(ixFileHandle);
    		return -1;
    	}
    	free(index_data2);
    	_ix->closeFile(ixFileHandle);
    }
    return 0;
}

// readTuple is restricted on _rbfm level
RC RelationManager::readTuple(const string &tableName,
							  const RID &rid, void *data)
{
	string data_file;
	vector<string> index_attr_name;
	vector<string> index_data_file;
    if(this->checkTableAndFetchInfo(tableName, data_file,
    		index_attr_name, index_data_file) != 0)
    {
		cerr << "In function RelationManager::readTuple()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::readTuple()." << endl;
		return -1;
	}
	if(tableName == TABLES_TABLE_NAME)
	{
		if(_rbfm->readRecord(this->TablesFileHandle, data_Attr, rid, data) != 0)
		{
			return -1;
		}
	}
	else if (tableName == COLUMNS_TABLE_NAME)
	{
		if(_rbfm->readRecord(this->ColumnsFileHandle, data_Attr, rid, data) != 0)
		{
			return -1;
		}
	}
	else if (tableName == INDEXES_TABLE_NAME)
	{
		if(_rbfm->readRecord(this->IndexesFileHandle, data_Attr, rid, data) != 0)
		{
			return -1;
		}
	}
	else
	{
		FileHandle data_filehandle;
		_rbfm->openFile(tableName, data_filehandle);
		if(_rbfm->readRecord(data_filehandle, data_Attr, rid, data) != 0)
		{
			_rbfm->closeFile(data_filehandle);
			return -1;
		}
		_rbfm->closeFile(data_filehandle);
	}
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
							   const void *data)
{
	_rbfm->printRecord(attrs, data);
	return 0;
}

// readAttribute is restricted on _rbfm level
RC RelationManager::readAttribute(const string &tableName,
								  const RID &rid,
								  const string &attributeName,
								  void *data)
{
	string data_file;
    if(this->checkIfTableExist(tableName) != 0)
    {
		cerr << "In function RelationManager::readAttribute()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::readAttribute()." << endl;
		return -1;
	}
	if(tableName == TABLES_TABLE_NAME)
	{
		if(_rbfm->readAttribute(this->TablesFileHandle, data_Attr,
								rid, attributeName, data) != 0)
		{
			return -1;
		}
	}
	else if (tableName == COLUMNS_TABLE_NAME)
	{
		if(_rbfm->readAttribute(this->ColumnsFileHandle, data_Attr,
								rid, attributeName, data) != 0)
		{
			return -1;
		}
	}
	else if (tableName == INDEXES_TABLE_NAME)
	{
		if(_rbfm->readAttribute(this->IndexesFileHandle, data_Attr,
								rid, attributeName, data) != 0)
		{
			return -1;
		}
	}
	else
	{
		FileHandle data_filehandle;
		_rbfm->openFile(tableName, data_filehandle);
		if(_rbfm->readAttribute(data_filehandle, data_Attr,
								rid, attributeName, data) != 0)
		{
			cerr << "Cannot read table in readAttribute: " << tableName << endl;
			_rbfm->closeFile(data_filehandle);
			return -1;
		}
		_rbfm->closeFile(data_filehandle);
	}
    return 0;
}

RC RelationManager::scan(const string &tableName,
					     const string &conditionAttribute,
					     const CompOp compOp,
					     const void *value,
					     const vector<string> &attributeNames,
					     RM_ScanIterator &rm_ScanIterator)
{
	string data_file;
    if(this->checkIfTableExist(tableName) != 0)
    {
		cerr << "In function RelationManager::scan()." << endl;
    	return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::scan()." << endl;
		return -1;
	}
	if(tableName == TABLES_TABLE_NAME)
	{
		rm_ScanIterator.open(this->TablesFileHandle, data_Attr,
							 conditionAttribute, compOp, value, attributeNames);
	}
	else if(tableName == COLUMNS_TABLE_NAME)
	{
		rm_ScanIterator.open(this->ColumnsFileHandle, data_Attr,
							 conditionAttribute, compOp, value, attributeNames);
	}
	else if(tableName == INDEXES_TABLE_NAME)
	{
		rm_ScanIterator.open(this->IndexesFileHandle, data_Attr,
							 conditionAttribute, compOp, value, attributeNames);
	}
	else
	{
		FileHandle data_filehandle;
		_rbfm->openFile(tableName, data_filehandle);
		rm_ScanIterator.open(data_filehandle, data_Attr,
							 conditionAttribute, compOp, value, attributeNames);
	}
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName,
								  const string &attributeName)
{
    return -1;
}

RC RelationManager::addAttribute(const string &tableName,
								 const Attribute &attr)
{
    return -1;
}

/* createIndex():
 * 1. create the index table file, open it
 * 2. get the name of the heap file, open it
 * 3. update this->AllTables
 * 4. iterate through the heap file to get (data, rid)
 * 5. insert all these composite keys to the index table file
 */
RC RelationManager::createIndex(const string &tableName,
								const string &attributeName)
{
	IndexManager * _ix = IndexManager::instance();
	if(this->checkIfTableExist(tableName) != 0)
	{
		cerr << "In function RelationManager::createIndex()." << endl;
		cerr << "This table does not exist: " << tableName << endl;
		return -1;
	}
	string index_table_name = tableName + "_" + attributeName;
    if(_ix->createFile(index_table_name) != 0)
    {
		cerr << "In function RelationManager::createIndex()." << endl;
		cerr << "This index table already exists: " << index_table_name << endl;
		return -1;
    }
	vector<Attribute> data_Attr;
	if(this->getAttributes(tableName, data_Attr) != 0)
	{
		cerr << "In function RelationManager::insertTuple()." << endl;
		return -1;
	}
    IXFileHandle ixFileHandle;
    if(_ix->openFile(index_table_name, ixFileHandle) != 0){
    	cerr << "In function RelationManager::createIndex()." << endl;
    	cerr << "cannot open index table." << endl;
    	return -1;
    }

    vector<Attribute> tablesAttributesVector;
    this->createVectorDataForTables(tablesAttributesVector);
    vector<Attribute> indexesAttributesVector;
    this->createVectorDataForIndexes(indexesAttributesVector);

    vector<SingleIndex> thisIndexes;
    SingleIndex new_Index;

    int pos = 0;
    vector<SingleTable>::const_iterator it_st;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, pos++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    		this->putIndexesMetaInfo(this->IndexesFileHandle,
    				st.table_id, tableName, attributeName,
    				index_table_name, indexesAttributesVector,
    				new_Index);
    		break;
    	}
    }
    this->AllTables[pos].indexes.push_back(new_Index);

    Attribute attr;
    this->getAttribute(tableName, attributeName, attr);
//    cout << "created attr of type " << attr.type << endl;
    RM_ScanIterator rmsi_heap_file;
    vector<string> projected_attrs_table;
    RID rid;
    projected_attrs_table.push_back(attributeName);
    if(scan(tableName, "", NO_OP, NULL,
    		projected_attrs_table, rmsi_heap_file) != 0)
    {
    	return -1;
    }
    void *returnedData_heap_file = malloc(PAGE_SIZE);
    int count = 0;
    while(rmsi_heap_file.getNextTuple(rid, returnedData_heap_file) != RM_EOF)
    {
    	void * key = malloc(PAGE_SIZE);
    	this->readAttribute(tableName, rid, attributeName, key);
//    	cout << "--------------------------------------" << endl;
//    	cout << *(float *) key << endl;
//    	cout << *(float *) (key + 1) << endl;
//    	cout << "--------------------------------------" << endl;
    	_ix->insertEntry(ixFileHandle, attr, (byte*) key + 1, rid);
        count++;
        free(key);
    }
    rmsi_heap_file.close();
    _ix->closeFile(ixFileHandle);
    free(returnedData_heap_file);

	return 0;
}

/* destroyIndex()
 * 1. destroy the index file
 * 2. erase the corresponding SingleIndex member in this->AllTables
 */
RC RelationManager::destroyIndex(const string &tableName,
								 const string &attributeName)
{
	string index_table_name = tableName + "_" + attributeName;
	if(this->closeAndDestroyTableFile(index_table_name) != 0)
	{
		cerr << "cannot destroy index file: " << index_table_name << endl;
		return -1;
	}
	else
	{
		return 0;
	}
    int pos_st = 0;
    int pos_si = 0;
    bool if_find = false;
    vector<SingleTable>::const_iterator it_st;
    vector<SingleIndex>::const_iterator it_si;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, pos_st++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    	    for(it_si = st.indexes.begin(); it_si != st.indexes.end(); it_si++, pos_si++)
    	    {
    	    	SingleIndex si = *it_si;
    	    	if(si.index_attr_name == attributeName)
    	    	{
    	    		if_find = true;
    	    		break;
    	    	}
    	    }
    	    cerr << "Cannot find attr: " << attributeName << endl;
    	    return -1;
    	}
    }
    if(!if_find)
    {
		cerr << "Cannot find table: " << tableName << endl;
		return -1;
    }
    this->AllTables[pos_st].indexes.erase(
    		this->AllTables[pos_st].indexes.begin() + pos_si);
    return 0;
}

RC RelationManager::indexScan(const string &tableName,
							  const string &attributeName,
							  const void *lowKey,
							  const void *highKey,
							  bool lowKeyInclusive,
							  bool highKeyInclusive,
							  RM_IndexScanIterator &rm_IndexScanIterator)
{
//	cout << "In RelationManager::indexScan" << endl;
	string index_table_name = tableName + "_" + attributeName;

	// TODO: warning need destroy
//	IXFileHandle ixFileHandle;


	IndexManager::instance()->openFile(index_table_name, rm_IndexScanIterator.ixfileHandle);

//	if (rm_IndexScanIterator.ixfileHandle.isOpen()) {
//		cout << "right 1 file handle open" << endl;
//	}

	Attribute attr;
	this->getAttribute(tableName, attributeName, attr);


	IndexManager::instance()->scan(rm_IndexScanIterator.ixfileHandle,
			  attr, lowKey, highKey, lowKeyInclusive,
			  highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);

//	rm_IndexScanIterator.initParam( attr,
//			lowKey, highKey, lowKeyInclusive, highKeyInclusive);

//	if (rm_IndexScanIterator.ixfileHandle.isOpen()) {
//		cout << "right 2 handle is open" << endl;
//	}

	return 0;
}

// ------------------- New functions (Xingyu Wu) ----------------------
void RelationManager::createVectorDataForTables(vector<Attribute> &recordDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);
}

void RelationManager::createVectorDataForColumns(vector<Attribute> &recordDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt; 
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}

void RelationManager::createVectorDataForIndexes(vector<Attribute> &recordDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "index-table-name"; // same as table-name
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "index-attr-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "index-file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);
}

RC RelationManager::closeAndDestroyTableFile(const string &tableName)
{
	if(_rbfm->destroyFile(tableName) != 0)
	{
		return -1;
	}
	return 0;
}

void RelationManager::prepareTablesRecord(SingleTable &table, void* data)
{
	int offset = 0;
	byte nullsIndicator = 0;

	memcpy((byte *) data + offset, &nullsIndicator, sizeof(byte));
	offset += sizeof(byte);

	int s_table_id = table.table_id;
	memcpy((byte *) data + offset, &s_table_id, sizeof(int));
	offset += sizeof(int);

	int s_table_name_len = table.table_name.length();
	memcpy((byte *) data + offset, &s_table_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			table.table_name.c_str(),
			s_table_name_len);
	offset += s_table_name_len;

	int s_file_name_len = table.file_name.length();
	memcpy((byte *) data + offset, &s_file_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			table.file_name.c_str(),
			s_file_name_len);
	offset += s_file_name_len;
}

void RelationManager::prepareColumnsRecord(SingleColumn &column, void* data)
{
	int offset = 0;
	byte nullsIndicator = 0;

	memcpy((byte *) data + offset, &nullsIndicator, sizeof(byte));
	offset += sizeof(byte);

	int s_table_id = column.table_id;
	memcpy((byte *) data + offset, &s_table_id, sizeof(AttrType));
	offset += sizeof(AttrType);

	int s_column_name_len = column.column_name.length();
	memcpy((byte *) data + offset, &s_column_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			column.column_name.c_str(),
			s_column_name_len);
	offset += s_column_name_len;

	int s_column_type = column.column_type;
	memcpy((byte *) data + offset, &s_column_type, sizeof(int));
	offset += sizeof(int);

	int s_column_length = column.column_length;
	memcpy((byte *) data + offset, &s_column_length, sizeof(int));
	offset += sizeof(int);

	int s_column_position = column.column_position;
	memcpy((byte *) data + offset, &s_column_position, sizeof(int));
	offset += sizeof(int);
}

void RelationManager::prepareIndexesRecord(SingleIndex &index, void* data)
{
	int offset = 0;
	byte nullsIndicator = 0;

	memcpy((byte *) data + offset, &nullsIndicator, sizeof(byte));
	offset += sizeof(byte);

	int s_table_id = index.table_id;
	memcpy((byte *) data + offset, &s_table_id, sizeof(AttrType));
	offset += sizeof(AttrType);

	int s_index_table_name_len = index.index_table_name.length();
	memcpy((byte *) data + offset, &s_index_table_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			index.index_table_name.c_str(),
			s_index_table_name_len);
	offset += s_index_table_name_len;

	int s_index_attr_name_len = index.index_attr_name.length();
	memcpy((byte *) data + offset, &s_index_attr_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			index.index_attr_name.c_str(),
			s_index_attr_name_len);
	offset += s_index_attr_name_len;

	int s_index_file_name_len = index.index_file_name.length();
	memcpy((byte *) data + offset, &s_index_file_name_len, sizeof(int));
	offset += sizeof(int);
	memcpy((byte *) data + offset,
			index.index_file_name.c_str(),
			s_index_file_name_len);
	offset += s_index_file_name_len;
}

RC RelationManager::putColumnsMetaInfo(FileHandle &fileHandle,
		  	  	  	   	   	   	   	  const int table_id,
		  	  	  	   	   	   	   	  const string column_name,
		  	  	  	   	   	   	   	  const AttrType column_type,
		  	  	  	   	   	   	   	  const int column_length,
		  	  	  	   	   	   	   	  const int column_position,
		  	  	  	   	   	   	   	  const vector<Attribute> columnsAttributesVector,
		  	  	  	   	   	   	   	  vector<SingleColumn> &ColumnsColumns)
{
	void* data = malloc(PAGE_SIZE);
	RID rid;
	rid.pageNum = -1;
	rid.slotNum = -1;
    SingleColumn columns_column_position = SingleColumn(
    		table_id, column_name, column_type, column_length,
    		column_position, rid);
    this->prepareColumnsRecord(columns_column_position, data);
    _rbfm->insertRecord(fileHandle, columnsAttributesVector, data, rid);
    columns_column_position.column_rid = rid;
    ColumnsColumns.push_back(columns_column_position);
    free(data);
    return 0;
}

RC RelationManager::putIndexesMetaInfo(FileHandle &fileHandle,
									   const int table_id,
									   const string table_name,
									   const string attr_name,
									   const string file_name,
									   const vector<Attribute> indexesAttributesVector,
									   SingleIndex &IndexesTable)
{
	void* data = malloc(PAGE_SIZE);
	RID rid;
	rid.pageNum = -1;
	rid.slotNum = -1;
	IndexesTable = SingleIndex(
			table_id, table_name, attr_name, file_name, rid);
	this->prepareIndexesRecord(IndexesTable, data);
	_rbfm->insertRecord(fileHandle, indexesAttributesVector, data, rid);
	IndexesTable.index_rid = rid;
	free(data);
	return 0;
}

RC RelationManager::putTablesMetaInfo(FileHandle &fileHandle,
		  	  	  	  	  	  	  	  const int table_id,
		  	  	  	  	  	  	  	  const string table_name,
		  	  	  	  	  	  	  	  const string file_name,
		  	  	  	  	  	  	  	  const vector<SingleColumn> table_columns,
		  	  	  	  	  	  	  	  const vector<SingleIndex> table_indexes,
		  	  	  	  	  	  	  	  const vector<Attribute> tablesAttributesVector)
{
	void* data = malloc(PAGE_SIZE);
	RID rid;
	rid.pageNum = -1;
	rid.slotNum = -1;
    SingleTable TablesTable = SingleTable(
    		table_id, table_name, file_name, rid,
    		table_columns, table_indexes);
    this->prepareTablesRecord(TablesTable, data);
    _rbfm->insertRecord(fileHandle, tablesAttributesVector, data, rid);
    TablesTable.table_rid = rid;
    this->AllTables.push_back(TablesTable);
    free(data);
    return 0;
}

RC RelationManager::getTablesColumnsMetaInfo()
{
	string MODE = "OpenOnly";
	if(openOrCreateAndOpenSysTables(MODE) != 0)
	{
		cerr << "In function getTablesColumnsMetaInfo():" << endl;
		cerr << "Opening FileHandle error!" << endl;
		return -1;
	}

    vector<Attribute> tablesAttributesVector;
    this->createVectorDataForTables(tablesAttributesVector);
    vector<Attribute> columnsAttributesVector;
    this->createVectorDataForColumns(columnsAttributesVector);
    vector<Attribute> indexesAttributesVector;
    this->createVectorDataForIndexes(indexesAttributesVector);

    RID rid;
    vector<SingleColumn> emptyColumns;
    vector<SingleIndex> emptyIndexes;

    // First we start with "Tables":
    RM_ScanIterator rmsi_table;
    vector<string> projected_attrs_table;
    for(unsigned int i = 0; i < tablesAttributesVector.size(); i++)
    {
    	projected_attrs_table.push_back(tablesAttributesVector[i].name);
    }
    if(scanInit(TABLES_TABLE_NAME, "", NO_OP, NULL,
    		projected_attrs_table, rmsi_table) != 0)
    {
    	return -1;
    }
    void *returnedData_Tables = malloc(PAGE_SIZE);
    SingleTable thisTable(-1, "", "", rid, emptyColumns, emptyIndexes);
    int count = 0;
    while(rmsi_table.getNextTuple(rid, returnedData_Tables) != RM_EOF)
    {
    	this->getSingleTable(returnedData_Tables, thisTable);
    	thisTable.table_rid = rid;
    	this->AllTables.push_back(thisTable);
        count++;
    }
    rmsi_table.close();
    free(returnedData_Tables);

    // Continue with Columns, push to SingleTable.columns
    RM_ScanIterator rmsi_columns;
    vector<string> projected_attrs_columns;
    for(unsigned int i = 0; i < columnsAttributesVector.size(); i++)
    {
    	projected_attrs_columns.push_back(columnsAttributesVector[i].name);
    }
    if(this->scanInit(COLUMNS_TABLE_NAME, "", NO_OP, NULL,
    		 projected_attrs_columns, rmsi_columns) != 0)
    {
    	return -1;
    }
    void *returnedData_Columns = malloc(PAGE_SIZE);
    count = 0;
    SingleColumn thisColumn(-1, "", -1, -1, -1, rid);
    vector<SingleColumn> Columns;
    vector<SingleTable>::const_iterator it_st;
    int i;
    while(rmsi_columns.getNextTuple(rid, returnedData_Columns) != RM_EOF)
    {
    	this->getSingleColumn(returnedData_Columns, thisColumn);
    	thisColumn.column_rid = rid;
    	i = 0;
        for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, i++)
        {
        	SingleTable st = *it_st;
        	if(st.table_id == thisColumn.table_id)
        	{
        		this->AllTables[i].columns.push_back(thisColumn);
        	}
        }
        count++;
    }
    rmsi_columns.close();
    free(returnedData_Columns);

    // Last, process Indexes, push to SingleTable.indexes
    RM_ScanIterator rmsi_indexes;
    vector<string> projected_attrs_indexes;
    for(unsigned int i = 0; i < indexesAttributesVector.size(); i++)
    {
    	projected_attrs_indexes.push_back(indexesAttributesVector[i].name);
    }
    if(this->scanInit(INDEXES_TABLE_NAME, "", NO_OP, NULL,
    		 projected_attrs_indexes, rmsi_indexes) != 0)
    {
    	return -1;
    }
    void *returnedData_Indexes = malloc(PAGE_SIZE);
    count = 0;
    SingleIndex thisIndex(-1, "", "", "", rid);
    vector<SingleIndex> Indexes;
    while(rmsi_indexes.getNextTuple(rid, returnedData_Indexes) != RM_EOF)
    {
    	this->getSingleIndex(returnedData_Indexes, thisIndex);
    	thisIndex.index_rid = rid;
    	i = 0;
        for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, i++)
        {
        	SingleTable st = *it_st;
        	if(st.table_id == thisIndex.table_id)
        	{
        		this->AllTables[i].indexes.push_back(thisIndex);
        	}
        }
        count++;
    }
    rmsi_indexes.close();
    free(returnedData_Indexes);

//    this->printAllTables();

    return 0;
}

RC RelationManager::scanInit(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	vector<Attribute> data_Attr;
	if(tableName == TABLES_TABLE_NAME)
	{
		createVectorDataForTables(data_Attr);
	    rm_ScanIterator.open(this->TablesFileHandle, data_Attr,
	    					 conditionAttribute, compOp, value,
	    					 attributeNames);
	}
	else if(tableName == COLUMNS_TABLE_NAME)
	{
		createVectorDataForColumns(data_Attr);
		rm_ScanIterator.open(this->ColumnsFileHandle, data_Attr,
		    				 conditionAttribute, compOp, value,
		    				 attributeNames);
	}
	else if(tableName == INDEXES_TABLE_NAME)
	{
		createVectorDataForIndexes(data_Attr);
		rm_ScanIterator.open(this->IndexesFileHandle, data_Attr,
		    				 conditionAttribute, compOp, value,
		    				 attributeNames);
	}
	else
	{
		return -1;
	}
    return 0;
}

RC RelationManager::checkIfSystemTable(const string &tableName)
{
	if((tableName == TABLES_TABLE_NAME) ||
	   (tableName == COLUMNS_TABLE_NAME) ||
	   (tableName == INDEXES_TABLE_NAME))
	{
		if(DEBUG)
		{
			cerr << "Operation to system tables." << endl;
		}
		return -1;
	}
	else
	{
		return 0;
	}
}

RC RelationManager::checkIfTableExist(const string &tableName)
{
	vector<SingleTable>::const_iterator it_st;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    		return 0;
    	}
    }
	cerr << "Cannot find table file: " << tableName << endl;
    return -1;
}

RC RelationManager::checkTableAndFetchInfo(const string &tableName,
		string &data_file,
		vector<string> &index_attr_name,
		vector<string> &index_data_file)
{
	vector<SingleTable>::const_iterator it_st;
	vector<SingleIndex>::const_iterator it_si;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++)
    {
    	SingleTable st = *it_st;
    	if(st.table_name == tableName)
    	{
    		data_file = st.file_name;
    		for(it_si = st.indexes.begin(); it_si != st.indexes.end(); it_si++)
    		{
    			SingleIndex si = *it_si;
				index_attr_name.push_back(si.index_attr_name);
				index_data_file.push_back(si.index_file_name);
    		}
    		return 0;
    	}
    }
	cerr << "Cannot find table file and fetch its info: " << tableName << endl;
    return -1;
}

RC RelationManager::deleteSystemTableTuple(const string &tableName, const RID &rid)
{
	if(this->checkIfSystemTable(tableName) != 0)
	{
		cerr << "In function RelationManager::deleteSystemTableTuple()." << endl;
		return -1;
	}
	vector<Attribute> data_Attr;
	if(tableName == TABLES_TABLE_NAME)
	{
		this->createVectorDataForTables(data_Attr);
	    if(_rbfm->deleteRecord(this->TablesFileHandle, data_Attr, rid))
	    {
	    	return -1;
	    }
	}
	else if(tableName == COLUMNS_TABLE_NAME)
	{
		this->createVectorDataForColumns(data_Attr);
	    if(_rbfm->deleteRecord(this->ColumnsFileHandle, data_Attr, rid))
	    {
	    	return -1;
	    }
	}
	else if(tableName == INDEXES_TABLE_NAME)
	{
		this->createVectorDataForIndexes(data_Attr);
	    if(_rbfm->deleteRecord(this->IndexesFileHandle, data_Attr, rid))
	    {
	    	return -1;
	    }
	}
	else
	{
		return -1;
	}
    return 0;
}

RC RelationManager::getSingleTable(const void* data,
		  	  	  				   SingleTable &thisTable)
{
	// Assume length of nullByteIndicator is 1 byte
	// for all table records and column records
	int offset = 1;

	int table_id;
	memcpy(&table_id, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisTable.table_id = table_id;

	int table_name_len;
	memcpy(&table_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* table_name = (char*) malloc(50); // VarChar no more than 50
	memset(table_name, 0, 50);
	memcpy(table_name, (byte*) data + offset, table_name_len);
	offset += table_name_len;
	thisTable.table_name = table_name;

	int file_name_len;
	memcpy(&file_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* file_name = (char*) malloc(50); // VarChar no more than 50
	memset(file_name, 0, 50);
	memcpy(file_name, (byte*) data + offset, file_name_len);
	offset += file_name_len;
	thisTable.file_name = file_name;

	free(table_name);
	free(file_name);

	return 0;
}

RC RelationManager::getSingleColumn(const void* data,
		  	  	  	 	 	 	    SingleColumn &thisColumn)
{
	// Assume length of nullByteIndicator is 1 byte
	// for all table records and column records
	int offset = 1;

	int table_id;
	memcpy(&table_id, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisColumn.table_id = table_id;

	int column_name_len;
	memcpy(&column_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* column_name = (char*) malloc(50); // VarChar no more than 50
	memset(column_name, 0, 50);
	memcpy(column_name, (byte*) data + offset, column_name_len);
	offset += column_name_len;
	thisColumn.column_name = column_name;

	int column_type;
	memcpy(&column_type, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisColumn.column_type = column_type;

	int column_length;
	memcpy(&column_length, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisColumn.column_length = column_length;

	int column_position;
	memcpy(&column_position, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisColumn.column_position = column_position;

	free(column_name);

	return 0;
}

RC RelationManager::getSingleIndex(const void* data,
		  	  	  	 	 	 	    SingleIndex &thisIndex)
{
	// Assume length of nullByteIndicator is 1 byte
	// for all table records and column records
	int offset = 1;

	int table_id;
	memcpy(&table_id, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	thisIndex.table_id = table_id;

	int table_name_len;
	memcpy(&table_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* table_name = (char*) malloc(50); // VarChar no more than 50
	memset(table_name, 0, 50);
	memcpy(table_name, (byte*) data + offset, table_name_len);
	offset += table_name_len;
	thisIndex.index_table_name = table_name;

	int attr_name_len;
	memcpy(&attr_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* attr_name = (char*) malloc(50); // VarChar no more than 50
	memset(attr_name, 0, 50);
	memcpy(attr_name, (byte*) data + offset, attr_name_len);
	offset += attr_name_len;
	thisIndex.index_attr_name = attr_name;

	int file_name_len;
	memcpy(&file_name_len, (byte*) data + offset, sizeof(int));
	offset += sizeof(int);
	char* file_name = (char*) malloc(50); // VarChar no more than 50
	memset(file_name, 0, 50);
	memcpy(file_name, (byte*) data + offset, file_name_len);
	offset += file_name_len;
	thisIndex.index_file_name = file_name;

	free(table_name);
	free(attr_name);
	free(file_name);

	return 0;
}

RC RelationManager::openOrCreateAndOpenSysTables(const string &operation)
{
	int RC;
	if(operation == "OpenOnly")
	{
	    RC = _rbfm->openFile(TABLES_TABLE_NAME, this->TablesFileHandle);
	    RC = _rbfm->openFile(COLUMNS_TABLE_NAME, this->ColumnsFileHandle);
	    RC = _rbfm->openFile(INDEXES_TABLE_NAME, this->IndexesFileHandle);
	    return RC;
	}
	else if(operation == "CreateAndOpen")
	{
	    RC = _rbfm->createFile(TABLES_TABLE_NAME);
	    RC = _rbfm->openFile(TABLES_TABLE_NAME, this->TablesFileHandle);
	    RC = _rbfm->createFile(COLUMNS_TABLE_NAME);
	    RC = _rbfm->openFile(COLUMNS_TABLE_NAME, this->ColumnsFileHandle);
	    RC = _rbfm->createFile(INDEXES_TABLE_NAME);
	    RC = _rbfm->openFile(INDEXES_TABLE_NAME, this->IndexesFileHandle);
	    return RC;
	}
	else
	{
		cerr << "MODE for openOrCreateAndOpenSysTables() incorrect." << endl;
		return -1;
	}
}

// debug function
void RelationManager::printAllTables()
{
	vector<SingleTable>::const_iterator it_st;
	vector<SingleColumn>::const_iterator it_sc;
	vector<SingleIndex>::const_iterator it_si;
	int i = 0;
    for(it_st = this->AllTables.begin(); it_st != this->AllTables.end(); it_st++, i++)
    {
    	SingleTable st = *it_st;
    	cout << st.table_id << '\t';
    	cout << st.table_name << '\t';
    	cout << st.file_name << '\n';
    	int j = 0;
    	for(it_sc = st.columns.begin(); it_sc != st.columns.end(); it_sc++, j++)
    	{
    	    SingleColumn sc = *it_sc;
    	    cout << '\t' << "Column " << j << ": ";
    	    cout << sc.table_id << '\t';
    	    cout << sc.column_name << '\t';
    	    cout << sc.column_type << '\t';
    	    cout << sc.column_length << '\t';
    	    cout << sc.column_position << '\n';
    	}
    	j = 0;
    	for(it_si = st.indexes.begin(); it_si != st.indexes.end(); it_si++, j++)
    	{
    	    SingleIndex si = *it_si;
    	    cout << '\t' << "Index " << j << ": ";
    	    cout << si.table_id << '\t';
    	    cout << si.index_table_name << '\t';
    	    cout << si.index_attr_name << '\t';
    	    cout << si.index_file_name << '\n';
    	}
    }
}

// ------------------- Two Iterators (Xingyu Wu) ----------------------
RM_ScanIterator::RM_ScanIterator(){
//	this->rbfm_ScanIterator = RBFM_ScanIterator();
}

RM_ScanIterator::~RM_ScanIterator(){
}

RC RM_ScanIterator::open(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp,
		const void * value, const vector<string> &attributeNames)
{
	int ret = this->rbfm_ScanIterator.createRBFM_ScanIterator(
				fileHandle, recordDescriptor,
				conditionAttribute, compOp, value, attributeNames);
	return ret;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	int ret = this->rbfm_ScanIterator.getNextRecord(rid, data);
	if(ret)
	{
		return RM_EOF;
	}
	else
	{
		return 0;
	}
}

RC RM_ScanIterator::close()
{
	int ret = this->rbfm_ScanIterator.close();
	return ret;
}

RM_IndexScanIterator::RM_IndexScanIterator(){
//	this->ix_ScanIterator
}

RM_IndexScanIterator::~RM_IndexScanIterator(){
//	this->close();
//	this->close();
}

RC RM_IndexScanIterator::initParam(
  		const Attribute attr, const void* lowkey, const void* highKey,
  		bool lowKeyInclusive, bool highKeyInclusive)
{
//	cout << "RM_IndexScanIterator::open" << endl;
//	if(lowkey){
//		if(attr.type == 1){
//			cout << "lowKey " << *(float*) (lowkey) << endl;
//		}
//	}
	int ret = this->ix_ScanIterator.init(this->ixfileHandle, attr,
				lowkey, highKey, lowKeyInclusive, highKeyInclusive);
	return ret;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *data)
{
//	cout << "In RM_IndexScanIterator::getNextEntry" << endl;
	int ret = this->ix_ScanIterator.getNextEntry(rid, data);
	if(ret)
	{
//		cout << "Encountered RM_EOF" << endl;
		return RM_EOF;
	}
	else
	{
		return 0;
	}
}

RC RM_IndexScanIterator::close()
{
//	this->ixfileHandle.closeFileHandle();
	IndexManager::instance()->closeFile(this->ixfileHandle);
	int ret = this->ix_ScanIterator.close();
	return ret;
}
