#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <cstring>
#include <cmath>
#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct {
	unsigned pageNum;    // page number
	unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
	TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
	EQ_OP = 0, // no condition// =
	LT_OP,      // <
	LE_OP,      // <=
	GT_OP,      // >
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;

/********************************************************************************
 The scan iterator is NOT required to be implemented for the part 1 of the project
 ********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();
/**
 * indeed the scan iterator of rbfm need used inner class to implement
 * but limited time, I just used calling function to finish.
 */
class RBFM_ScanIterator {
public:
	RBFM_ScanIterator(){
		this->value = malloc(PAGE_SIZE);
		this->valueNull = false;
	};

	~RBFM_ScanIterator(){
		free(this->value);
	};

	// Never keep the results in the memory. When getNextRecord() is called,
	// a satisfying record needs to be fetched from the file.
	// "data" follows the same format as RecordBasedFileManager::insertRecord().
	RC getNextRecord(RID &rid, void * data);

	RC next(void * data);

	RC hasNext(bool &hasNext);

	RC createRBFM_ScanIterator(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute, const CompOp compOp,
			const void * value, const vector<string> &attributeNames);

	RC close();

private:
	RC compare(const void * compareAttribute, bool &compareResult);
	RC getNextRid();
	RC getData(void * data);

	// hold fileHandle
	FileHandle fileHandle;

	// hold recordDescriptor
	vector<Attribute> recordDescriptor;

	// compareAttributeName
	string conditionAttributeName;

	// compareOperator
	CompOp compOp;

	// all other attributes' Name
	vector<string> attributeNames;

	/* getValueType*/
	AttrType valueType;

	// what is value means, include NullIndicator or not?
	// compareValue
	void * value;
	bool valueNull = false;
	// hold nextRid, which is the next record, but may not be available for comparison
	RID nextRid;
	RID currentRid;

	// exitNext is a flag signs if hasNext()
	bool exitNext = true;
};

class RecordBasedFileManager {
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
	//  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
	//     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
	//     Each bit represents whether each field value is null or not.
	//     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
	//     If k-th bit from the left is set to 0, k-th field contains non-null values.
	//     If there are more than 8 fields, then you need to find the corresponding byte first,
	//     then find a corresponding bit inside that byte.
	//  2) Actual data is a concatenation of values of the attributes.
	//  3) For Int and Real: use 4 bytes to store the value;
	//     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
	// For example, refer to the Q8 of Project 1 wiki page.
	RC insertRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const void *data,
			RID &rid);

	RC readRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid,
			void *data);

	// This method will be mainly used for debugging/testing.
	// The format is as follows:
	// field1-name: field1-value  field2-name: field2-value ... \n
	// (e.g., age: 24  height: 6.1  salary: 9000
	//        age: NULL  height: 7.5  salary: 7500)
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/******************************************************************************************************************************************************************
	 IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
	 ******************************************************************************************************************************************************************/
	/* TODO: manage 2048 available page at a available slot record page */
	RC deleteRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the RID does not change after an update
	RC updateRecord(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const void *data,
			const RID &rid);

	RC readAttribute(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor, const RID &rid,
			const string &attributeName, void * data);


	// Scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
			const void * value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);

	RC getNextValidRid(FileHandle fileHandle, RID &rid, bool &exitNextRid);

public:
	/* created by Bicheng Wang */
	RC readAttributes(FileHandle &fileHandle,
				const vector<Attribute> &recordDescriptor, const RID &rid,
				const vector<string> &attributeNames, void * data);

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;

	/* create by Bicheng */
	/* TODO: manage 1 index page + 2047 available page
	 * index page record all available space in every page in their group
	 * insert/delete/update record in one page need also update index page
	 * append new page need to check index page and every 2048 page need make now index page
	 * index page structure: from 1 to 2047 short int save the available space
	 * 2048 short int save how many page the group have now
	 */

	// hold PagedFileManager
	PagedFileManager * pagedFileManager;

	/* data management */
	RC getAttributeLocationInFormatData(const void * formatData,
			const unsigned short int &attributePosition, unsigned short int &startIndex,
			unsigned short int &offset);

	// get first rid in record data
	RC getFirstRidInFormatData(const void * data,
			const unsigned short int dataSize, RID &rid);

	// set first rid in format data
	RC setFirstRidInFormatData(void * formatData, const RID &firstRid,
			const unsigned short int &dataSize);

	// get attribute in format data
	RC getAttributeInFormatData(const void * formatData,
			const unsigned short int &attributePosition, void * attribute,
			unsigned short int &attributeSize);

	// decide if format data is a link rid
	RC isFormatDataLink(const void * formatData,
			const unsigned short int dataSize, bool &isCurrentDataLink);

	// decide if this format data is linked by another
	RC isFormatDataLinked(const void * formatData,
			const unsigned short int dataSize, bool &isCurrentDataLinked);

	// format data
	RC data2FormatDataNCalculateDataSize(
			const vector<Attribute> &recordDescriptor, const void *data,
			void *formatData, unsigned short int &dataSize);

	// format data
	RC data2FormatData(const vector<Attribute> &recordDescriptor,
			const void *data, void *formatData);

	// deFormat data
	RC formatData2Data(const vector<Attribute> &recordDescriptor,
			const void * formatData, void * data,
			const unsigned short int &formatDataSize);

	/* record management in page level */
	//
	RC insertRecordToPageAtNextSlot(void * pageData, const void * formatData,
			const unsigned short int &dataSize,
			const unsigned short int &currentSlotNum);

	// insert record to page
	RC insertRecordToPage(void * pageData, const void *data,
			const unsigned short int &dataSize, unsigned &slotNum);

	// delete record from page
	RC deleteRecordInPage(void * pageData, const unsigned short int slotNum);

	// update record to page at slot number
	RC updateRecordToPageAtSlotNumber(void * pageData, const void * formatData,
			const unsigned short int &dataSize,
			const unsigned short int &currentSlotNum);

	// get format data in page
	RC getFormatDataInPage(const void * pageData,
			const unsigned short int &slotNum, void * data,
			unsigned short int &dataSize);

	// get attribute in page
	RC getAttributeInPage(const void * pageData,
			const unsigned short int &slotNum,
			const unsigned short int &attributePosition, void * attribute,
			unsigned short int &attributeSize);

	/* slot management in page level */
	// get page information by page data from first slot
	RC getPageInfoByData(const void * pageData,
			unsigned short int &firstAvailableSlotIndex,
			unsigned short int &slotNum);

	// get page information from first slot
	RC getPageInfo(FileHandle &fileHandle, PageNum currentPageIndex,
			unsigned short int &firstAvailableSlotIndex,
			unsigned short int &slotNumRecord);

	// update page information at first slot
	RC updatePageInfo(void * pageData,
			unsigned short int firstAvailableSlotIndex,
			unsigned short int currentSlotNum);

	// initial page information at first slot
	RC initPageInfo(void * pageData);

	// insert new slot information
	RC insertSlotInfo(void *pageData, const unsigned short int startIndex,
			const unsigned short int offsetNumber,
			const unsigned short int currentSlotNum);

	// get the following slot information
	RC getSlotInfo(const void * pageData, const unsigned short int slotNum,
			unsigned short int &startSlotIndex,
			unsigned short int &occupiedSlotNum);

	// update following slot information
	RC updateSlotInfo(void *pageData, const unsigned short int slotNum,
			const unsigned short int slotStartIndex,
			const unsigned short int slotOffset);

	RC isCurrentPageAvailableInsert(const void * pageData,
			const unsigned short int &dataSize, bool &currentPageAvailable);

	RC isCurrentPageAvailableUpdate(const void * pageData,
			const unsigned short int &dataSize,
			const unsigned short int &slotNum, bool &currentPageAvailable);

	/* page management in file level */
	RC appendSignedPageInFile(FileHandle &fileHandle);

	RC insertFormatDataToFile(FileHandle fileHandle, const void * formatData,
			const unsigned short int &dataSize, RID &rid);

	RC updateFormatDataInFile(FileHandle fileHandle, void * formatData,
			unsigned short int &dataSize, const RID &rid);

	// delete formatData in file system
	RC deleteFormatDataInFile(FileHandle &fileHandle, const RID rid);

	// read formatData in fileSystem
	RC readFormatDataInFile(FileHandle fileHandle,
			const RID &rid, void * formatData, unsigned short int &dataSize);

	// find available page in fileSystem, if not find, append a new page in file system
	RC findAvailablePageInFile(FileHandle &fileHandle,
			const unsigned short int dataSize, unsigned &pageNum);
};
//
//// interface from record to page
//class FileSystem{
//public:
//	FileSystem();
//	~FileSystem();
//	RC initFile();
//	RC insertFormatData(FileHandle fileHandle, const void * formatData, const unsigned short int &dataSize, RID &rid);
//	RC deleteFormatData(FileHandle &fileHandle, const RID rid);
//	RC updateFormatData(FileHandle fileHandle, void * formatData, unsigned short int &dataSize, const RID &rid);
//	RC readFormatData(FileHandle fileHandle, const RID &rid, FormatData formatData, unsigned short int &dataSize);
//protected:
//	RC findAvailablePage(FileHandle &fileHandle, const unsigned short int dataSize, unsigned &pageNum);
//	RC appendPage(FileHandle &fileHandle);
//private:
//};
//
//// object from file to
//class Page{
//public:
//	Page();
//	~Page();
//	RC initPage(const void * pageData);
//
//private:
//	// page information
//	void * pageData;
//
//};
//
//// interface from Data to FormatData
//class FormatData{
//public:
//	FormatData();
//	~FormatData();
//	RC initFormatData(void * data);
//	RC setFormatDataWithFirst(const RID rid);
//
//protected:
//private:
//	void * formatData;
//};

#endif
