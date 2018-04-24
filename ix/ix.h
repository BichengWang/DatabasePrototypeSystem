#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stack>
#include <cfloat>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

# define IS_INDEX_PAGE 5
# define IS_LEAF_PAGE 10

# define CKEY_EQUAL         100
# define CKEY_FIND_BIGGER   200
# define CKEY_EXCEED        300
# define CKEY_CHANGE_PAGE   400

# define LH_SMALL_RH 1
# define LH_EQUAL_RH 0
# define LH_BIGGE_RH -1

typedef unsigned Size;

typedef struct {
	PageNum pageNum;    // page number
	Size slotNum;    // slot number in the page
} PageMap;

typedef struct {
	void * key;
	Size keySize;
	RID rid;
} CompositeKey;

class IX_ScanIterator;
class IXFileHandle;
class PageData;
class IXPageData;
class IndexPageData;
class LeafPageData;

class IndexManager {

public:
	static IndexManager* instance();

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle,
			const Attribute &attribute) const;

protected:
	IndexManager();
	~IndexManager();

private:

	static IndexManager *_index_manager;

	PagedFileManager * pagedFileManager;

	PageData * pageData;

	IndexPageData * indexPageData;

	LeafPageData * leafPageData;

	RC calcKeySize(const Attribute &attribute, const void * key,
			int &keyLength);

	RC checkAttributeCorrect(void * pageData, const Attribute &attribute);

	RC depthFirstPreorderTreeTraversal(IXFileHandle &ixfileHandle,
			void * curPage, const Attribute &attribute, int level) const;

	RC init(IXFileHandle &ixfileHandle);

	RC makeCompositeKey(const Attribute &attribute, const void * key,
			const RID &rid, CompositeKey &compositeKey);

	RC compositeKeySanityCheck(CompositeKey &compositeKey);

    RC leafPageInsertEntry(IXFileHandle &ixfileHandle, const CompositeKey &compositeKey,
    		const Attribute &attribute, void * curPage, const PageNum &curPageNum);

    RC indexPageInsertEntry(IXFileHandle &ixfileHandle, CompositeKey &compositeKey,
    		const Attribute &attribute, void * curPage, const PageNum &curPageNum,
    		const PageNum newIndexPageNum);

    RC indexPageSplitTillNewRoot(IXFileHandle &ixfileHandle, CompositeKey &compositeKey,
    		const Attribute &attribute, void * curPage, const PageNum &curPageNum,
    		const PageNum newIndexPageNum);

	RC scanSanityCheck(const Attribute &attribute, const void *lowKey,
			const void *highKey, bool lowKeyInclusive, bool highKeyInclusive);
};

class IX_ScanIterator {
public:

	// Constructor
	IX_ScanIterator();

	// Destructor
	~IX_ScanIterator();

	RC init(IXFileHandle &ixfileHandle, const Attribute attribute,
			const void* lowkey, const void* highKey, bool lowKeyInclusive,
			bool highKeyInclusive);

	RC getFirstAvailablePage(IXFileHandle &ixfileHandle,
			const Attribute attribute);

	RC makeLowCompositeKey(CompositeKey &compositeKey,
			const Attribute &attribute, const void * lowkey,
			const bool &lowKeyInclusive);

	RC makeHighCompositeKey(CompositeKey &compositeKey,
			const Attribute &attribute, const void * highKey,
			const bool &highKeyInclusive);

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);

	// Terminate index scan
	RC close();

	RC compare(const void * key, const void * target,
			const Attribute &attribute);
	RC compare(const RID &target, const RID &key);
	RC compare(const CompositeKey &key, const CompositeKey &target,
			const Attribute &attribute);

	CompositeKey lowCompositeKey;
	CompositeKey highCompositeKey;
	void * curPage;
	void * lKey;
	void * hKey;
	void* realHKey;

	PageData * pageData;
	IndexPageData * indexPageData;
	LeafPageData * leafPageData;

	IXFileHandle * ixfileHandle;

	Attribute attribute;

	bool lKeyInclusive;
	bool hKeyInclusive;

	PageNum lKeyPageNum;
	Size lKeySlotNum;
	PageNum hKeyPageNum;
	Size hKeySlotNum;

	RID realHRID;
	Size realHKeySize;

};

class IXFileHandle : public FileHandle {
public:

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;

	// Constructor
	IXFileHandle();

	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount);

	unsigned getNumberOfPages(){
		return this->numberOfPages;
	}
};

class PageData {
public:
	static PageData * instance();

	RC getRootInfo(const void * pageData, unsigned int &rootPageNum);

	RC setRootInfo(void * pageData, const unsigned int rootPageNum);

	RC getAttributeInfo(const void * pageData, Attribute &attribute);

	RC setAttributeInfo(void * pageData, const Attribute attribute);

public:

	/* page management in page level */
	RC signNewPage(void * pageData);
	RC deletePage(void * pageData);

	/* CRUD */
	RC insertRecord(void * pageData, const void *record,
			const Size &recordSize, Size &slotNum);

	RC deleteRecord(void * pageData, const Size &slotNum);

	RC updateRecord(void * pageData, const void * formatData,
			const Size &dataSize,
			const Size &currentSlotNum);

	RC getRecord(const void * pageData, const Size &slotNum,
			void * record, Size &recordSize);

	/* page information */
	RC availableInsert(const void * pageData,
			const Size &recordSize, bool &currentPageAvailable);

	RC availiableSpace(const void * pageData, Size &space);

protected:

	PageData();
	~PageData();

	/* getter setter */
	RC getPageInfo(const void * pageData,
			Size &firstAvailableIndex,
			Size &totalSlotNum);

	RC setPageInfo(void * pageData, const Size &firstAvailableIndex,
			const Size &totalSlotNum);

	RC getSlotInfo(const void * pageData, const Size &slotNum,
			Size &slotStartIndex, Size &slotOffset);

	RC setSlotInfo(void *pageData, const Size &slotNum,
			const Size &slotStartIndex,
			const Size &slotOffset);

private:
	static PageData * _page_data;
};

class IXPageData: protected PageData {
public:
	static IXPageData * instance();

	RC insertRecord(void * pageData, const unsigned &slotNum,
			const void * record, const Size &recordSize);

	RC deleteRecord(void * pageData, const Size &slotNum);

protected:
	IXPageData();
	~IXPageData();

	/* insert delete helper */
	RC insertSlotInfo(void * pageData, const Size &slotNum);

	RC deleteSlotInfo(void * pageData, const Size &slotNum);

	RC insertRecordBySlotNum(void * pageData, const Size &slotNum,
			const void * record, const Size &recordSize);

	RC deleteRecordBySlotNum(void * pageData,
			const Size &slotNum);

	/* split records */
	RC splitWithRecord(void * pageData, void * newPageData,
			const void * insertRecord,
			const Size &insertRecordSize,
			const Size &insertSlotNum);

	/* split records helper */
	RC deleteFormerPageRecord(void * newPageData, const Size &splitNum);

	RC deleteLatterPageRecord(void * newPageData, const Size &splitSlotNum);

	/* comparator */
	RC compare(const void * key, const void * target,
			const Attribute &attribute);
	RC compare(const RID &target, const RID &key);

	RC compare(const CompositeKey &key, const CompositeKey &target,
			const Attribute &attribute);

private:
	static IXPageData * _ix_page_data;
};

class IndexPageData: public IXPageData {

public:
	static IndexPageData * instance();

	RC signNewPage(void * pageData);

	RC pageType(const void * pageData, int &pageType);

	/* getter setter */
	RC insertFirstIndex(void * pageData, const PageNum &pageNum);

	RC insertIndexRecord(void * pageData, CompositeKey &compositeKey,
			const PageNum &pageNum, const Attribute &attribute);

//	RC pointToIndexRecord(const void * pageData, const Size slotNum, void * indexRecord, Size &indexRecordSize);

	RC getCompositeKey(const void * pageData, const Size &slotNum,
			CompositeKey &compositeKey);

	RC getKey(const void * pageData, const Size &slotNum,
			void * key, Size &keySize);

//	RC pointToKey(const void * pageData, const Size keyNum, void * key, Size &keySize);

	RC getValue(const void * pageData, const Size &valueNum,
			PageNum &pageNum);

	/* split */
	RC split(void * pageData, void * newPageData, const Attribute &attribute,
			CompositeKey &compositeKey, PageNum pageNum);

	/* find */
	RC findCompositeKeyPosition(const void * pageData,
			const CompositeKey &targetCompositeKey, const Attribute &attribute,
			Size &slotNum);

	/* print */
	RC indexPageToString(const void * pageData, const Attribute &attribute,
			string &output);

	RC compositeKeyToString(const CompositeKey &compositeKey,
			const Attribute &attribute, string &output);

protected:
	IndexPageData();
	~IndexPageData();

	/* package index record */
	RC packageIndexRecord(CompositeKey &compositeKey, const PageNum &pageNum,
			void * indexRecord, Size &indexRecordSize);

	RC unpackIndexRecord(void * indexRecord, const Size &indexRecordSize,
			CompositeKey &compositeKey, PageNum &pageNum);

private:
	static IndexPageData * _index_page_data;
};

class LeafPageData: public IXPageData {
public:
	static LeafPageData * instance();

	RC signNewPage(void * pageData);

	RC pageType(const void * pageData, int &pageType);

	/* getter setter */
	RC setNextPageNum(void * pageData, const PageNum &pageNum);

	RC getNextPageNum(const void * pageData, PageNum &pageNum);

	RC insertLeafRecord(void * pageData, const CompositeKey &compositedKey,
			const Attribute &attribute);

//	RC pointToLeafRecord(const void * pageData, const Size slotNum, void * leafRecord, Size &leafRecordSize);

	RC getCompositeKey(const void * pageData, const Size &slotNum,
			CompositeKey &compositeKey);

	RC getKey(const void * pageData, const Size &keyNum,
			void * key, Size &keySize);

//	RC pointToKey(const void * pageData, const Size keyNum,
//			void * key, Size &keySize);

	RC getValue(const void * pageData, const Size &valueNum,
				RID &rid);

	/* split */
	RC split(void * pageData, void * newPageData, const PageNum &newPageNum,
			const Attribute &attribute, CompositeKey &compositeKey);

	/* find */
	RC findCompositeKeyPosition(const void * pageData,
			const CompositeKey &targetCompositeKey, const Attribute &attribute,
			Size &slotNum);

	RC findNextCompositeKeyPosition(const void * pageData,
			const CompositeKey &targetCompositeKey, const Attribute &attribute,
			Size &slotNum);

	/* print */
	RC leafPageToString(const void * pageData, const Attribute &attribute,
			string &output);

protected:
	LeafPageData();
	~LeafPageData();

private:
	static LeafPageData * _leaf_page_data;
};

#endif
