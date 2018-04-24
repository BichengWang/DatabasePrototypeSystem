#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();
	return _index_manager;
}

IndexManager::IndexManager() {
	this->pagedFileManager = PagedFileManager::instance();
	this->pageData = PageData::instance();
	this->indexPageData = IndexPageData::instance();
	this->leafPageData = LeafPageData::instance();
}

IndexManager::~IndexManager() {
	if (_index_manager != NULL) {
		delete _index_manager;
		_index_manager = 0;
	}
}

RC IndexManager::createFile(const string &fileName) {
	if (this->pagedFileManager->createFile(fileName) != 0) {
		return -1;
	}
	IXFileHandle ixfileHandle;
	if(this->openFile(fileName, ixfileHandle) != 0){
		cerr << "Exception in IndexManager::createFile, this->openFile();" << endl;
		return -1;
	}
	if(this->init(ixfileHandle) != 0){
		cerr << "Exception in IndexManager::createFile, this->init();" << endl;
		return -1;
	}
	if(this->closeFile(ixfileHandle) != 0){
		cerr << "Exception in IndexManager::createFile, this->closeFile();" << endl;
		return -1;
	}
	return 0;
}

RC IndexManager::destroyFile(const string &fileName) {
	return this->pagedFileManager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	if (ixfileHandle.isOpen()) {
		return -1;
	}
	return this->pagedFileManager->openFile(fileName, ixfileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	return this->pagedFileManager->closeFile(ixfileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	void * curPage = malloc(PAGE_SIZE);
	memset(curPage, 0, PAGE_SIZE);
	PageNum curPageNum = 0;
	PageNum nextPageNum;
	Size curSlot = 0;
	PageMap pageMap;
	pageMap.pageNum = 0;
	pageMap.slotNum = 0;
	stack<PageMap> pageStack;

	CompositeKey compositeKey;
	compositeKey.key = malloc(PAGE_SIZE);
	memset(compositeKey.key, 0, PAGE_SIZE);
	if(this->makeCompositeKey(attribute, key, rid, compositeKey) != 0){
		cerr << "Exception in IndexManager::insertEntry," << endl;
		cerr << "in this->makeCompositeKey();" << endl;
		free(compositeKey.key);
		return -1;
	}
	if(this->compositeKeySanityCheck(compositeKey) != 0){
		cerr << "Exception in IndexManager::insertEntry:" << endl;
		cerr << "compositeKey too large to fit in one page." << endl;
		free(compositeKey.key);
		return -1;
	}
	if(ixfileHandle.readPage(curPageNum, curPage) != 0){
		cerr << "Exception in function insertEntry():" << endl;
		cerr << "Cannot read meta page." << endl;
		free(compositeKey.key);
		return -1;
	}
	RC ret;
//	RC ret = checkAttributeCorrect(curPage, attribute);
//	if (ret == 1) {
//		this->pageData->setAttributeInfo(curPage, attribute);
//		ixfileHandle.writePage(curPageNum, curPage);
//	} else if (ret == -1) {
//		cerr << "Exception in function insertEntry():" << endl;
//		cerr << "Attribute in index file is not correct." << endl;
//		free(compositeKey.key);
//		free(curPage);
//		return -1;
//	}
	this->pageData->getRootInfo(curPage, curPageNum);
	if(ixfileHandle.readPage(curPageNum, curPage) != 0){
		cerr << "Exception in function insertEntry():" << endl;
		cerr << "Cannot read root page." << endl;
		free(compositeKey.key);
		return -1;
	}

	int isIndexPage;
	this->indexPageData->pageType(curPage, isIndexPage);
	while (isIndexPage == IS_INDEX_PAGE) {
		if(this->indexPageData->findCompositeKeyPosition(curPage, compositeKey,
				attribute, curSlot) == -1){
			cerr << "Exception in IndexManager::insertEntry," << endl;
			cerr << "in indexPageData->findCompositeKeyPosition();" << endl;
			free(compositeKey.key);
			free(curPage);
			return -1;
		}
		if(this->indexPageData->getValue(curPage, curSlot, nextPageNum) != 0){
			cerr << "Exception in IndexManager::insertEntry," << endl;
			cerr << "in indexPageData->getValue();" << endl;
			free(compositeKey.key);
			free(curPage);
			return -1;
		}
		pageMap.pageNum = curPageNum;
		pageMap.slotNum = curSlot;
		pageStack.push(pageMap);
		curPageNum = nextPageNum;
		ixfileHandle.readPage(nextPageNum, curPage);
		this->indexPageData->pageType(curPage, isIndexPage);
	}

	ret = this->leafPageData->findCompositeKeyPosition(curPage, compositeKey,
			attribute, curSlot);
	if (ret == CKEY_EQUAL || ret == -1) {
		cerr << "Exception in function insertEntry():\n";
		cerr << "Cannot locate, or duplicate entry in leaf page.\n";
		free(curPage);
		free(compositeKey.key);
		return -1;
	}
	bool isAvailableInsert;
	pageData->availableInsert(curPage, compositeKey.keySize + sizeof(RID),
			isAvailableInsert);

	if (isAvailableInsert) {
		if(this->leafPageInsertEntry(ixfileHandle, compositeKey, attribute,
								     curPage, curPageNum)){
			cerr << "Exception in function insertEntry():\n";
			cerr << "in this->leafPageInsertEntry()\n";
			return -1;
		}
		free(curPage);
		free(compositeKey.key);
		return 0;
	}

	void * newLeafPage = malloc(PAGE_SIZE);
	memset(newLeafPage, 0, PAGE_SIZE);
	ixfileHandle.appendPage(newLeafPage);
	int newLeafPageNum;
	newLeafPageNum = ixfileHandle.getNumberOfPages() - 1;
	if(this->leafPageData->split(curPage, newLeafPage, newLeafPageNum, attribute,
			compositeKey) != 0){
		cerr << "Exception in function insertEntry():\n";
		cerr << "in leafPageData->split()\n";
		free(curPage);
		free(compositeKey.key);
		free(newLeafPage);
		return -1;
	}
	ixfileHandle.writePage(curPageNum, curPage);
	ixfileHandle.writePage(newLeafPageNum, newLeafPage);
	free(newLeafPage);

	PageMap lastPageMap;
	int lastPageNum;
//	int lastSlotNum;
	int newIndexPageNum;
	int oldIndexPageNum;
	newIndexPageNum = newLeafPageNum;
	while (true) {
		if (pageStack.size() == 0) {
			if(this->indexPageSplitTillNewRoot(ixfileHandle, compositeKey,
					attribute, curPage, curPageNum, newIndexPageNum)){
				cerr << "Exception in function insertEntry():\n";
				cerr << "in this->indexPageSplitTillNewRoot()\n";
				free(curPage);
				free(compositeKey.key);
				return -1;
			}
			break;
		}
		lastPageMap = pageStack.top();
		pageStack.pop();
		lastPageNum = lastPageMap.pageNum;
//		lastSlotNum = lastPageMap.slotNum;
		ixfileHandle.readPage(lastPageNum, curPage);
		curPageNum = lastPageNum;
		this->pageData->availableInsert(curPage,
				compositeKey.keySize + sizeof(RID) + sizeof(PageNum),
				isAvailableInsert);
		if (isAvailableInsert) {
			if(this->indexPageInsertEntry(ixfileHandle, compositeKey,
					attribute, curPage, curPageNum, newIndexPageNum)){
				cerr << "Exception in function insertEntry():\n";
				cerr << "in this->indexPageInsertEntry()\n";
				free(curPage);
				free(compositeKey.key);
				return -1;
			}
			break;
		} else {
			oldIndexPageNum = newIndexPageNum;
			void * newIndexPage = malloc(PAGE_SIZE);
			memset(newIndexPage, 0, PAGE_SIZE);
			ixfileHandle.appendPage(newIndexPage);
			newIndexPageNum = ixfileHandle.getNumberOfPages() - 1;
			if(this->indexPageData->split(curPage, newIndexPage, attribute,
					compositeKey, oldIndexPageNum) != 0){
				cerr << "Exception in function insertEntry():\n";
				cerr << "in indexPageData->split()\n";
				free(curPage);
				free(compositeKey.key);
				return -1;
			}
			ixfileHandle.writePage(curPageNum, curPage);
			ixfileHandle.writePage(newIndexPageNum, newIndexPage);
			free(newIndexPage);
		}
	}
	free(curPage);
	free(compositeKey.key);
	return 0;
}

// LazyDelete for now
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	PageNum curPageNum = 0;
	Size curSlot = 0;
	PageNum nextPageNum;

	CompositeKey compositeKey;
	compositeKey.key = malloc(PAGE_SIZE);
	memset(compositeKey.key, 0, PAGE_SIZE);
	if(this->makeCompositeKey(attribute, key, rid, compositeKey) != 0){
		cerr << "Exception in IndexManager::deleteEntry, this->makeCompositeKey();" << endl;
		return -1;
	}
	if(this->compositeKeySanityCheck(compositeKey) != 0){
		cerr << "Exception in IndexManager::deleteEntry:" << endl;
		cerr << "compositeKey too large to fit in one page." << endl;
		return -1;
	}

	void * curPage = malloc(PAGE_SIZE);
	memset(curPage, 0, PAGE_SIZE);
	ixfileHandle.readPage(curPageNum, curPage);

//	if (checkAttributeCorrect(curPage, attribute) != 0) {
//		cerr << "Exception in function deleteEntry():\n";
//		cerr << "Attribute in index file is not correct.\n";
//		free(compositeKey.key);
//		free(curPage);
//		return -1;
//	}

	this->pageData->getRootInfo(curPage, curPageNum);
	ixfileHandle.readPage(curPageNum, curPage);

	int isIndexPage;
	this->indexPageData->pageType(curPage, isIndexPage);
	while (isIndexPage == IS_INDEX_PAGE) {
		this->indexPageData->findCompositeKeyPosition(curPage, compositeKey,
				attribute, curSlot);
		this->indexPageData->getValue(curPage, curSlot, nextPageNum);
		curPageNum = nextPageNum;
		ixfileHandle.readPage(nextPageNum, curPage);
		this->indexPageData->pageType(curPage, isIndexPage);
	}

	RC rc = this->leafPageData->findCompositeKeyPosition(curPage, compositeKey,
			attribute, curSlot);
	if (rc == CKEY_FIND_BIGGER || rc == CKEY_EXCEED) {
		cerr << "Exception in function deleteEntry():" << endl;
		cerr << "Cannot locate entry in leaf page." << endl;
		free(compositeKey.key);
		free(curPage);
		return -1;
	}
	leafPageData->deleteRecord(curPage, curSlot);
	ixfileHandle.writePage(curPageNum, curPage);
	free(compositeKey.key);
	free(curPage);
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
	if(!ixfileHandle.isOpen()) {
		cerr << "Exception in function scan(): cannot open file." << endl;
		return -1;
	}
	int lowKeyLength;
	int highKeyLength;
	if(lowKey != NULL){
		this->calcKeySize(attribute, lowKey, lowKeyLength);
	}
	if(highKey != NULL){
		this->calcKeySize(attribute, highKey, highKeyLength);
	}
	if(this->scanSanityCheck(attribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive) != 0){
		cerr << "Exception in function scan():" << endl;
		cerr << "Input condition failed sanity check." << endl;
		return -1;
	}

	if(ix_ScanIterator.init(ixfileHandle, attribute, lowKey, highKey,
			lowKeyInclusive, highKeyInclusive) != 0){
		cerr << "Exception in function scan():" << endl;
		cerr << "In ix_ScanIterator.init()" << endl;
		return -1;
	}
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {

	unsigned int curPageNum = 0;

	void * curPage = malloc(PAGE_SIZE);
	memset(curPage, 0, PAGE_SIZE);
	ixfileHandle.readPage(curPageNum, curPage);

	if (this->pageData->getRootInfo(curPage, curPageNum)) {
		cerr << "Exception in function printBtree():\n";
		cerr << "in pageData->getRootInfo()\n";
		return;
	}
	ixfileHandle.readPage(curPageNum, curPage);

	int isIndexPage;
	if (this->indexPageData->pageType(curPage, isIndexPage)) {
		cerr << "Exception in function printBtree():\n";
		cerr << "in indexPageData->pageType()\n";
		return;
	}
	int level = 0;
	string leaf_line = "";
	if (isIndexPage == IS_INDEX_PAGE) {
		cout << "{[";
		depthFirstPreorderTreeTraversal(ixfileHandle, curPage, attribute,
				level);
		cout << endl;
		free(curPage);
		return;
	} else if (isIndexPage == IS_LEAF_PAGE) {
		this->leafPageData->leafPageToString(curPage, attribute, leaf_line);
		cout << leaf_line;
		cout << endl;
		free(curPage);
		return;
	} else {
		cerr << "Exception in function PrintBTree():\n";
		cerr << "Parameter isIndexPage is: " << isIndexPage << "\n";
		free(curPage);
		return;
	}
}

RC IndexManager::init(IXFileHandle &ixfileHandle)
{
	void * metaPage = malloc(PAGE_SIZE);
	memset(metaPage, 0, PAGE_SIZE);
	void * rootPage = malloc(PAGE_SIZE);
	memset(rootPage, 0, PAGE_SIZE);
	Attribute attribute;
	attribute.name = "INVALID";
	attribute.type = TypeInt;
	attribute.length = 0;
	if(this->pageData->setRootInfo(metaPage, 1) != 0){
		cerr << "Exception in IndexManager::init(), pageData->setRootInfo()" << endl;
		return -1;
	}
	if(this->pageData->setAttributeInfo(metaPage, attribute) != 0){
		cerr << "Exception in IndexManager::init(), pageData->setAttributeInfo()" << endl;
		return -1;
	}
	if(this->leafPageData->signNewPage(rootPage) != 0){
		cerr << "Exception in IndexManager::init(), pageData->signNewPage()" << endl;
		return -1;
	}
	if(ixfileHandle.appendPage(metaPage) != 0){
		cerr << "Exception in IndexManager::init(), ixfileHandle.appendPage(metaPage)" << endl;
		return -1;
	}
	if(ixfileHandle.appendPage(rootPage) != 0){
		cerr << "Exception in IndexManager::init(), ixfileHandle.appendPage(rootPage)" << endl;
		return -1;
	}
	free(metaPage);
	free(rootPage);
	return 0;
}

RC IndexManager::calcKeySize(const Attribute &attribute, const void * key,
		int &keyLength) {
	if (attribute.type == 0) {
		keyLength = sizeof(int);
	} else if (attribute.type == 1) {
		keyLength = sizeof(float);
	} else if (attribute.type == 2) {
		int varcharSize;
		memcpy(&varcharSize, key, sizeof(int));
		keyLength = varcharSize + sizeof(int);
	} else {
		cerr << "Exception in function IndexManager::calcKeySize()" << endl;
		cerr << "Invalid attribute type!" << endl;
		return -1;
	}
	return 0;
}

RC IndexManager::makeCompositeKey(const Attribute &attribute, const void * key,
		const RID &rid, CompositeKey &compositeKey){
	int keyLength;
	if(this->calcKeySize(attribute, key, keyLength) != 0){
		cerr << "Exception in IndexManager::makeCompositeKey, this->getKeyLength();" << endl;
		return -1;
	}
	memcpy(compositeKey.key, key, keyLength);
	compositeKey.keySize = keyLength;
	compositeKey.rid.pageNum = rid.pageNum;
	compositeKey.rid.slotNum = rid.slotNum;
	return 0;
}

RC IndexManager::compositeKeySanityCheck(CompositeKey &compositeKey){
	// TODO: need to be accurate
	if(compositeKey.keySize + sizeof(RID) >= PAGE_SIZE){
		return -1;
	}
	return 0;
}

RC IndexManager::checkAttributeCorrect(void * pageData,
		const Attribute &attribute) {
	Attribute attributeInFile;
	string invalid = "INVALID";
	this->pageData->getAttributeInfo(pageData, attributeInFile);
	if (attributeInFile.name == invalid && attributeInFile.type == (AttrType) 0
			&& attributeInFile.length == 0) {
		this->pageData->setAttributeInfo(pageData, attributeInFile);
		return 1;
	}
	if (attributeInFile.name != attribute.name
			|| attributeInFile.type != attribute.type
			|| attributeInFile.length != attribute.length) {
		return -1;
	}
	return 0;
}

RC IndexManager::leafPageInsertEntry(IXFileHandle &ixfileHandle,
		const CompositeKey &compositeKey,
		const Attribute &attribute, void * curPage,
		const PageNum &curPageNum)
{
	if(this->leafPageData->insertLeafRecord(curPage, compositeKey, attribute)){
		cerr << "Exception in function leafPageInsertEntry():\n";
		cerr << "in leafPageData->insertLeafRecord()\n";
		return -1;
	}
	if(ixfileHandle.writePage(curPageNum, curPage)){
		cerr << "Exception in function leafPageInsertEntry():\n";
		cerr << "in ixfileHandle.writePage()\n";
		return -1;
	}
	return 0;
}

RC IndexManager::indexPageInsertEntry(IXFileHandle &ixfileHandle, CompositeKey &compositeKey,
		const Attribute &attribute, void * curPage, const PageNum &curPageNum,
		const PageNum newIndexPageNum)
{
	if(this->indexPageData->insertIndexRecord(curPage, compositeKey,
			newIndexPageNum, attribute)){
		cerr << "Exception in function indexPageInsertEntry():\n";
		cerr << "in indexPageData->insertIndexRecord()\n";
		return -1;
	}
	if(ixfileHandle.writePage(curPageNum, curPage)){
		cerr << "Exception in function indexPageInsertEntry():\n";
		cerr << "in ixfileHandle.writePage()\n";
		return -1;
	}
	return 0;
}

RC IndexManager::indexPageSplitTillNewRoot(IXFileHandle &ixfileHandle,
		CompositeKey &compositeKey, const Attribute &attribute,
		void * curPage, const PageNum &curPageNum,
        const PageNum newIndexPageNum)
{
	int newRootPageNum;
	void * newRootPage = malloc(PAGE_SIZE);
	memset(newRootPage, 0, PAGE_SIZE);
	if(this->indexPageData->signNewPage(newRootPage)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "in indexPageData->signNewPage()\n";
		return -1;
	}
	ixfileHandle.appendPage(newRootPage);
	newRootPageNum = ixfileHandle.getNumberOfPages() - 1;
	if(ixfileHandle.readPage(newRootPageNum, newRootPage)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "while reading new root page.\n";
		return -1;
	}
	if(this->indexPageData->insertFirstIndex(newRootPage, curPageNum)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "in indexPageData->setFirstIndex()\n";
		return -1;
	}
	if(this->indexPageData->insertIndexRecord(newRootPage, compositeKey,
			newIndexPageNum, attribute)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "in indexPageData->insertIndexRecord()\n";
		return -1;
	}
	if(ixfileHandle.writePage(newRootPageNum, newRootPage)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "while writing new root page.\n";
		return -1;
	}
	void * metaPage = malloc(PAGE_SIZE);
	memset(metaPage, 0, PAGE_SIZE);
	ixfileHandle.readPage(0, metaPage);
	if(this->pageData->setRootInfo(metaPage, newRootPageNum)){
		cerr << "Exception in function indexPageSplitTillNewRoot():\n";
		cerr << "in pageData->setRootInfo()\n";
		return -1;
	}
	ixfileHandle.writePage(0, metaPage);
	free(newRootPage);
	free(metaPage);
	return 0;
}

RC IndexManager::scanSanityCheck(const Attribute &attribute,
		const void *lowKey, const void *highKey,
		bool lowKeyInclusive, bool highKeyInclusive){
	if(lowKey == NULL || highKey == NULL){
		return 0;
	}
	if(attribute.type == 0){
		int lowKeyInt;
		int highKeyInt;
		memcpy(&lowKeyInt, lowKey, sizeof(int));
		memcpy(&highKeyInt, highKey, sizeof(int));
		if(lowKeyInt > highKeyInt){
			cerr << "Exception in function scanSanityCheck()" << endl;
			cerr << "INT: low key bigger than high key." << endl;
			return -1;
		}
	} else if(attribute.type == 1){
		int lowKeyFloat;
		int highKeyFloat;
		memcpy(&lowKeyFloat, lowKey, sizeof(int));
		memcpy(&highKeyFloat, highKey, sizeof(int));
		if(lowKeyFloat > highKeyFloat){
			cerr << "Exception in function scanSanityCheck()" << endl;
			cerr << "INT: low key bigger than high key." << endl;
			return -1;
		}
	} else if(attribute.type == 2){
		//TODO: make it better
		return 0;
	} else{
		cerr << "Exception in function scanSanityCheck()" << endl;
		cerr << "Invalid attribute type!" << endl;
		return -1;
	}
	return 0;
}

RC IndexManager::depthFirstPreorderTreeTraversal(IXFileHandle &ixfileHandle,
		void * curPage, const Attribute &attribute, int level) const {
	string index_line = "";
	string leaf_line = "";
	string prefix(level, '\t');
	this->indexPageData->indexPageToString(curPage, attribute, index_line);
	cout << prefix << index_line << endl;
	cout << prefix << "\"children\":[" << endl;
	PageNum nextPageNum;
	Size slotNum = 0;
	while (true) {
		void * nextPage = malloc(PAGE_SIZE);
		RC rc;
		rc = this->indexPageData->getValue(curPage, slotNum, nextPageNum);
		if (rc == -1) {
			free(nextPage);
			break;
		}
		else{
		if (ixfileHandle.readPage(nextPageNum, nextPage)) {
			cerr << "Exception in function depthFirstPreorderTreeTraversal():\n";
			cerr << "in ixfileHandle.readPage()\n";
			return -1;
		}
		int isIndexPage;
		if (this->indexPageData->pageType(nextPage, isIndexPage)) {
			cerr << "Exception in function depthFirstPreorderTreeTraversal():\n";
			cerr << "in indexPageData->pageType()\n";
			return -1;
		}
		if (isIndexPage == IS_LEAF_PAGE) {
			this->leafPageData->leafPageToString(nextPage, attribute, leaf_line);
		} else {
			depthFirstPreorderTreeTraversal(ixfileHandle, nextPage, attribute, level + 1);
		}
		cout << "\t" << prefix << leaf_line << "," << endl;
		leaf_line = "";
		free(nextPage);
		slotNum++;
		}
	}
	cout << prefix << "]}" << endl;
	return 0;
}


IX_ScanIterator::IX_ScanIterator() {
	this->lowCompositeKey.key = malloc(PAGE_SIZE);
	this->highCompositeKey.key = malloc(PAGE_SIZE);
	this->curPage = malloc(PAGE_SIZE);
	this->lKey = malloc(PAGE_SIZE);
	this->hKey = malloc(PAGE_SIZE);
	this->realHKey = malloc(PAGE_SIZE);

	this->pageData = PageData::instance();
	this->indexPageData = IndexPageData::instance();
	this->leafPageData = LeafPageData::instance();
}

IX_ScanIterator::~IX_ScanIterator() {
	free(this->realHKey);
	free(this->hKey);
	free(this->lKey);
	free(this->curPage);
	free(this->highCompositeKey.key);
	free(this->lowCompositeKey.key);
}

RC IX_ScanIterator::init(IXFileHandle &ixfileHandle, const Attribute attribute,
		const void* lowkey, const void* highKey, bool lowKeyInclusive,
		bool highKeyInclusive) {
	this->ixfileHandle = &ixfileHandle;
	//TODO: bug point
//	if (lowkey == NULL && highKey == NULL) {
//		cerr << "lowKey == NULL && highKey == NULL" << endl;
//		return 0;
//	}

	if (highKey != NULL) {
		memcpy(this->hKey, highKey, PAGE_SIZE);
	}
	if (lowkey != NULL) {
		memcpy(this->lKey, lowkey, PAGE_SIZE);
	}

	this->lKeyInclusive = lowKeyInclusive;
	this->hKeyInclusive = highKeyInclusive;
	this->attribute = attribute;

	makeLowCompositeKey(this->lowCompositeKey, attribute, lowkey,
			lowKeyInclusive);

//	int i;
//	memcpy(&i, this->lowCompositeKey.key, sizeof(int));

	makeHighCompositeKey(this->highCompositeKey, attribute, highKey,
			highKeyInclusive);

//	memcpy(&i, this->highCompositeKey.key, sizeof(int));

	if (this->compare(this->lowCompositeKey, this->highCompositeKey,
			attribute) == LH_BIGGE_RH) {
		cout << "logical error of bound " << endl;
		return -1;
	}

//	if(this->ixfileHandle->isOpen()){
//		cout << "right ixfileHandle is open"<< endl;
//	}
	if (getFirstAvailablePage(ixfileHandle, attribute)) {
		return -1;
	}


	// TODO: comment this
//	IndexManager::instance()->printBtree(ixfileHandle, attribute);
	return 0;
}

RC IX_ScanIterator::getFirstAvailablePage(IXFileHandle &ixfileHandle,
		const Attribute attribute) {
	RC rc;
	PageNum curPageNum = 0;
	Size curSlot = 0;
	PageNum nextPageNum;

	void * curPage = malloc(PAGE_SIZE);
	memset(curPage, 0, PAGE_SIZE);
	ixfileHandle.readPage(curPageNum, curPage);

	this->pageData->getRootInfo(curPage, curPageNum);

	ixfileHandle.readPage(curPageNum, curPage);
	nextPageNum = curPageNum;

	int isIndexPage;
	this->indexPageData->pageType(curPage, isIndexPage);
	while (isIndexPage == IS_INDEX_PAGE) {
		rc = this->indexPageData->findCompositeKeyPosition(curPage,
				this->lowCompositeKey, attribute, curSlot);
		if (rc == -1) {
			cout << "find INDEX key error " << endl;
			return -1;
		}
		rc = this->indexPageData->getValue(curPage, curSlot, nextPageNum);
		if (rc == -1) {
			cout << "get value error " << endl;
			return -1;
		}
		rc = ixfileHandle.readPage(nextPageNum, curPage);
		if (rc == -1) {
			cout << "read page error " << endl;
			return -1;
		}
		this->indexPageData->pageType(curPage, isIndexPage);
	}

//	this->curPage = malloc(PAGE_SIZE);

//	cout << "++++++++++" << endl;
//	cout << "nextPageNum" << nextPageNum << endl;
//	if(ixfileHandle.isOpen()){
//		cout <<"right" << endl;
//		cout << ixfileHandle.getNumberOfPages()<< endl;
//	}
	if(this->curPage == NULL){
//		cout << "error"<< endl;
		this->curPage = malloc(PAGE_SIZE);
	}
	ixfileHandle.readPage(nextPageNum, this->curPage);
//	cout << "++++++++++" << endl;
//	free(curPage);
	return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
//	cout << "In IX_ScanIterator::getNextEntry" << endl;
	RC rc;
	Size nextSlotNum;
	while (true) {
		rc = this->leafPageData->findNextCompositeKeyPosition(this->curPage,
				this->lowCompositeKey, this->attribute, nextSlotNum);
		if (rc == -1) {
			return -1;
		}
		if (rc == CKEY_FIND_BIGGER) {
			break;
		}
		if (rc == CKEY_CHANGE_PAGE) {
			PageNum nextPageNum;
			if(this->leafPageData->getNextPageNum(this->curPage, nextPageNum) == -1){
//				cout << "Error in function IX_ScanIterator::getNextEntry();" << endl;
				return -1;
			}
			if (nextPageNum == 0) {
//				cout << "index EOF because of nextPageNum == 0;" << endl;
				return IX_EOF;
			} else {
				this->ixfileHandle->readPage(nextPageNum, this->curPage);
			}
		}
	}
	CompositeKey nextCompositeKey;
	nextCompositeKey.key = malloc(PAGE_SIZE);

	rc = this->leafPageData->getCompositeKey(this->curPage, nextSlotNum,
			nextCompositeKey);
	if (rc == -1) {
		cout << "should not happen " << endl;
		free(nextCompositeKey.key);
		return -1;
	}
	int compareResult = this->compare(nextCompositeKey, this->highCompositeKey,
			this->attribute);
	if (compareResult == LH_BIGGE_RH || LH_EQUAL_RH) {
		free(nextCompositeKey.key);
//		cout << "index EOF because of compareResult == LH_BIGGE_RH || LH_EQUAL_RH;" << endl;
		return IX_EOF;
	}

	int i;
	memcpy(&i, nextCompositeKey.key, sizeof(int));

	memcpy(key, nextCompositeKey.key, nextCompositeKey.keySize);
	memcpy(&rid.pageNum, &nextCompositeKey.rid.pageNum, sizeof(unsigned));
	memcpy(&rid.slotNum, &nextCompositeKey.rid.slotNum, sizeof(unsigned));

	this->lowCompositeKey.keySize = nextCompositeKey.keySize;
	memcpy(lowCompositeKey.key, nextCompositeKey.key, nextCompositeKey.keySize);
	this->lowCompositeKey.rid.pageNum = nextCompositeKey.rid.pageNum;
	this->lowCompositeKey.rid.slotNum = nextCompositeKey.rid.slotNum;
	return 0;
}

RC IX_ScanIterator::makeLowCompositeKey(CompositeKey &compositeKey,
		const Attribute &attribute, const void * lowkey,
		const bool &lowKeyInclusive) {
	if (lowkey == NULL) {
		if (attribute.type == TypeVarChar) {
			int len = 0;
			compositeKey.keySize = len + sizeof(int);
			memcpy(compositeKey.key, &len, sizeof(int));
		} else if (attribute.type == TypeReal) {
			float f = FLT_MIN;
			compositeKey.keySize = sizeof(float);
			memcpy(compositeKey.key, &f, sizeof(float));
		} else {
			int i = INT_MIN;
			compositeKey.keySize = sizeof(int);
			memcpy(compositeKey.key, &i, sizeof(int));
		}
		compositeKey.rid.pageNum = 0;
		compositeKey.rid.slotNum = 0;
	} else {
		if (lowKeyInclusive) {
			compositeKey.rid.pageNum = 0;
			compositeKey.rid.slotNum = 0;
		} else {
			compositeKey.rid.pageNum = UINT_MAX;
			compositeKey.rid.slotNum = UINT_MAX;
		}
		if (attribute.type == TypeVarChar) {
			int len;
			memcpy(&len, lowkey, sizeof(int));
			compositeKey.keySize = len + sizeof(int);
			memcpy(compositeKey.key, lowkey, compositeKey.keySize);
		} else if (attribute.type == TypeReal) {
			compositeKey.keySize = sizeof(float);
			memcpy(compositeKey.key, lowkey, sizeof(float));
		} else {
			compositeKey.keySize = sizeof(int);
			memcpy(compositeKey.key, lowkey, sizeof(int));
		}
	}
	return 0;
}

RC IX_ScanIterator::makeHighCompositeKey(CompositeKey &compositeKey,
		const Attribute &attribute, const void * highKey,
		const bool &highKeyInclusive) {
	if (highKey == NULL) {
		if (attribute.type == TypeVarChar) {
			int len = 1;
			char c = CHAR_MAX;
			compositeKey.keySize = len + sizeof(int);
			memcpy(compositeKey.key, &len, sizeof(int));
			memcpy((byte *) compositeKey.key + sizeof(int), &c, len);

		} else if (attribute.type == TypeReal) {
			float f = FLT_MAX;
			compositeKey.keySize = sizeof(float);
			memcpy(compositeKey.key, &f, sizeof(float));
		} else {
			int i = INT_MAX;
			compositeKey.keySize = sizeof(int);
			memcpy(compositeKey.key, &i, sizeof(int));
		}
		compositeKey.rid.pageNum = UINT_MAX;
		compositeKey.rid.slotNum = UINT_MAX;
	} else {
		if (highKeyInclusive) {
			compositeKey.rid.pageNum = UINT_MAX;
			compositeKey.rid.slotNum = UINT_MAX;
		} else {
			compositeKey.rid.pageNum = 0;
			compositeKey.rid.slotNum = 0;
		}
		if (attribute.type == TypeVarChar) {
			int len;
			memcpy(&len, highKey, sizeof(int));
			compositeKey.keySize = len + sizeof(int);
			memcpy(compositeKey.key, highKey, compositeKey.keySize);

		} else if (attribute.type == TypeReal) {
			compositeKey.keySize = sizeof(float);
			memcpy(compositeKey.key, highKey, sizeof(float));
		} else {
			compositeKey.keySize = sizeof(int);
			memcpy(compositeKey.key, highKey, sizeof(int));
		}
	}
	return 0;
}

RC IX_ScanIterator::close() {
//	free(this->curPage);
//	free(this->lowCompositeKey.key);
//	free(this->highCompositeKey.key);
	return 0;
}

RC IX_ScanIterator::compare(const void * target, const void * cur,
		const Attribute &attribute) {
	if (attribute.type == TypeVarChar) {
		int TstrLen;
		memcpy(&TstrLen, target, sizeof(int));

		char* Tcharvalue = new char[TstrLen + 1];
		memcpy(Tcharvalue, (char*) target + sizeof(int), TstrLen);
		Tcharvalue[TstrLen] = '\0';

		int strLen;
		memcpy(&strLen, cur, sizeof(int));

		char* charvalue = new char[strLen + 1];
		memcpy(charvalue, (char*) cur + sizeof(int), strLen);
		charvalue[strLen] = '\0';

		int comparison = strcmp(Tcharvalue, charvalue);
		delete[] charvalue;
		delete[] Tcharvalue;

		if (comparison < 0) {
			return LH_SMALL_RH;

		} else if (comparison == 0) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else if (attribute.type == TypeInt) {
		int Tvalue;
		memcpy(&Tvalue, target, sizeof(int));

		int value;
		memcpy(&value, cur, sizeof(int));

		if (Tvalue < value) {
			return LH_SMALL_RH;
		} else if (Tvalue == value) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else {
		float Tvalue;
		memcpy(&Tvalue, target, sizeof(float));

		float value;
		memcpy(&value, cur, sizeof(float));

		if (Tvalue < value) {
			return LH_SMALL_RH;
		} else if (Tvalue == value) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	}
}

//compare with RID
RC IX_ScanIterator::compare(const RID &target, const RID &cur) {
	if (target.pageNum == cur.pageNum) {
		if (target.slotNum < cur.slotNum) {
			return LH_SMALL_RH;
		} else if (target.slotNum == cur.slotNum) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else if (target.pageNum < cur.pageNum) {
		return LH_SMALL_RH;
	} else {
		return LH_BIGGE_RH;
	}
}

RC IX_ScanIterator::compare(const CompositeKey &target, const CompositeKey &cur,
		const Attribute &attribute) {
	int keyCompareResult = compare(target.key, cur.key, attribute);

	if (keyCompareResult != 0)
		return keyCompareResult;
	else
		return this->compare(target.rid, cur.rid);
}

IXFileHandle::IXFileHandle() {
	this->ixReadPageCounter = 0;
	this->ixWritePageCounter = 0;
	this->ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	return FileHandle::collectCounterValues(readPageCount, writePageCount,
			appendPageCount);
}

/******************************** class PageData ********************************/
PageData* PageData::_page_data = 0;
PageData* PageData::instance() {
	if (!_page_data)
		_page_data = new PageData();
	return _page_data;
}
PageData::PageData() {

}
PageData::~PageData() {
	if (_page_data != NULL) {
		delete _page_data;
		_page_data = 0;
	}
}

/**************** meta page management ****************/
RC PageData::getRootInfo(const void * pageData, unsigned int &rootPageNum) {
	memcpy(&rootPageNum, (byte*) pageData, sizeof(int));
	return 0;
}

RC PageData::setRootInfo(void * pageData, const unsigned int rootPageNum) {
	memcpy((byte*) pageData, &rootPageNum, sizeof(int));
	return 0;
}

RC PageData::getAttributeInfo(const void * pageData, Attribute &attribute) {
	int offset;
	int attributeNameSize;
	offset = sizeof(int);
	memcpy(&attributeNameSize, (byte*) pageData + offset, sizeof(int));
	offset += sizeof(int);
	attribute.name.assign((byte *) pageData + offset, attributeNameSize);
	offset += attributeNameSize;
	memcpy(&attribute.type, (byte*) pageData + offset, sizeof(int));
	offset += sizeof(int);
	memcpy(&attribute.length, (byte*) pageData + offset, sizeof(int));
	return 0;
}

RC PageData::setAttributeInfo(void *pageData, const Attribute attribute) {
	int offset = 0;
	int attributeNameSize;
	attributeNameSize = attribute.name.size();
	offset += sizeof(int);
	memcpy((byte*) pageData + offset, &attributeNameSize, sizeof(int));
	offset += sizeof(int);
	memcpy((byte*) pageData + offset, attribute.name.c_str(),
			attributeNameSize);
	offset += attributeNameSize;
	memcpy((byte*) pageData + offset, &attribute.type, sizeof(int));
	offset += sizeof(int);
	memcpy((byte*) pageData + offset, &attribute.length, sizeof(int));
	return 0;
}

/**************** page management in page level ****************/
RC PageData::signNewPage(void * pageData) {
	Size firstAvailableIndex = 0;
	Size totalSlotNum = 0;
	if (this->setPageInfo(pageData, firstAvailableIndex, totalSlotNum) != 0) {
		return -1;
	}
	return 0;
}

RC PageData::deletePage(void * pageData) {
	return -1;
}

/**************** record management in page level ****************/
RC PageData::insertRecord(void * pageData, const void *data,
		const Size &dataSize, unsigned &slotNum) {
	return -1;
}

RC PageData::deleteRecord(void * pageData, const Size &slotNum) {
	return -1;
}

RC PageData::updateRecord(void * pageData, const void * formatData,
		const Size &dataSize,
		const Size &currentSlotNum) {
	return -1;
}

RC PageData::getRecord(const void * pageData, const Size &slotNum,
		void * record, Size &recordSize) {
	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, slotNum, slotStartIndex, slotOffset);
	recordSize = slotOffset;
	memcpy(record, (byte*) pageData + slotStartIndex, slotOffset);
	return 0;
}

/**************** page information ****************/
RC PageData::getPageInfo(const void * pageData,
		Size &firstAvailableIndex,
		Size &totalSlotNum) {
	memcpy(&firstAvailableIndex,
			(byte*) pageData + PAGE_SIZE - 2 * sizeof(Size),
			sizeof(Size));
	memcpy(&totalSlotNum,
			(byte*) pageData + PAGE_SIZE - sizeof(Size),
			sizeof(Size));
	return 0;
}

RC PageData::setPageInfo(void * pageData,
		const Size &firstAvailableIndex,
		const Size &totalSlotNum) {
	memcpy((byte*) pageData + PAGE_SIZE - 2 * sizeof(Size),
			&firstAvailableIndex, sizeof(Size));
	memcpy((byte*) pageData + PAGE_SIZE - sizeof(Size),
			&totalSlotNum, sizeof(Size));
	return 0;
}

RC PageData::getSlotInfo(const void * pageData,
		const Size &slotNum, Size &slotStartIndex,
		Size &slotOffset) {
	Size offset = PAGE_SIZE
			- 2 * sizeof(Size) * (slotNum + 2);
	memcpy(&slotStartIndex, (byte*) pageData + offset,
			sizeof(Size));
	memcpy(&slotOffset, (byte*) pageData + offset + sizeof(Size),
			sizeof(Size));
	return 0;
}

RC PageData::setSlotInfo(void *pageData, const Size &slotNum,
		const Size &slotStartIndex,
		const Size &slotOffset) {
	Size offset = PAGE_SIZE
			- 2 * sizeof(Size) * (slotNum + 2);
	memcpy((byte*) pageData + offset, &slotStartIndex,
			sizeof(Size));
	memcpy((byte*) pageData + offset + sizeof(Size), &slotOffset,
			sizeof(Size));
	return 0;
}

RC PageData::availableInsert(const void * pageData,
		const Size &dataSize, bool &currentPageAvailable) {
	Size space;
	this->availiableSpace(pageData, space);
	if (dataSize > PAGE_SIZE) {
		cout << "dataSize out of bound" << dataSize << endl;
		return -1;
	}
	int temp = space - dataSize - 2 * sizeof(Size);
	if (temp > 0) {
		currentPageAvailable = true;
	} else {
		currentPageAvailable = false;
	}
	return 0;
}

RC PageData::availiableSpace(const void * pageData, Size &space) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	space = PAGE_SIZE - firstAvailableIndex
			- (totalSlotNum + 1) * 2 * sizeof(Size);
	return 0;
}

/******************************** class IXPageData ********************************/
IXPageData* IXPageData::_ix_page_data = 0;
IXPageData* IXPageData::instance() {
	if (!_ix_page_data)
		_ix_page_data = new IXPageData();
	return _ix_page_data;
}
IXPageData::IXPageData() {
}
IXPageData::~IXPageData() {
	if (_ix_page_data != NULL) {
		delete _ix_page_data;
		_ix_page_data = 0;
	}
}

RC IXPageData::insertRecord(void * pageData, const unsigned &slotNum,
		const void * record, const Size &recordSize) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	bool currentPageAvailable = false;
	if (this->availableInsert(pageData, recordSize, currentPageAvailable)
			!= 0) {
		return -1;
	}
	if (!currentPageAvailable) {
		cout << "totalSlotNum" << totalSlotNum << endl;
		cout << "!!!fail to calculate, in insertRecord." << endl;
		return -1;
	}

	if (this->insertSlotInfo(pageData, slotNum) != 0) {
		return -1;
	}

	if (this->insertRecordBySlotNum(pageData, slotNum, record, recordSize)
			!= 0) {
		return -1;
	}

	return 0;
}

RC IXPageData::deleteRecord(void * pageData,
		const Size &slotNum) {

	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	if (slotNum >= totalSlotNum) {
		cout << "deleteSlotNum" << slotNum << endl;
		cout << "totalSlotNum" << totalSlotNum << endl;
		cout << "delete slot out of bound" << endl;
		return -1;
	}

	if (this->deleteRecordBySlotNum(pageData, slotNum) != 0) {
		cout << "fail to deleteRecordBySlotNum in IXPageData::deleteRecord"
				<< endl;
		return -1;
	}

	if (this->deleteSlotInfo(pageData, slotNum) != 0) {
		cout << "fail to deleteSlotInfo in IXPageData::deleteRecord" << endl;
		return -1;
	}

	return 0;
}

RC IXPageData::insertSlotInfo(void * pageData,
		const Size &slotNum) {
	// get page info

	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	// if slot number is the last one
	if (slotNum == totalSlotNum) {
		this->setPageInfo(pageData, firstAvailableIndex, totalSlotNum + 1);
		this->setSlotInfo(pageData, slotNum, firstAvailableIndex, 0);
		return 0;
	}
	// else

	// get old slot info
	Size slotStartIndex;
	Size slotOffset;
	if (this->getSlotInfo(pageData, slotNum, slotStartIndex, slotOffset) != 0) {
		return -1;
	}

	// move following slot backward
	Size slotSize = 2 * sizeof(Size);
	memmove((byte*) pageData + PAGE_SIZE - (totalSlotNum + 2) * slotSize,
			(byte*) pageData + PAGE_SIZE - (totalSlotNum + 1) * slotSize,
			(totalSlotNum - slotNum) * slotSize);

	// set slot info
	slotOffset = 0;
	if (this->setSlotInfo(pageData, slotNum, slotStartIndex, slotOffset) != 0) {
		return -1;
	}

	// set page info
	totalSlotNum++;
	if (this->setPageInfo(pageData, firstAvailableIndex, totalSlotNum) != 0) {
		return -1;
	}
	return 0;
}

RC IXPageData::deleteSlotInfo(void * pageData,
		const Size &slotNum) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, slotNum, slotStartIndex, slotOffset);
	if (slotOffset != 0) {
		cout << "delete record's slot offset is still not 0" << endl;
		return -1;
	}

	if (slotNum == (totalSlotNum - 1)) {
		this->setPageInfo(pageData, firstAvailableIndex, (totalSlotNum - 1));
		return 0;
	}

	memmove(
			(byte*) pageData + PAGE_SIZE
					- totalSlotNum * 2 * sizeof(Size),
			(byte*) pageData + PAGE_SIZE
					- (totalSlotNum + 1) * 2 * sizeof(Size),
			(totalSlotNum - slotNum - 1) * 2 * sizeof(Size));

	this->setPageInfo(pageData, firstAvailableIndex, (totalSlotNum - 1));
	return 0;
}

RC IXPageData::insertRecordBySlotNum(void * pageData,
		const Size &slotNum, const void * record,
		const Size &recordSize) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, slotNum, slotStartIndex, slotOffset);
	if (slotOffset != 0) {
		cout << "logic error in insertRecordBySlotNum" << endl;
		return -1;
	}

	memmove((byte*) pageData + slotStartIndex + recordSize,
			(byte*) pageData + slotStartIndex,
			firstAvailableIndex - slotStartIndex - slotOffset);

	memcpy((byte*) pageData + slotStartIndex, record, recordSize);
	short int moveOffset = recordSize - slotOffset;

	slotOffset = recordSize;
	this->setSlotInfo(pageData, slotNum, slotStartIndex, slotOffset);

	Size followingSlotNum = slotNum + 1;
	while (followingSlotNum < totalSlotNum) {
		Size followingSlotStartIndex;
		Size followingSlotOffset;
		this->getSlotInfo(pageData, followingSlotNum, followingSlotStartIndex,
				followingSlotOffset);
		this->setSlotInfo(pageData, followingSlotNum,
				followingSlotStartIndex + moveOffset, followingSlotOffset);
		followingSlotNum++;
	}

	firstAvailableIndex += moveOffset;
	this->setPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	return 0;
}

RC IXPageData::deleteRecordBySlotNum(void * pageData,
		const Size &slotNum) {
	Size deleteSlotStartIndex;
	Size deleteSlotOffset;
	this->getSlotInfo(pageData, slotNum, deleteSlotStartIndex,
			deleteSlotOffset);

	if (deleteSlotOffset == 0) {
		cerr << "slot record has already been deleted, but condition restored"
				<< endl;
		return -1;
	}

	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	if (slotNum == (totalSlotNum - 1)) {
		this->setSlotInfo(pageData, slotNum, deleteSlotStartIndex, 0);
		this->setPageInfo(pageData, firstAvailableIndex - deleteSlotOffset,
				totalSlotNum);
		return 0;
	}

	memmove((byte*) pageData + deleteSlotStartIndex,
			(byte*) pageData + deleteSlotStartIndex + deleteSlotOffset,
			firstAvailableIndex - deleteSlotStartIndex - deleteSlotOffset);
	short int moveOffset = deleteSlotOffset;

	if (this->setSlotInfo(pageData, slotNum, deleteSlotStartIndex, 0) != 0) {
		return -1;
	}

	Size currentSlotNum = slotNum + 1;
	while (currentSlotNum < totalSlotNum) {
		Size currentSlotStartIndex;
		Size currentSlotOffset;
		this->getSlotInfo(pageData, currentSlotNum, currentSlotStartIndex,
				currentSlotOffset);
		this->setSlotInfo(pageData, currentSlotNum,
				currentSlotStartIndex - moveOffset, currentSlotOffset);
		currentSlotNum++;
	}

	this->setPageInfo(pageData, firstAvailableIndex - deleteSlotOffset,
			totalSlotNum);

	return 0;
}

RC IXPageData::splitWithRecord(void * pageData, void * newPageData,
		const void * insertRecord, const Size &insertRecordSize,
		const Size &insertSlotNum) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);

	if (totalSlotNum == 0) {
		return -1;
	}

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, 0, slotStartIndex, slotOffset);

	Size splitPoint = (firstAvailableIndex - slotStartIndex
			+ insertRecordSize) / 2;


	this->getSlotInfo(pageData, 0, slotStartIndex, slotOffset);

	Size splitSlotNum = 0;
	bool isFormerPage = false;
	while (slotStartIndex < splitPoint && splitSlotNum < totalSlotNum) {
		this->getSlotInfo(pageData, splitSlotNum, slotStartIndex, slotOffset);
		if (splitSlotNum >= insertSlotNum) {
			isFormerPage = true;
			slotStartIndex += insertRecordSize;
		}
		splitSlotNum++;
	}
	if (splitSlotNum == 0 ) {
		cout << "splitSlotNum:" << splitSlotNum << "totalSlotNum"
				<< totalSlotNum << endl;
		cout << "logical error, compositeKey itself huge than one page" << endl;
		return -1;
	}
	splitSlotNum--;

	memcpy(newPageData, pageData, PAGE_SIZE);

	this->deleteFormerPageRecord(pageData, splitSlotNum);

	this->deleteLatterPageRecord(newPageData, splitSlotNum);

	if (isFormerPage) {
		this->insertRecord(pageData, insertSlotNum, insertRecord,
				insertRecordSize);
	} else {
		this->insertRecord(newPageData, insertSlotNum - splitSlotNum,
				insertRecord, insertRecordSize);
	}

	return 0;
}

RC IXPageData::deleteFormerPageRecord(void * pageData,
		const Size &splitSlotNum) {
	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, splitSlotNum, slotStartIndex, slotOffset);
	this->setPageInfo(pageData, slotStartIndex, splitSlotNum);
	return 0;
}

RC IXPageData::deleteLatterPageRecord(void * newPageData,
		const Size &splitSlotNum) {

	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(newPageData, firstAvailableIndex, totalSlotNum);

	Size firstSlotStartIndex;
	Size firstSlotOffset;
	this->getSlotInfo(newPageData, 0, firstSlotStartIndex, firstSlotOffset);

	Size splitSlotStartIndex;
	Size splitSlotOffset;
	this->getSlotInfo(newPageData, splitSlotNum, splitSlotStartIndex,
			splitSlotOffset);

	memmove((byte*) newPageData + firstSlotStartIndex,
			(byte*) newPageData + splitSlotStartIndex,
			firstAvailableIndex - splitSlotStartIndex);

	short int moveOffset = splitSlotStartIndex - firstSlotStartIndex;

	Size currentSlotNum = splitSlotNum;
	while (currentSlotNum < totalSlotNum) {
		Size currentSlotStartIndex;
		Size currentSlotOffset;
		this->getSlotInfo(newPageData, currentSlotNum, currentSlotStartIndex,
				currentSlotOffset);
		this->setSlotInfo(newPageData, currentSlotNum,
				currentSlotStartIndex - moveOffset, currentSlotOffset);
		currentSlotNum++;
	}

	Size slotSize = 2 * sizeof(Size);
	memmove(
			(byte*) newPageData + PAGE_SIZE
					- (totalSlotNum + 1 - splitSlotNum) * slotSize,
			(byte*) newPageData + PAGE_SIZE - (totalSlotNum + 1) * slotSize,
			(totalSlotNum - splitSlotNum) * slotSize);

	this->setPageInfo(newPageData, firstAvailableIndex - moveOffset,
			totalSlotNum - splitSlotNum);

	return 0;
}

RC IXPageData::compare(const void * target, const void * cur,
		const Attribute &attribute) {
	if (attribute.type == TypeVarChar) {
		int TstrLen;
		memcpy(&TstrLen, target, sizeof(int));

		char* Tcharvalue = new char[TstrLen + 1];
		memcpy(Tcharvalue, (char*) target + sizeof(int), TstrLen);
		Tcharvalue[TstrLen] = '\0';

		int strLen;
		memcpy(&strLen, cur, sizeof(int));

		char* charvalue = new char[strLen + 1];
		memcpy(charvalue, (char*) cur + sizeof(int), strLen);
		charvalue[strLen] = '\0';

		int comparison = strcmp(Tcharvalue, charvalue);
		delete[] charvalue;
		delete[] Tcharvalue;

		if (comparison < 0) {
			return LH_SMALL_RH;
		} else if (comparison == 0) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else if (attribute.type == TypeInt) {
		int Tvalue;
		memcpy(&Tvalue, target, sizeof(int));

		int value;
		memcpy(&value, cur, sizeof(int));

		if (Tvalue < value) {
			return LH_SMALL_RH;
		} else if (Tvalue == value) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else {
		float Tvalue;
		memcpy(&Tvalue, target, sizeof(float));

		float value;
		memcpy(&value, cur, sizeof(float));

		if (Tvalue < value) {
			return LH_SMALL_RH;
		} else if (Tvalue == value) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	}
}

RC IXPageData::compare(const RID &target, const RID &cur) {
	if (target.pageNum == cur.pageNum) {
		if (target.slotNum < cur.slotNum) {
			return LH_SMALL_RH;
		} else if (target.slotNum == cur.slotNum) {
			return LH_EQUAL_RH;
		} else {
			return LH_BIGGE_RH;
		}
	} else if (target.pageNum < cur.pageNum) {
		return LH_SMALL_RH;
	} else {
		return LH_BIGGE_RH;
	}
}

RC IXPageData::compare(const CompositeKey &target, const CompositeKey &cur,
		const Attribute &attribute) {
	int keyCompareResult = compare(target.key, cur.key, attribute);

	if (keyCompareResult != 0)
		return keyCompareResult;
	else
		return this->compare(target.rid, cur.rid);
}

/******************************** class IndexPageData ********************************/
IndexPageData* IndexPageData::_index_page_data = 0;
IndexPageData * IndexPageData::instance() {
	if (!_index_page_data)
		_index_page_data = new IndexPageData();

	return _index_page_data;
}
IndexPageData::IndexPageData() {

}
IndexPageData::~IndexPageData() {
	if (_index_page_data != NULL) {
		delete _index_page_data;
		_index_page_data = 0;
	}
}

RC IndexPageData::signNewPage(void * pageData) {
	int temp = IS_INDEX_PAGE;
	memcpy(pageData, &temp, sizeof(int));
	this->setPageInfo(pageData, sizeof(int), 0);
	return 0;
}

RC IndexPageData::pageType(const void * pageData, int &pageType) {
	memcpy(&pageType, pageData, sizeof(int));
	return 0;
}

RC IndexPageData::insertFirstIndex(void * pageData, const PageNum &pageNum) {
	void * record = malloc(sizeof(PageNum));
	memcpy(record, &pageNum, sizeof(PageNum));
	Size recordSize = sizeof(PageNum);
	if (this->insertRecord(pageData, 0, record, recordSize) != 0) {
		free(record);
		return -1;
	}
	free(record);
	return 0;
}

RC IndexPageData::insertIndexRecord(void * pageData, CompositeKey &compositeKey,
		const PageNum &pageNum, const Attribute &attribute) {
	Size slotNum;
	this->findCompositeKeyPosition(pageData, compositeKey, attribute, slotNum);
	slotNum++;

	void * indexRecord;
	indexRecord = compositeKey.key;
	Size indexRecordSize = compositeKey.keySize;
	memcpy((byte*) indexRecord + indexRecordSize, &compositeKey.rid.pageNum,
			sizeof(compositeKey.rid.pageNum));
	indexRecordSize += sizeof(compositeKey.rid.pageNum);
	memcpy((byte*) indexRecord + indexRecordSize, &compositeKey.rid.slotNum,
			sizeof(compositeKey.rid.slotNum));
	indexRecordSize += sizeof(compositeKey.rid.slotNum);
	memcpy((byte*) indexRecord + indexRecordSize, &pageNum, sizeof(PageNum));
	indexRecordSize += sizeof(PageNum);

	this->insertRecord(pageData, slotNum, indexRecord, indexRecordSize);

	return 0;
}

RC IndexPageData::getCompositeKey(const void * pageData,
		const Size &slotNum, CompositeKey &compositeKey) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (slotNum >= totalSlotNum || slotNum < 1) {
		cout << "error, do not exist composite key" << endl;
		return -1;
	}

	void * record;
	record = compositeKey.key;
	Size recordSize;
	if (this->getRecord(pageData, slotNum, record, recordSize) != 0) {
		return -1;
	}
	compositeKey.keySize = recordSize - sizeof(RID) - sizeof(PageNum);

	memcpy(&compositeKey.rid.pageNum, (byte*) record + compositeKey.keySize,
			sizeof(compositeKey.rid.pageNum));
	memcpy(&compositeKey.rid.slotNum,
			(byte*) record + compositeKey.keySize
					+ sizeof(compositeKey.rid.pageNum),
			sizeof(compositeKey.rid.slotNum));

	return 0;
}

RC IndexPageData::getKey(const void * pageData,
		const Size &keyNum, void * key,
		Size &keySize) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (keyNum >= totalSlotNum || keyNum < 1) {
		cout << "key access out of bound" << endl;
		return -1;
	}

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, keyNum, slotStartIndex, slotOffset);

	keySize = slotOffset - sizeof(RID) - sizeof(PageNum);
	memcpy(key, (byte*) pageData + slotStartIndex, keySize);
	return 0;
}

RC IndexPageData::getValue(const void * pageData,
		const Size &valueNum, PageNum &pageNum) {

	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (valueNum >= totalSlotNum) {
		return -1;
	}

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, valueNum, slotStartIndex, slotOffset);
	memcpy(&pageNum,
			(byte*) pageData + slotStartIndex + slotOffset - sizeof(PageNum),
			sizeof(PageNum));

	return 0;
}

RC IndexPageData::split(void * pageData, void * newPageData,
		const Attribute &attribute, CompositeKey &compositeKey,
		PageNum pageNum) {

	// find insert position
	Size insertSlotNum;
	if (this->findCompositeKeyPosition(pageData, compositeKey, attribute,
			insertSlotNum) == -1) {
		cout << "fail to findCompositeKeyPosition" << endl;
		return -1;
	}
	insertSlotNum++;

	// package to index record
	void * record;
	record = compositeKey.key;
	Size recordSize;
	if (this->packageIndexRecord(compositeKey, pageNum, record, recordSize)
			!= 0) {
		cout << "fail to IndexPageData::split" << endl;
		return -1;
	}

	// split with index record
	if (this->splitWithRecord(pageData, newPageData, record, recordSize,
			insertSlotNum) != 0) {
		cout << "fail to splitWithRecord in IndexPageData::split" << endl;
		return -1;
	}

	// get first record from second page
	if (this->getRecord(newPageData, 0, record, recordSize) != 0) {
		cout << "fail to getRecord in IndexPageData::split" << endl;
		return -1;
	}

	// return key
	compositeKey.key = record;
	if (this->unpackIndexRecord(record, recordSize, compositeKey, pageNum)
			!= 0) {
		cout << "fail to unpackIndexRecord in IndexPageData::split" << endl;
		return -1;
	}

	// reinsert page number in to new page data
	if (this->deleteRecord(newPageData, 0) != 0) {
		cout << "fail to deleteRecord in IndexPageData::split" << endl;
		return -1;
	}

	if (this->insertFirstIndex(newPageData, pageNum) != 0) {
		cout << "fail to insertFirstIndex in IndexPageData::split" << endl;
		return -1;
	}

	return 0;
}

RC IndexPageData::findCompositeKeyPosition(const void * pageData,
		const CompositeKey &targetCompositeKey, const Attribute &attribute,
		Size &slotNum) {
	if (targetCompositeKey.key == NULL) {
		cout << "Key from composite key is null" << endl;
		return -1;
	}

	CompositeKey curCompositeKey;
	curCompositeKey.key = malloc(PAGE_SIZE);

	Size totalSlotNum, firstAvailabeIndex;
	RC rc;
	slotNum = 1;
	this->getPageInfo(pageData, firstAvailabeIndex, totalSlotNum);
	if (totalSlotNum == 0) {
		return -1;
	}
	while (slotNum < totalSlotNum) {
		int getKeyResult = this->getCompositeKey(pageData, slotNum,
				curCompositeKey);
		if (getKeyResult == -1) {
			return -1;
		}

		int compareResult = this->compare(targetCompositeKey, curCompositeKey,
				attribute);
		if (compareResult == LH_BIGGE_RH || compareResult == LH_EQUAL_RH) {
			slotNum = slotNum + 1;
		}

		if (compareResult == LH_SMALL_RH) {
			rc = CKEY_FIND_BIGGER;
			break;
		}
	}

	if (slotNum == totalSlotNum) {
		rc = CKEY_EXCEED;
	}

	slotNum = slotNum - 1;
	if (slotNum >= totalSlotNum) {
		return -1;
	}

	free(curCompositeKey.key);

	return rc;
}

RC IndexPageData::indexPageToString(const void * pageData,
		const Attribute &attribute, string &output) {
	Size slotNum = 1, temp, totalSlotNum;
	CompositeKey compositeKey;
	compositeKey.key = malloc(PAGE_SIZE);
	RC rc;
	this->getPageInfo(pageData, temp, totalSlotNum);
	bool notFirst = false;
	output = "\"keys\": [";
	while (slotNum < totalSlotNum) {
		if (slotNum != 0)
			rc = this->getCompositeKey(pageData, slotNum, compositeKey);
		if (notFirst){
			output = output + ", ";
		}
		output = output + "\"";
		string output2 = "";
		this->compositeKeyToString(compositeKey, attribute, output2);
		output = output + output2;
		output = output + "\"";
		notFirst = true;
		slotNum = slotNum + 1;
	}
	output = output + "],";
	free(compositeKey.key);
	return 0;
}

RC IndexPageData::compositeKeyToString(const CompositeKey &compositeKey,
		const Attribute &attribute, string &output) {
	if (attribute.type == TypeInt) {
		int i;
		memcpy(&i, compositeKey.key, sizeof(int));
		output = to_string(i);
	} else if (attribute.type == TypeReal) {
		float f;
		memcpy(&f, compositeKey.key, sizeof(float));
		output = to_string(f);
	} else if (attribute.type == TypeVarChar) {
		int len;
		memcpy(&len, compositeKey.key, sizeof(int));
		output.assign((byte *) compositeKey.key + 4, len);
	}
	return 0;
}

/* protected */
RC IndexPageData::packageIndexRecord(CompositeKey &compositeKey,
		const PageNum &pageNum, void * indexRecord,
		Size &indexRecordSize) {
	indexRecordSize = compositeKey.keySize + sizeof(RID) + sizeof(PageNum);
	memcpy((byte*) indexRecord + compositeKey.keySize,
			&compositeKey.rid.pageNum, sizeof(compositeKey.rid.pageNum));
	memcpy(
			(byte*) indexRecord + compositeKey.keySize
					+ sizeof(compositeKey.rid.pageNum),
			&compositeKey.rid.slotNum, sizeof(compositeKey.rid.slotNum));
	memcpy((byte*) indexRecord + compositeKey.keySize + sizeof(RID), &pageNum,
			sizeof(PageNum));
	return 0;
}

RC IndexPageData::unpackIndexRecord(void * indexRecord,
		const Size &indexRecordSize, CompositeKey &compositeKey,
		PageNum &pageNum) {
	compositeKey.keySize = indexRecordSize - sizeof(RID) - sizeof(PageNum);
	memcpy(&compositeKey.rid.pageNum,
			(byte*) indexRecord + compositeKey.keySize,
			sizeof(compositeKey.rid.pageNum));
	memcpy(&compositeKey.rid.slotNum,
			(byte*) indexRecord + compositeKey.keySize
					+ sizeof(compositeKey.rid.pageNum),
			sizeof(compositeKey.rid.slotNum));
	memcpy(&pageNum, (byte*) indexRecord + compositeKey.keySize + sizeof(RID),
			sizeof(PageNum));
	return 0;
}


/******************************** class LeafPageData ********************************/
LeafPageData* LeafPageData::_leaf_page_data = 0;
LeafPageData * LeafPageData::instance() {
	if (!_leaf_page_data)
		_leaf_page_data = new LeafPageData();

	return _leaf_page_data;
}
LeafPageData::LeafPageData() {

}
LeafPageData::~LeafPageData() {
	if (_leaf_page_data != NULL) {
		delete _leaf_page_data;
		_leaf_page_data = 0;
	}
}

RC LeafPageData::signNewPage(void * pageData) {
	int temp = IS_LEAF_PAGE;
	memcpy(pageData, &temp, sizeof(int));
	this->setNextPageNum(pageData, 0);
	this->setPageInfo(pageData, sizeof(int) + sizeof(PageNum), 0);
	return 0;
}

RC LeafPageData::pageType(const void * pageData, int &pageType) {
	memcpy(&pageType, pageData, sizeof(int));
	return 0;
}

RC LeafPageData::setNextPageNum(void * pageData, const PageNum &pageNum) {
	memcpy((byte*) pageData + sizeof(int), &pageNum, sizeof(PageNum));
	return 0;
}

RC LeafPageData::getNextPageNum(const void * pageData, PageNum &pageNum) {
	memcpy(&pageNum, (byte*) pageData + sizeof(int), sizeof(PageNum));
	return 0;
}

RC LeafPageData::insertLeafRecord(void * pageData,
		const CompositeKey &compositeKey, const Attribute &attribute) {
	Size slotNum;
	RC rc = this->findCompositeKeyPosition(pageData, compositeKey, attribute,
			slotNum);
	if (rc == CKEY_EQUAL) {
		cout << "duplicate insert" << endl;
		return -1;
	}

	void * leafRecord;
	leafRecord = compositeKey.key;
	Size leafRecordSize = compositeKey.keySize;
	memcpy((byte*) leafRecord + leafRecordSize, &compositeKey.rid.pageNum,
			sizeof(compositeKey.rid.pageNum));
	leafRecordSize += sizeof(compositeKey.rid.pageNum);
	memcpy((byte*) leafRecord + leafRecordSize, &compositeKey.rid.slotNum,
			sizeof(compositeKey.rid.slotNum));
	leafRecordSize += sizeof(compositeKey.rid.slotNum);

	this->insertRecord(pageData, slotNum, leafRecord, leafRecordSize);

	return 0;
}

RC LeafPageData::getCompositeKey(const void * pageData,
		const Size &slotNum, CompositeKey &compositeKey) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (slotNum >= totalSlotNum) {
		return -1;
	}

	void * record;
	record = compositeKey.key;
	Size recordSize;
	if (this->getRecord(pageData, slotNum, record, recordSize) != 0) {
		return -1;
	}
	compositeKey.keySize = recordSize - sizeof(RID);
	memcpy(&compositeKey.rid.pageNum, (byte*) record + compositeKey.keySize,
			sizeof(compositeKey.rid.pageNum));
	memcpy(&compositeKey.rid.slotNum,
			(byte*) record + compositeKey.keySize
					+ sizeof(compositeKey.rid.pageNum),
			sizeof(compositeKey.rid.slotNum));

	return 0;
}

RC LeafPageData::getKey(const void * pageData, const Size &keyNum,
		void * key, Size &keySize) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (keyNum >= totalSlotNum) {
		return -1;
	}

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, keyNum, slotStartIndex, slotOffset);

	keySize = slotOffset - sizeof(RID);
	memcpy(key, (byte*) pageData + slotStartIndex, keySize);
	return 0;
}

RC LeafPageData::getValue(const void * pageData,
		const Size &valueNum, RID &rid) {
	Size firstAvailableIndex;
	Size totalSlotNum;
	this->getPageInfo(pageData, firstAvailableIndex, totalSlotNum);
	if (valueNum >= totalSlotNum) {
		return -1;
	}

	Size slotStartIndex;
	Size slotOffset;
	this->getSlotInfo(pageData, valueNum, slotStartIndex, slotOffset);

	memcpy(&rid.pageNum,
			(byte*) pageData + slotStartIndex + slotOffset - sizeof(RID),
			sizeof(rid.pageNum));
	memcpy(&rid.slotNum,
			(byte*) pageData + slotStartIndex + slotOffset - sizeof(RID)
					+ sizeof(rid.slotNum), sizeof(rid.slotNum));
	return 0;
}

RC LeafPageData::split(void * pageData, void * newPageData,
		const PageNum &newPageNum, const Attribute &attribute,
		CompositeKey &compositeKey) {
	Size insertSlotNum;
	RC rc = this->findCompositeKeyPosition(pageData, compositeKey, attribute,
			insertSlotNum);

	if (rc == CKEY_EQUAL) {
		cout << "LeafPageData::split is duplicate" << endl;
		return -1;
	}

	// package to index record, append rid to compositekey.key
	void * leafRecord;
	Size leafRecordSize;
	leafRecord = compositeKey.key;
	leafRecordSize = compositeKey.keySize;
	memcpy((byte *) leafRecord + leafRecordSize, &compositeKey.rid.pageNum,
			sizeof(compositeKey.rid.pageNum));
	leafRecordSize += sizeof(compositeKey.rid.pageNum);
	memcpy((byte *) leafRecord + leafRecordSize, &compositeKey.rid.slotNum,
			sizeof(compositeKey.rid.slotNum));
	leafRecordSize += sizeof(compositeKey.rid.slotNum);

	// split with index record
	this->splitWithRecord(pageData, newPageData, leafRecord, leafRecordSize,
			insertSlotNum);

	// set first page pointer
	this->setNextPageNum(pageData, newPageNum);

	// get first record from second page
	this->getCompositeKey(newPageData, 0, compositeKey);
	// return compositeKey

	return 0;
}

RC LeafPageData::findCompositeKeyPosition(const void * pageData,
		const CompositeKey &targetCompositeKey, const Attribute &attribute,
		Size &slotNum) {
	if (targetCompositeKey.key == NULL) {
		slotNum = 0;
		return -1;
	}

	CompositeKey curCompositeKey;
	curCompositeKey.key = malloc(PAGE_SIZE);

	RC rc;
	slotNum = 0;
	Size totalSlotNum, firstAvailabeIndex;
	this->getPageInfo(pageData, firstAvailabeIndex, totalSlotNum);

	while (slotNum < totalSlotNum) {
		int getKeyResult = this->getCompositeKey(pageData, slotNum,
				curCompositeKey);
		if (getKeyResult == -1) {
			cout << "[findCompositeKeyPosition] accessing invalid slotNum  "
					<< endl;
			return -1;
		}

		int compareResult = this->compare(targetCompositeKey, curCompositeKey,
				attribute);

		// targetCompositeKey == curCompositeKey
		if (compareResult == LH_EQUAL_RH) {
			rc = CKEY_EQUAL;
			break;
		}

		// targetCompositeKey < curCompositeKey
		if (compareResult == LH_SMALL_RH) {
			rc = CKEY_FIND_BIGGER;
			break;
		}

		// targetCompositeKey > curCompositeKey
		slotNum = slotNum + 1;
	}

	if (slotNum == totalSlotNum) {
		rc = CKEY_EXCEED;
	}

	free(curCompositeKey.key);

	return rc;
}

RC LeafPageData::findNextCompositeKeyPosition(const void * pageData,
		const CompositeKey &targetCompositeKey, const Attribute &attribute,
		Size &slotNum) {
	if (targetCompositeKey.key == NULL) {
		cout << "Should not use null to find next composite key" << endl;
		return -1;
	}

	CompositeKey curCompositeKey;
	curCompositeKey.key = malloc(PAGE_SIZE);

	RC rc;
	slotNum = 0;
	Size totalSlotNum, firstAvailabeIndex;
	this->getPageInfo(pageData, firstAvailabeIndex, totalSlotNum);
	while (slotNum < totalSlotNum) {
		int getKeyResult = this->getCompositeKey(pageData, slotNum,
				curCompositeKey);
		if (getKeyResult == -1) {
			free(curCompositeKey.key);
			cout << "[findCompositeKeyPosition] accessing invalid slotNum  "
					<< endl;
			return -1;
		}

		int compareResult = this->compare(targetCompositeKey, curCompositeKey,
				attribute);

		if (compareResult == LH_SMALL_RH) {
			rc = CKEY_FIND_BIGGER;
			break;
		}

		slotNum = slotNum + 1;
	}

	if (slotNum == totalSlotNum) {
		rc = CKEY_CHANGE_PAGE;
	}

	free(curCompositeKey.key);
	return rc;
}

RC LeafPageData::leafPageToString(const void * pageData,
		const Attribute &attribute, string &output) {
	Size slotNum = 0;
	Size keySize;
	string tempstring;
	void * key = malloc(PAGE_SIZE);
	RID rid;
	output = "{\"keys\": [";
	while(this->getKey(pageData, slotNum, key, keySize) != -1
			&& this->getValue(pageData, slotNum, rid) != -1) {
		output = output + "\"";
		if (attribute.type == TypeInt) {
			int i;
			memcpy(&i, key, sizeof(int));
			tempstring = to_string(i);
		} else if (attribute.type == TypeReal) {
			float f;
			memcpy(&f, key, sizeof(float));
			tempstring = to_string(f);
		} else if (attribute.type == TypeVarChar) {
			int len;
			memcpy(&len, key, sizeof(int));
			tempstring.assign((byte *) key + 4, len);
		}
		output = output + tempstring;
		output = output + ":(";
		output = output + to_string(rid.pageNum);
		output = output + ",";
		output = output + to_string(rid.slotNum);
		output = output + ")\",";
		slotNum = slotNum + 1;
	}
	output = output + "]}";
	free(key);
	return 0;
}

