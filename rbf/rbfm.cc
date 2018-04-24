#include "rbfm.h"

/**
 * create interator
 * every iterator needs it to initial
 */
RC RBFM_ScanIterator::createRBFM_ScanIterator(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttributeName, const CompOp compOp,
		const void * value, const vector<string> &attributeNames) {
	this->fileHandle = fileHandle;
	this->recordDescriptor = recordDescriptor;
	this->attributeNames = attributeNames;
	this->conditionAttributeName = conditionAttributeName;
	this->compOp = compOp;

	// initial rid
	// attention set nextRid start before first slot
	this->nextRid.pageNum = 1;
	this->nextRid.slotNum = 1;
	// currentRid will be initial by getNextRid
	this->currentRid.pageNum = 0;
	this->currentRid.slotNum = 0;
	this->getNextRid();

	/* value is not API data but may be NULL */
	if(value != NULL){
		memcpy(this->value, value, PAGE_SIZE);
		this->valueNull = false;
	} else {
		this->valueNull = true;
	}
	// check compOp
	if (compOp == NO_OP) {
		return 0;
	} else {
		// else
		// check valueType
		bool getAttributePosition = false;
		for (int i = 0; i < recordDescriptor.size(); i++) {
			if (recordDescriptor[i].name == conditionAttributeName) {
				this->valueType = recordDescriptor[i].type;
				getAttributePosition = true;
				break;
			}
		}
		// check valueType initial
		if (!getAttributePosition) {
			cout
					<< "!!!not found attribute type in RBFM_ScanIterator::createRBFM_ScanIterator"
					<< endl;
			return -1;
		}
	}
	return 0;
}

RC RBFM_ScanIterator::close() {
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void * data) {
	RC rc = this->next(data);
	if (rc == RBFM_EOF) {
		return RBFM_EOF;
	}else if(rc == -1){
		cout << "!!!error" << endl;
		return -1;
	}
	rid = this->currentRid;
	return 0;
}

/**
 * has next
 * return: hasNext
 * only show if there is next rid, but maybe not available
 */
RC RBFM_ScanIterator::hasNext(bool &hasNext) {
	hasNext = this->exitNext;
	return 0;
}

/**
 * next
 * return: data
 * suppose: current state: already get the right attributes and return
 */
RC RBFM_ScanIterator::next(void * data) {
	bool hasNext = false;
	if (this->hasNext(hasNext) != 0) {
		return -1;
	}
	if (hasNext == false) {
		return RBFM_EOF;
	}

	// start next
//	bool exitNextRid;
	if (this->compOp == NO_OP) {
		// get current data
		if (this->getData(data) != 0) {
			return -1;
		}
		// set next rid to next position
		if (this->getNextRid() != 0) {
			return -1;
		}

		return 0;
	} else { // compare requirement
		bool compResult = false;
		// current rid is available so exit next rid is true;
		void * compareAttribute = malloc(PAGE_SIZE);
		// find until next rid is end
		while (this->exitNext) {
//			cout << "read record at page:" << this->nextRid.pageNum << "solt:" << this->nextRid.slotNum<< endl;
			RecordBasedFileManager::instance()->readAttribute(this->fileHandle,
					this->recordDescriptor, this->nextRid, this->conditionAttributeName,
					compareAttribute);
//			cout << "compareAttribute:" << *(int *) ((char *) compareAttribute + 1)<< endl;
			// compare current attribute to value
			if (this->compare(compareAttribute, compResult)!=0) {
				free(compareAttribute);
				return -1;
			}
//			cout << "compResult" << compResult << endl;
			// if find it, break
			if (compResult == true) {
//				cout << "find it" << endl;
				break;
			}
			// jump to next rid
			if (this->getNextRid() != 0) {
				free(compareAttribute);
				return -1;
			}
		}
		free(compareAttribute);
		// if next rid is end, file is end
		if (!this->exitNext) {
			return RBFM_EOF;
		}
		// else
		// get data
		if (this->getData(data) != 0) {
			return -1;
		}
		// get next rid
		if (this->getNextRid() != 0) {
			return -1;
		}
		// set exit next
//		if (!exitNextRid) {
//			this->exitNext = false;
//		}
		return 0;
	}
}

RC RBFM_ScanIterator::compare(const void * compareValue, bool &compareResult) {
	if (this->compOp == NO_OP) {
		compareResult = true;
		return 0;
	}
	// compare null indicator
	// check if compare is null
	bool compareNullBit = false;
	compareNullBit = *(byte*)compareValue & 0x80;

	// check if condition is null
	if(this->valueNull){
		if(compOp == EQ_OP){
			if(compareNullBit){
				compareResult = true;
				return 0;
			}
		}else if(compOp == NE_OP){
			if(!compareNullBit){
				compareResult = true;
				return 0;
			}
		}
	}else if (this->valueType == TypeInt) { // compare int
		// check if null
		if (compareNullBit) {
			compareResult = false;
			return 0;
		}
		int compareAttribute;
		int conditionAttribute;
		memcpy(&compareAttribute, (byte*) compareValue + 1, sizeof(int));
		memcpy(&conditionAttribute, this->value, sizeof(int));
		if (compOp == EQ_OP) {
			compareResult = compareAttribute == conditionAttribute;
		} else if (compOp == LT_OP) {
			compareResult = compareAttribute < conditionAttribute;
		} else if (compOp == LE_OP) {
			compareResult = compareAttribute <= conditionAttribute;
		} else if (compOp == GT_OP) {
			compareResult = compareAttribute > conditionAttribute;
		} else if (compOp == GE_OP) {
			compareResult =compareAttribute  >= conditionAttribute;
		} else if (compOp == NE_OP) {
			compareResult = compareAttribute !=  conditionAttribute ;
		}
		return 0;
	} else if (this->valueType == TypeReal) {
		// check if null
		if (compareNullBit) {
			compareResult = false;
			return 0;
		}
		float conditionAttribute;
		float compareAttribute;
		memcpy(&conditionAttribute, (byte*) this->value, sizeof(float));
//		memcpy(&conditionAttribute, (byte*) this->value + 1, sizeof(float));
		memcpy(&compareAttribute, (byte*) compareValue + 1, sizeof(float));
		if (compOp == EQ_OP) {
			compareResult = compareAttribute  == conditionAttribute;
		}else if (compOp == LT_OP) {
			compareResult =compareAttribute <  conditionAttribute;
		}else if (compOp == LE_OP) {
			compareResult = compareAttribute <= conditionAttribute;
		}else if (compOp == GT_OP) {
			compareResult = compareAttribute > conditionAttribute;
		}else if (compOp == GE_OP) {
			compareResult = compareAttribute >= conditionAttribute;
		}else if (compOp == NE_OP) {
			compareResult = compareAttribute != conditionAttribute;
		}
		return 0;
	} else if (this->valueType == TypeVarChar) {
		// check if null
		if (compareNullBit) {
			compareResult = false;
			return 0;
		}
		int conditionLen;
		int compareLen;
//		memcpy(&conditionLen, (byte*) this->value + sizeof(byte), sizeof(int));
		memcpy(&conditionLen, (byte*) this->value, sizeof(int));
		memcpy(&compareLen, (byte*) compareValue + sizeof(byte), sizeof(int));
		char * conditionAttribute = new char[conditionLen + 1];
		char * compareAttribute = new char[compareLen + 1];
//		memcpy(conditionAttribute,
//				(byte*) this->value + sizeof(byte) + sizeof(int),
//				conditionLen * sizeof(char));
		memcpy(conditionAttribute,
				(byte*) this->value + sizeof(int),
				conditionLen * sizeof(char));
		conditionAttribute[conditionLen] = '\0';
		memcpy(compareAttribute,
				(byte*) compareValue + sizeof(byte) + sizeof(int),
				compareLen * sizeof(char));
		compareAttribute[compareLen] = '\0';
		if (compOp == EQ_OP) {
			compareResult = (strcmp(compareAttribute, conditionAttribute) == 0);
		}else if (compOp == LT_OP) {
			compareResult = (strcmp(compareAttribute, conditionAttribute) < 0);
		}else if (compOp == LE_OP) {
			compareResult = (strcmp(compareAttribute, conditionAttribute) <= 0);
		}else if (compOp == GT_OP) {
			compareResult = (strcmp(compareAttribute, conditionAttribute) > 0);
		}else if (compOp == GE_OP) {
			compareResult = (strcmp(compareAttribute, conditionAttribute) >= 0);
		}else if (compOp == NE_OP) {
			compareResult = (strcmp( compareAttribute, conditionAttribute) != 0);
		}
		delete[] conditionAttribute;
		delete[] compareAttribute;
		return 0;
	}
	return -1;
}

RC RBFM_ScanIterator::getNextRid() {
	// save current rid
	/* not efficiency but for safty */
	// update currentRid
	this->currentRid = this->nextRid;
	// update nextRid
	return RecordBasedFileManager::instance()->getNextValidRid(this->fileHandle,
			this->nextRid, this->exitNext);
}

RC RBFM_ScanIterator::getData(void * data) {
//	cout << "RBFM_ScanIterator::getData get data from page:" << this->nextRid.pageNum << "slot:" << this->nextRid.slotNum << endl;
//	cout << "attribute:" << this->attributeNames.size()<< endl;
	RC rc = RecordBasedFileManager::instance()->readAttributes(this->fileHandle,
			this->recordDescriptor, this->nextRid, this->attributeNames, data);
//	cout << "data in getData" <<  *(int *) ((char *) data + 1) << endl;
//	cout << "nullIndicator in getData:" << *(byte*)data<< endl;
//	cout << "...records from attributes..." << endl;
//	RecordBasedFileManager::instance()->printRecord(recordDescriptor, data);
//	cout << "...records from read..." << endl;
//	void * tempdata = malloc(PAGE_SIZE);
//	RecordBasedFileManager::instance()->readRecord(fileHandle, recordDescriptor, this->nextRid, tempdata);
//	RecordBasedFileManager::instance()->printRecord(recordDescriptor, tempdata);
//	free(tempdata);
//	cout << "...end..." << endl;
	return rc;
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	pagedFileManager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
	if (_rbf_manager != NULL) {
		delete _rbf_manager;
		_rbf_manager = 0;
	}
}

/**
 * create file
 * param: fileName
 * create the first page as info page and the second page as start page
 * at the same time
 */
RC RecordBasedFileManager::createFile(const string &fileName) {

	if (pagedFileManager->createFile(fileName) != 0)
		return -1;

// append one page;
	FileHandle fileHandle;
	if (this->openFile(fileName, fileHandle) != 0)
		return -1;
	if (this->appendSignedPageInFile(fileHandle) != 0)
		return -1;
	if (this->appendSignedPageInFile(fileHandle) != 0)
		return -1;

	return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pagedFileManager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return pagedFileManager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pagedFileManager->closeFile(fileHandle);
}

/**
 * insert record into page
 * param: fileHandle, recordDescriptor, data
 * return: rid
 * First, data2FormatDataNCalculateDataSize;
 * Second, findAvailablePage;
 * Third, readPage;
 * Fifth, insertRecordToPage;
 * Sixth, writePage;
 */
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	if (recordDescriptor.size() == 0) {
		cout
				<< "record descriptor is empty in RecordBasedFileManager::insertRecord"
				<< endl;
		return -1;
	}


// format data
// get format data size
	unsigned short int formatDataSize;
	void * formatData = malloc(PAGE_SIZE);

	if (this->data2FormatDataNCalculateDataSize(recordDescriptor, data, formatData,
			formatDataSize)) {
		free(formatData);
		cout << "!!!fail to format data in RecordBasedFileManager::insertRecord"
				<< endl;
		return -1;
	}

	if (this->insertFormatDataToFile(fileHandle, formatData, formatDataSize,
			rid)) {
		free(formatData);
		cout << "!!!fail to insert format data to file in InsertRecord" << endl;
		return -1;
	}

	free(formatData);
	return 0;
}

/**
 * read record
 * param: fileHandle, recordDescriptor, RID
 * return: data
 * read
 *
 * First, get page data by readPage method;
 * Second, get slot information(start slot index, offsetSlotNum) by getSlotInfo method;
 * Third, copy this part of page data into data.
 */
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int formatDataSize;

	if (this->readFormatDataInFile(fileHandle, rid, formatData, formatDataSize)
			!= 0) {
		free(formatData);
		return -1;
	}

	if (this->formatData2Data(recordDescriptor, formatData, data,
			formatDataSize) != 0) {
		free(formatData);
		cout << "!!!fail to format data to data" << endl;
		return -1;
	}
	free(formatData);
	return 0;
}

/**
 * print record
 * param: recordDescriptor, data
 * format print record by itself length
 * Frist, get null indicator array by recordDescriptor.size()
 * Second, read data after null indicator according to null indicator
 * Third, get information and format print
 */
RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {

	unsigned fieldNum = recordDescriptor.size();

#ifdef DEBUG
	cerr << "field number: " << fieldNum << endl;
#endif

	if (fieldNum == 0) {
		cout
				<< "!!!empty record descriptor in RecordBasedFileManager::printRecord"
				<< endl;
		return -1;
	}

// calculate nullFieldsIndicatorActualSize
	unsigned nullFieldsIndicatorActualSize = ceil((double) fieldNum / CHAR_BIT);

#ifdef DEBUG
	cerr << "null indicator size: " << nullFieldsIndicatorActualSize << endl;
#endif

// read null indicator from raw data
	unsigned char * nullFieldsIndicator = new unsigned char[nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

// set field position point array
	int * fieldPosition = new int[fieldNum];
// initial point offset
	unsigned positionOffset = nullFieldsIndicatorActualSize;

#ifdef DEBUG
	cerr << "position first begin at: " << positionOffset << endl;
#endif

// set buffer
//	byte* buffer = (byte*) malloc(PAGE_SIZE);

	for (unsigned i = 0; i < fieldNum; i++) {

		cout << recordDescriptor[i].name << ":\t";

		bool nullBit = nullFieldsIndicator[i / CHAR_BIT]
				& (1 << (CHAR_BIT - 1 - i % CHAR_BIT));

		if (!nullBit) { // if not null
			AttrType type = recordDescriptor[i].type;
			if (type == TypeInt) {
				int * buffer = new int;
				memcpy(buffer, (byte*) data + positionOffset, sizeof(int));

				cout << *(int*) buffer << "\t";

				delete buffer;
				positionOffset += sizeof(int);
			} else if (type == TypeReal) {
				float * buffer = new float();
				memcpy(buffer, (byte*) data + positionOffset, sizeof(float));

				cout << *(float*) buffer << "\t";

				delete buffer;
				positionOffset += sizeof(float);
			} else {
				int len = 0;
				memcpy(&len, (byte*) data + positionOffset, sizeof(int));
				positionOffset += sizeof(int);

				char * buffer = new char[len + 1];
				memcpy(buffer, (byte*) data + positionOffset,
						len * sizeof(char));
				buffer[len] = '\0';

				cout << (char*) (buffer) << "\t\t";

				delete[] buffer;
				positionOffset += len;
			}
		} else { // if null

			cout << "NULL" << "\t";

			positionOffset += 0;
		}
		fieldPosition[i] = positionOffset;

#ifdef DEBUG
		cerr << "position offset: " << positionOffset << endl;
#endif

	}

	cout << "" << endl;

	delete[] nullFieldsIndicator;
	return 0;
}

/**
 * delete record
 * param:fileHandle, recordDescriptor, rid
 * delete record in the page, just reset the offset of slot to zero
 * step: read page;
 * second: send delete instruction to slot level
 * third: remove the following available slot to forward.
 */
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	if (this->deleteFormatDataInFile(fileHandle, rid) != 0) {
		return -1;
	}
	return 0;
}

/**
 * update record
 * First: update when page have available space
 * Second: update when page have not available space
 */
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {
//	cout << "update record starting..." << endl;
// format data
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int dataSize;
	if (this->data2FormatDataNCalculateDataSize(recordDescriptor, data,
			formatData, dataSize) != 0) {
		free(formatData);
		return -1;
	}
// update format data in file
	if (this->updateFormatDataInFile(fileHandle, formatData, dataSize, rid)
			!= 0) {
		free(formatData);
		return -1;
	}
	return 0;
}

/**
 * read attribute
 * param: filehandle, recordDescriptor, rid, attributeName
 * return: data
 * First, get format data;
 * Second, get attribute according to format data
 */
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string &attributeName, void * data) {

// get attribute position
	bool getAttributePosition = false;
	unsigned short int attributePosition;
	for (unsigned short int i = 0; i < recordDescriptor.size(); i++) {
		if (recordDescriptor[i].name == attributeName) {
			attributePosition = i;
			getAttributePosition = true;
			break;
		}
	}

	if (getAttributePosition == false || attributePosition < 0) {
		cout << "!!!not found attribute position." << endl;
		return -1;
	}

// get formatData
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int formatDataSize;
	if (this->readFormatDataInFile(fileHandle, rid, formatData,
			formatDataSize)) {
		free(formatData);
		return -1;
	}

	unsigned short int dataSize;
	if (this->getAttributeInFormatData(formatData, attributePosition, data,
			dataSize) != 0) {
		free(formatData);
		return -1;
	}

	free(formatData);
	return 0;
}

/**
 * read attributes
 * param: fileHandle, recordDescriptor, rid, attributeNames, data
 * return: data
 * read all suitable attribute according to attributeNames and rid
 */
RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const vector<string> &attributeNames, void * data) {
	unsigned short int dataSize = 0;

	// get formatData
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int formatDataSize;

	if (this->readFormatDataInFile(fileHandle, rid, formatData,
			formatDataSize)) {
		free(formatData);
		return -1;
	}

	// get attributes in page

	// initial null indicator
	unsigned nullFieldsIndicatorActualSize = 1
			+ ((attributeNames.size() - 1) / CHAR_BIT);

	unsigned short int attributePosition;
	void * attribute = malloc(PAGE_SIZE);
	unsigned short int attributeSize;

// copy data and remember null indicator
	byte * nullIndicators = new byte[nullFieldsIndicatorActualSize];
	memset(nullIndicators, 0, nullFieldsIndicatorActualSize);
	// set dataOffset
	dataSize += nullFieldsIndicatorActualSize;

	byte nullIndicatorStandaard = 0x80;
	for (int i = 0; i < attributeNames.size(); i++) {
		bool getAttributePosition = false;

		// find attributePosition in recordDescriptor
		for (unsigned short int j = 0; j < recordDescriptor.size(); j++) {
			if (recordDescriptor[j].name == attributeNames[i]) {
				attributePosition = j;
				getAttributePosition = true;
				break;
			}
		}
		if (getAttributePosition == false) {
			free(attribute);
			free(formatData);
			cout << "!!!not found attribute in attributeDescriptor." << endl;
			return -1;
		}

		// get attribute in format data according to position
		this->getAttributeInFormatData(formatData, attributePosition, attribute,
				attributeSize);

		// copy data
		memcpy((byte*) data + dataSize, (byte*) attribute + sizeof(byte),
				attributeSize - sizeof(byte));
		dataSize += attributeSize - sizeof(byte);

		// set null indicators
		if(attributeSize - sizeof(byte) == 0){
			nullIndicators[i / CHAR_BIT] |= (nullIndicatorStandaard
								>> (i % CHAR_BIT));
		}

		// copy temp null indicator
	}

	// copy nullIndicators
	memcpy(data, nullIndicators, nullFieldsIndicatorActualSize * sizeof(byte));

	delete[] nullIndicators;
//	cout << "print in this:"<< endl;
//	this->printRecord(recordDescriptor, data);
//	cout << "print end in this"<< endl;
	free(formatData);
	return 0;
}

/**
 * scan
 */
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {
	if (rbfm_ScanIterator.createRBFM_ScanIterator(fileHandle, recordDescriptor,
			conditionAttribute, compOp, value, attributeNames) != 0) {
		return -1;
	}
	return 0;
}

/* public method by Bicheng */
/**
 * get next available rid
 * param: fileHandle, rid
 * return: rid, exitNextRid
 * get next available rid, if next rid is a linked record jump it
 * if it is end of file exitNextRid = false;
 */
RC RecordBasedFileManager::getNextValidRid(FileHandle fileHandle, RID &rid,
		bool &exitNextRid) {
	exitNextRid = false;
	// check rid
	if(rid.pageNum < 1 || rid.slotNum < 1){
		return -1;
	}

	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		free(pageData);
		return -1;
	}

	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		free(pageData);
		cout
				<< "!!!fail to get page info in RecordBasedFileManager::getNextValidRid."
				<< endl;
		return -1;
	}

	// try to get next slot in current page
	rid.slotNum++;
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int slotStartIndex;
	unsigned short int slotOffset;
	while (rid.slotNum <= slotNumRecord) {
		// get slot information
		if(this->getSlotInfo(pageData, rid.slotNum, slotStartIndex, slotOffset)!=0){
			free(formatData);
			free(pageData);
			cout << "!!!fail to get slot information in RecordBasedFileManager::getNextValidRid."<< endl;
			return -1;
		}
		// check slot is available
		if (slotOffset != 0) {
			// get data
			if(this->getFormatDataInPage(pageData, rid.slotNum, formatData,
					slotOffset)!=0){
				free(formatData);
				free(pageData);
				return -1;
			}
			// check is linked
			bool isCurrentDataLinked;
			if(this->isFormatDataLinked(formatData, slotOffset, isCurrentDataLinked)!=0){
				free(formatData);
				free(pageData);
				return -1;
			}
			if (!isCurrentDataLinked) {
				exitNextRid = true;
				free(formatData);
				free(pageData);
				return 0;
			}
		}
		rid.slotNum++;
	}

	// try to get next page
	rid.pageNum++;
	while (rid.pageNum < fileHandle.getNumberOfPages()) {
		// reset slot number
		rid.slotNum = 2;
		// reload new page
		fileHandle.readPage(rid.pageNum, pageData);
		// reload page info
		this->getPageInfoByData(pageData, firstAvailableSlotIndex,
				slotNumRecord);

		// travel slot
		while (rid.slotNum <= slotNumRecord) {

			unsigned short int slotStartIndex;
			unsigned short int slotOffset;
			this->getSlotInfo(pageData, rid.slotNum, slotStartIndex,
					slotOffset);
			if (slotOffset != 0) {

				this->getFormatDataInPage(pageData, rid.slotNum, formatData,
						slotOffset);

				bool isCurrentDataLink;
				this->isFormatDataLinked(formatData, slotOffset,
						isCurrentDataLink);

				if (!isCurrentDataLink) {
					// free format data
					free(formatData);
					// free current page
					free(pageData);
					exitNextRid = true;
					return 0;
				}
			}
			rid.slotNum++;
		}
		rid.pageNum++;
	}

	// highlight not exit net rid
	exitNextRid = false;
	// free format data
	free(formatData);
	// free current page
	free(pageData);
	return 0;
}

/* private methods by Bicheng */

/* data management in record level */
/**
 * getAttributeLocation
 */
RC RecordBasedFileManager::getAttributeLocationInFormatData(
		const void * formatData, const unsigned short int &attributePosition,
		unsigned short int &startIndex, unsigned short int &offset) {
	unsigned short int formatDataOffset = sizeof(RID)
			+ attributePosition * sizeof(unsigned short int);
	memcpy(&startIndex, (byte*) formatData + formatDataOffset,
			sizeof(unsigned short int));
	formatDataOffset += sizeof(unsigned short int);
	memcpy(&offset, (byte*) formatData + formatDataOffset,
			sizeof(unsigned short int));
	offset -= startIndex;
	return 0;
}

/**
 * get attribute in format data
 * param: formatData, attributePosition
 * return: attribute, attributeSize
 */
RC RecordBasedFileManager::getAttributeInFormatData(const void * formatData,
		const unsigned short int &attributePosition, void * attribute,
		unsigned short int &attributeSize) {
//	cout << "attributePosition" << attributePosition << endl;

	// find position
	unsigned short int startIndex;
	unsigned short int offset;
	if (this->getAttributeLocationInFormatData(formatData, attributePosition,
			startIndex, offset)) {
		return -1;
	}
	// copy attribute
	memcpy((byte*) attribute + sizeof(byte), (byte*) formatData + startIndex,
			offset);
	/*TODO: different from prof. test version in null indicator number */
	// insert attribute null indicator
	attributeSize = offset + sizeof(byte);
	// TODO: need package
	byte nullIndicator = 0x00;
	// set nullIndicator
	if (offset == 0) {
		nullIndicator = 0x80;
	}
	memcpy(attribute, &nullIndicator, sizeof(byte));
	return 0;
}

/**
 * find first rid in record
 * param: record data
 * return: first rid
 */
RC RecordBasedFileManager::getFirstRidInFormatData(const void * formatData,
		const unsigned short int dataSize, RID &rid) {
	if (dataSize < sizeof(RID)) {
		cout << "!!!get firstRid out of bound in RecordBasedFileManager::getFirstRidInFormatData" << endl;
		return -1;
	}
	memcpy(&rid.pageNum, (byte*) formatData, sizeof(rid.pageNum));
	memcpy(&rid.slotNum, (byte*) formatData + sizeof(rid.pageNum), sizeof(rid.slotNum));
	return 0;
}

/**
 * set first rid
 */
RC RecordBasedFileManager::setFirstRidInFormatData(void * formatData,
		const RID &firstRid, const unsigned short int &dataSize) {
	if (dataSize < sizeof(RID)) {
		cout << "!!!set first rid out of bound in RecordBasedFileManager::setFirstRidInFormatData" << endl;
		return -1;
	}
	memcpy(formatData, &firstRid.pageNum, sizeof(firstRid.pageNum));
	memcpy((byte*) formatData + sizeof(firstRid.pageNum), &firstRid.slotNum,
			sizeof(firstRid.slotNum));
	return 0;
}

/**
 * is current data link to another
 * param: pageData, slotNum
 * return isCurrentDataLink
 */
RC RecordBasedFileManager::isFormatDataLink(const void * formatData,
		const unsigned short int dataSize, bool &isCurrentDataLink) {
	RID firstRid;
	isCurrentDataLink = false;
	if (this->getFirstRidInFormatData(formatData, dataSize, firstRid) != 0) {
		return -1;
	}
	if (firstRid.pageNum != 0) {
		isCurrentDataLink = true;
	}
	return 0;
}

/**
 * is current data linked by another
 * param: pageData, slotNum
 * return isCurrentDataLinked
 */
RC RecordBasedFileManager::isFormatDataLinked(const void * formatData,
		const unsigned short int dataSize, bool &isCurrentDataLinked) {
	RID firstRid;
	isCurrentDataLinked = false;
	if (this->getFirstRidInFormatData(formatData, dataSize, firstRid) != 0) {
		return -1;
	}
	if (firstRid.pageNum == 0 && firstRid.slotNum == 1) {
		isCurrentDataLinked = true;
	}
	return 0;
}

/**
 * data to format data and calculate data size
 * convert data to format data for 0(1) field access in the future and by the way calculate data size
 */
RC RecordBasedFileManager::data2FormatDataNCalculateDataSize(
		const vector<Attribute> &recordDescriptor, const void *data,
		void *formatData, unsigned short int &dataSize) {

	unsigned fieldNum = recordDescriptor.size();

//	cout << "field number:" << fieldNum << endl;

	if (fieldNum == 0) {
		cout << "!!!empty record descriptor" << endl;
		return -1;
	}

// calculate nullFieldsIndicatorActualSize
	unsigned nullFieldsIndicatorActualSize = ceil((double) fieldNum / CHAR_BIT);

// read null indicator from data
	unsigned char * nullFieldsIndicator = new unsigned char [nullFieldsIndicatorActualSize];
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

// initial point offset
// attention: the start of it is from null indicator because of similarity to raw data
	unsigned positionOffset = sizeof(RID)
			+ (fieldNum + 1) * sizeof(unsigned short int)
			+ nullFieldsIndicatorActualSize;

//	cout << "positionOffsetBegin: " << positionOffset << endl;

// before fields data I personally also want to save the null fields indicator;

	unsigned short int* fieldPositions = new unsigned short int[fieldNum + 1];
	fieldPositions[0] = positionOffset;
	unsigned short int dataOffset = nullFieldsIndicatorActualSize;
	for (unsigned i = 0; i < fieldNum; i++) {

		bool nullBit = nullFieldsIndicator[i / CHAR_BIT]
				& (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
		if (!nullBit) { // not null
			AttrType type = recordDescriptor[i].type;
			if (type == TypeInt) {
				dataOffset += sizeof(int);
				positionOffset += sizeof(int);
			} else if (type == TypeReal) {
				dataOffset += sizeof(float);
				positionOffset += sizeof(float);
			} else {
				int len = 0;
				memcpy(&len, (byte*) data + dataOffset, sizeof(int));
				dataOffset += sizeof(int) + len;
				positionOffset += sizeof(int) + len;
			}
		} else {
			// positionOffset += 0;
			// dataOffset += 0;
//			cout << "find null:" << i << endl;
		}

		// record the position offset
		fieldPositions[i + 1] = positionOffset;

//		cout << "positionOffset"<< i+1 << ":" << positionOffset << endl;

	}

	unsigned short int offset = 0;
	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;

	// copy first rid
	memcpy((byte*) formatData + offset, &rid.pageNum, sizeof(rid.pageNum));
	offset += sizeof(rid.pageNum);
	memcpy((byte*) formatData + offset, &rid.slotNum, sizeof(rid.slotNum));
	offset += sizeof(rid.slotNum);

	memcpy((byte*) formatData + offset, fieldPositions,
			sizeof(unsigned short int) * (fieldNum + 1));
	offset += sizeof(unsigned short int) * (fieldNum + 1);

	// position offset is the truly length of data, including null indicator and fields' data.
	memcpy((byte*) formatData + offset, data, positionOffset - offset);
	offset = positionOffset;

	// copy last rid
	memcpy((byte*) formatData + offset, &rid.pageNum, sizeof(rid.pageNum));
	offset += sizeof(rid.pageNum);
	memcpy((byte*) formatData + offset, &rid.slotNum, sizeof(rid.slotNum));
	offset += sizeof(rid.slotNum);

	// return dataSize
	dataSize = offset;

	delete[] nullFieldsIndicator;
	return 0;
}

/**
 * format data without need length
 */
RC RecordBasedFileManager::data2FormatData(
		const vector<Attribute> &recordDescriptor, const void *data,
		void *formatData) {
	unsigned short int dataSize = 0;
	return data2FormatDataNCalculateDataSize(recordDescriptor, data, formatData,
			dataSize);
}

/**
 * convert format data to data
 * param: recordDescriptor, formatData, formatDataSize
 * return: data
 * decode format data
 */
RC RecordBasedFileManager::formatData2Data(
		const vector<Attribute> &recordDescriptor, const void * formatData,
		void * data, const unsigned short int &formatDataSize) {

	unsigned short int offset = 0;
	offset += sizeof(RID)
			+ (recordDescriptor.size() + 1) * sizeof(unsigned short int);
	memcpy(data, (byte*) formatData + offset,
			formatDataSize - offset - sizeof(RID));
	offset = formatDataSize - sizeof(RID);

	return 0;
}

/* record management in page level*/

/**
 * insert record to page at fixed slot number
 * currentSlotNum must be the next of currentSlot in page
 */
RC RecordBasedFileManager::insertRecordToPageAtNextSlot(void * pageData,
		const void * formatData, const unsigned short int &dataSize,
		const unsigned short int &currentSlotNum) {

	// read page information
	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		cout << "!!!fail to get page info, in RecordBasedFileManager::insertRecordToPageAtSlotNumber." << endl;
		return -1;
	}

	// slotNumRecord update
	slotNumRecord++;
	// check currentSlotNum
	if(currentSlotNum != slotNumRecord){
		cout << "!!!currentSlotNum is not the next of slotNumRecord" << endl;
		return -1;
	}

	// copy data into pageData
	memcpy((byte*)pageData + firstAvailableSlotIndex, formatData, dataSize);

	// update slotInfo
	if (this->updateSlotInfo(pageData, currentSlotNum, firstAvailableSlotIndex,
			dataSize) != 0) {
		cout << "!!!fail to update slot information" << endl;
		return -1;
	}

//	cout << "record insert start:"<< firstAvailableSlotIndex<< "dataSize:"<< dataSize << endl;

	// update firstAvailableIndex and slotNumRecord
	firstAvailableSlotIndex += dataSize;
	if (this->updatePageInfo(pageData, firstAvailableSlotIndex, slotNumRecord)
			!= 0) {
		cout << "!!!fail to update page information" << endl;
		return -1;
	}
//	cout << "current slotNum" << slotNumRecord << "firstAvailableSlotIndex" << firstAvailableSlotIndex << endl;
	return 0;
}

/**
 * insertRecordToPage
 * param: fileHanle, data, dataSize, rid
 * save record into page according to rid
 * problem: not use the firstAvailableSlotIndex param from before method, considering concurrent visit
 * First: read page information
 * Second: find available slot number, if not, make a new one
 * Third: insert information into slot, and update page information
 * Forth: insert record
 */

RC RecordBasedFileManager::insertRecordToPage(void * pageData, const void *data,
		const unsigned short int &dataSize, unsigned &slotNum) {

	// get page information
	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		cout << "!!!fail to get page info, in insertRecord2Slot." << endl;
		return -1;
	}

	/* TODO: find available slot number */
	// find an available slot
	unsigned short int currentSlotNum = 2;
	unsigned short int currentSlotStartIndex;
	unsigned short int currentSlotOffset;
	bool isOldSlot = false; // flag current slot is an old slot
	while (currentSlotNum <= slotNumRecord) {
		// get slot offset information
		if (this->getSlotInfo(pageData, currentSlotNum, currentSlotStartIndex,
				currentSlotOffset) != 0) {
			cout
					<< "!!!fail to get slot info in RecordBasedFileManager::insertRecordToPage"
					<< endl;
			return -1;
		}
		if (currentSlotOffset == 0) {
			isOldSlot = true;
			break;
		}
		currentSlotNum++;
	}

	// update slot num
	slotNum = currentSlotNum;

	// if insert in new slot
	if (!isOldSlot) {
//		cout << "insert in new slot" << endl;
		if(this->insertRecordToPageAtNextSlot(pageData, data, dataSize, slotNum)!=0){
			return -1;
		}
		return 0;
	}

	// else if the slot is old, update old slot
	if(this->updateRecordToPageAtSlotNumber(pageData, data, dataSize, slotNum)!=0){
		return -1;
	}
	return 0;
}

/**
 * delete record in page
 * param: pageData, slotNum
 * return: pageData
 * delete record
 */
RC RecordBasedFileManager::deleteRecordInPage(void * pageData,
		const unsigned short int slotNum) {

// get slot record information
	unsigned short int deleteSlotStartIndex;
	unsigned short int deleteSlotOffset;
	if (this->getSlotInfo(pageData, slotNum, deleteSlotStartIndex,
			deleteSlotOffset) != 0) {
		cout
				<< "!!!fail to get slot info, read record error in RecordBasedFileManager::deleteRecordInPage"
				<< endl;
		return -1;
	}

// check if slot record has already deleted
	if (deleteSlotOffset == 0) {
		cout
				<< "!!!this slot had been delete before in RecordBasedFileManager::deleteRecordInPage"
				<< endl;
		return -1;
	}

// get page information
	unsigned short int slotNumRecord;
	unsigned short int firstAvailableSlotIndex;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		cout
				<< "!!!fail to get page info, findInsertPosition in RecordBasedFileManager::deleteRecordInPage."
				<< endl;
		return -1;
	}

// move after records forward
	if (firstAvailableSlotIndex > deleteSlotStartIndex + deleteSlotOffset) {
		memmove((byte*) pageData + deleteSlotStartIndex,
				(byte*) pageData + deleteSlotStartIndex + deleteSlotOffset,
				firstAvailableSlotIndex - deleteSlotStartIndex
						- deleteSlotOffset);
	}

// update offsetSlot of delete record to 0
	if (this->updateSlotInfo(pageData, slotNum, deleteSlotStartIndex, 0) != 0) {
		cout
				<< "!!!fail to update slot info in RecordBasedFileManager::deleteRecordInPage"
				<< endl;
		return -1;
	}

// update all following slot information
	unsigned short int currentSlotNum = slotNum + 1;
	while (currentSlotNum <= slotNumRecord) {
		unsigned short int currentSlotStartIndex;
		unsigned short int currentSlotOffset;
		if (this->getSlotInfo(pageData, currentSlotNum, currentSlotStartIndex,
				currentSlotOffset) != 0) {
			cout
					<< "!!!cannot get slot information in RecordBasedFileManager::deleteRecordInPage."
					<< endl;
			return -1;
		}
		if (this->updateSlotInfo(pageData, currentSlotNum,
				currentSlotStartIndex - deleteSlotOffset, currentSlotOffset)
				!= 0) {
			cout
					<< "!!!cannot update slot information in RecordBasedFileManager::deleteRecordInPage."
					<< endl;
			return -1;
		}
		currentSlotNum++;
	}

// update page information
	if (this->updatePageInfo(pageData,
			firstAvailableSlotIndex - deleteSlotOffset, slotNumRecord) != 0) {
		cout << "!!!fail to update page information" << endl;
		return -1;
	}
	return 0;
}

/**
 * update record to page at slot number
 * need to move record in page
 */
RC RecordBasedFileManager::updateRecordToPageAtSlotNumber(void * pageData,
		const void * formatData, const unsigned short int &dataSize,
		const unsigned short int &currentSlotNum) {
// read page information
	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		cout << "!!!fail to get page info, in insertRecord2Slot." << endl;
		return -1;
	}

// read current slot info
	unsigned short int currentSlotStartIndex;
	unsigned short int currentSlotOffset;
	if (this->getSlotInfo(pageData, currentSlotNum, currentSlotStartIndex,
			currentSlotOffset) != 0) {
		cout
				<< "!!!fail to get slot info in RecordBasedFileManager::insertRecordToPage"
				<< endl;
		return -1;
	}

	if((dataSize - currentSlotOffset + firstAvailableSlotIndex + slotNumRecord * 2 * sizeof(unsigned short int)) >= PAGE_SIZE){
		cout << "!!!calculate error"<< endl;
		return -1;
	}

//	cout << "before insert, following start from:" << currentSlotStartIndex + currentSlotOffset <<endl;
	// move records
	memmove((byte*) pageData + currentSlotStartIndex + dataSize,
			(byte*) pageData + currentSlotStartIndex + currentSlotOffset,
			firstAvailableSlotIndex - currentSlotStartIndex - currentSlotOffset);
	// calculate move offset maybe positive
	short int moveOffset = dataSize - currentSlotOffset;
//	cout << "move offset: " << moveOffset << endl;

	// copy formatData into pageData
	memcpy((byte*) pageData + currentSlotStartIndex, formatData, dataSize);

	// update currentSlotOffset
	currentSlotOffset = dataSize;
	if (this->updateSlotInfo(pageData, currentSlotNum, currentSlotStartIndex,
			currentSlotOffset) != 0) {
		cout << "!!!fail to update slot information" << endl;
		return -1;
	}

// update all following slot information
	unsigned short int followingSlotNum = currentSlotNum + 1;
	while (followingSlotNum <= slotNumRecord) {
		unsigned short int followingSlotStartIndex;
		unsigned short int followingSlotOffset;
		if (this->getSlotInfo(pageData, followingSlotNum,
				followingSlotStartIndex, followingSlotOffset) != 0) {
			cout
					<< "!!!cannot get slot information in RecordBasedFileManager::deleteRecordInPage."
					<< endl;
			return -1;
		}

		// update following slot start index
		followingSlotStartIndex += moveOffset;
//		cout << "following slot:" << followingSlotNum << ",startIndex:"<< followingSlotStartIndex << ",slotOffset:" << followingSlotOffset << endl;
		if (this->updateSlotInfo(pageData, followingSlotNum,
				followingSlotStartIndex, followingSlotOffset) != 0) {
			cout
					<< "!!!cannot update slot information in RecordBasedFileManager::deleteRecordInPage."
					<< endl;
			return -1;
		}
		followingSlotNum++;
	}
// update firstAvailableIndex
	firstAvailableSlotIndex += moveOffset;
	if (this->updatePageInfo(pageData, firstAvailableSlotIndex, slotNumRecord)
			!= 0) {
		cout << "!!!fail to update page information" << endl;
		return -1;
	}
	return 0;
}

/**
 * find data in page
 * param: pageData, slotNum
 * return: data, dataSize
 */
RC RecordBasedFileManager::getFormatDataInPage(const void * pageData,
		const unsigned short int &slotNum, void * data,
		unsigned short int &dataSize) {

// get slot information
	unsigned short int startSlotIndex;
	unsigned short int slotOffsetNumber;
	if (this->getSlotInfo(pageData, slotNum, startSlotIndex, slotOffsetNumber)
			!= 0) {
		cout
				<< "!!!fail to get slot info, read record error in RecordBasedFileManager::readRecord"
				<< endl;
		return -1;
	}

	if (slotOffsetNumber == 0) {
		cout
				<< "!!!this slot had been delete before in RecordBasedFileManager::findRecordInPage"
				<< endl;
		return -1;
	}

// get format data
//	cout << "startSlotIndex:" << startSlotIndex << "slotOffsetNumber" << slotOffsetNumber << endl;
	memcpy(data, (byte*) pageData + startSlotIndex, slotOffsetNumber);
// return formatDataSize
	dataSize = slotOffsetNumber;
	return 0;
}

/**
 * get attribute in page
 * param: pageData, slotNum, attributePosition
 * return:attribute
 *
 */
RC RecordBasedFileManager::getAttributeInPage(const void * pageData,
		const unsigned short int &slotNum,
		const unsigned short int &attributePosition, void * attribute,
		unsigned short int &attributeSize) {
	unsigned short int formatDataSize;
	void * formatData = malloc(PAGE_SIZE);
	if (this->getFormatDataInPage(pageData, slotNum, formatData, formatDataSize)
			!= 0) {
		free(formatData);
		return -1;
	}
	if (this->getAttributeInFormatData(formatData, attributePosition, attribute,
			attributeSize) != 0) {
		free(formatData);
		return -1;
	}
	free(formatData);
	return 0;
}

/* slot management in page level*/
/**
 * is current page available to insert data
 */
RC RecordBasedFileManager::isCurrentPageAvailableInsert(const void * pageData,
		const unsigned short int &dataSize, bool &currentPageAvailable) {
	// attention: currentSize include slotInfo size
	// when update both currentSize and oldSize do not need include slotInfo
	// when insert currentSize need include slotInfo

	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		return -1;
	}
	// attention: insert formatData also need insert one slotInfo
	if (firstAvailableSlotIndex * sizeof(byte)
			+ (slotNumRecord + 1) * 2 * sizeof(unsigned short int)
			+ dataSize < PAGE_SIZE) {
//		cout << "can insert:" << endl;
//		cout << "currentData:" << dataSize << endl;
//		cout << "firstAvailableSlotIndex" << firstAvailableSlotIndex << endl;
//		cout << "slotNumRecord" << slotNumRecord << endl;
		currentPageAvailable = true;
	} else {
		currentPageAvailable = false;
	}
	return 0;
}

/**
 * is current page available to insert data
 */
RC RecordBasedFileManager::isCurrentPageAvailableUpdate(const void * pageData,
		const unsigned short int &currentFormatDataSize,
		const unsigned short int &oldFormatDataSize,
		bool &currentPageAvailable) {

	// attention: currentSize include slotInfo size
	// when update both currentSize and oldSize do not need include slotInfo
	// when insert currentSize need include slotInfo
	int dataSize = currentFormatDataSize - oldFormatDataSize;
	// check dataSize
	if (dataSize < 0) {
		currentPageAvailable = true;
		return 0;
	}

	unsigned short int firstAvailableSlotIndex;
	unsigned short int slotNumRecord;
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		return -1;
	}
	if (firstAvailableSlotIndex + dataSize
			+ (slotNumRecord) * 2 * sizeof(unsigned short int) < PAGE_SIZE) {
//		cout << "can update" << endl;
//		cout << "currentData:" << currentFormatDataSize << endl;
//		cout << "oldData:" << oldFormatDataSize << endl;
//		cout << "firstAvailableSlotIndex" << firstAvailableSlotIndex << endl;
//		cout << "slotNumRecord" << slotNumRecord << endl;
		currentPageAvailable = true;
	} else {
		currentPageAvailable = false;
	}
	return 0;
}

/**
 * get page information by page data
 * param: format page data
 * return: slotNumRecord,firstAvailableSlotIndex
 * get the page information: slot number record, first avaiable slot index
 */
RC RecordBasedFileManager::getPageInfoByData(const void * pageData,
		unsigned short int &firstAvailableSlotIndex,
		unsigned short int &slotNumRecord) {
	if (pageData == NULL) {
		cout << "!!!page data is null, page data cannot be decoded" << endl;
		return -1;
	}
	memcpy(&firstAvailableSlotIndex,
			(byte*) pageData + PAGE_SIZE - 2 * sizeof(unsigned short int),
			sizeof(unsigned short int));
	memcpy(&slotNumRecord,
			(byte*) pageData + PAGE_SIZE - sizeof(unsigned short int),
			sizeof(unsigned short int));

	return 0;
}

/**
 * get page information by page index
 * param: fileHandle, currentPageIndex
 * return: slotNumRecord,firstAvailableSlotIndex
 */
RC RecordBasedFileManager::getPageInfo(FileHandle &fileHandle,
		PageNum currentPageIndex, unsigned short int &firstAvailableSlotIndex,
		unsigned short int &slotNumRecord) {
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(currentPageIndex, pageData) != 0) {
		free(pageData);
		return -1;
	}
	if (this->getPageInfoByData(pageData, firstAvailableSlotIndex,
			slotNumRecord) != 0) {
		free(pageData);
		return -1;
	}
	free(pageData);
	return 0;
}

/**
 * update page information
 * param: pageData, firstAvailableSlotIndex, slotNumRecord
 * update page information at the header of page(the end of page).
 */
RC RecordBasedFileManager::updatePageInfo(void * pageData,
		const unsigned short int firstAvailableSlotIndex,
		const unsigned short int slotNumRecord) {
	memcpy((byte*) pageData + PAGE_SIZE - 2 * sizeof(unsigned short int),
			&firstAvailableSlotIndex, sizeof(unsigned short int));
	memcpy((byte*) pageData + PAGE_SIZE - sizeof(unsigned short int),
			&slotNumRecord, sizeof(unsigned short int));
	return 0;
}

/**
 * initial page information for blank page
 * return: pageData;
 * return a signed page data
 */
RC RecordBasedFileManager::initPageInfo(void * pageData) {
	unsigned short int firstAvailableIndex = 0;
// attention the slot num is 1, because it includes header record
	unsigned short int slotNum = 1;
	this->updatePageInfo(pageData, firstAvailableIndex, slotNum);
	return 0;
}

/**
 * insert slot information into page
 * param: slotStartIndex, slotOffsetNumber, currentSlotNum
 * return: pageData
 * insert the slot index into page
 * attention: current slot number means: slot record number after insert slot info add first header info
 */
RC RecordBasedFileManager::insertSlotInfo(void *pageData,
		const unsigned short int slotStartIndex,
		const unsigned short int slotOffsetNumber,
		const unsigned short int currentSlotNum) {
	unsigned short int offset = PAGE_SIZE - 2 * sizeof(unsigned short int) * (currentSlotNum);
	memcpy((byte*) pageData + offset, &slotStartIndex,
			sizeof(unsigned short int));
	memcpy((byte*) pageData + offset + sizeof(unsigned short int),
			&slotOffsetNumber, sizeof(unsigned short int));

#ifdef DEBUG
	cerr << "insertSlotInfo: " << endl;
	cerr << "startIndex:" << slotStartIndex << endl;
	cerr << "offsetNumber:" << slotOffsetNumber << endl;
#endif

	return 0;
}

/**
 * get slot information
 * param: pageData, slotNum
 * return startSlotIndex, slotOffsetNumber
 * get the slot information according to slot number in the page
 */
RC RecordBasedFileManager::getSlotInfo(const void * pageData,
		const unsigned short int slotNum, unsigned short int &slotStartIndex,
		unsigned short int &slotOffsetNumber) {
	// check slotNum
	if(slotNum < 2){
		cout << "!!!slotNum lower than bound getSlotInfo."<< endl;
		return -1;
	}
	unsigned short int offset = PAGE_SIZE - 2 * sizeof(unsigned short int) * (slotNum);
	memcpy(&slotStartIndex, (byte*) pageData + offset,
			sizeof(unsigned short int));
	memcpy(&slotOffsetNumber,
			(byte*) pageData + offset + sizeof(unsigned short int),
			sizeof(unsigned short int));
	return 0;
}

/**
 * update slot information
 * param: pageData, slotPosition, newSlotStartIndex, newSlotOffsetNumer
 * update the pointed slot information: start index and offset
 */
RC RecordBasedFileManager::updateSlotInfo(void *pageData,
		const unsigned short int slotNum,
		const unsigned short int slotStartIndex,
		const unsigned short int slotOffset) {
	if (slotNum < 2) {
		cout
				<< "!!!cannot update the first slot information, because it is related to the whole page in RecordBasedFileManager::updateSlotInfo"
				<< endl;
		return -1;
	}
	unsigned short int offset = PAGE_SIZE
			- 2 * sizeof(unsigned short int) * (slotNum);
	memcpy((byte*) pageData + offset, &slotStartIndex,
			sizeof(unsigned short int));
	offset += sizeof(unsigned short int);
	memcpy((byte*) pageData + offset, &slotOffset, sizeof(unsigned short int));

#ifdef DEBUG
	cerr << "insertSlotInfo: " << endl;
	cerr << "startIndex:" << slotStartIndex << endl;
	cerr << "offsetNumber:" << slotOffsetNumber << endl;
#endif

	return 0;
}

/* page management methods in file level */


/**
 * append signed page
 * param: fileHandle
 */
RC RecordBasedFileManager::appendSignedPageInFile(FileHandle &fileHandle) {
	void *data = malloc(PAGE_SIZE);
	if (this->initPageInfo(data) != 0) {
		free(data);
		cout << "!!!fail to append signed page" << endl;
		return -1;
	}
	if (fileHandle.appendPage(data) != 0) {
		free(data);
		cout << "!!!fail to append signed page" << endl;
		return -1;
	}
	free(data);
//	cout << "append a new signed page: " << fileHandle.getNumberOfPages() - 1
//			<< endl;
	return 0;
}

/* file level */
/**
 * insert format data to file
 * param: fileHandle, formatData, dataSize
 * return: rid
 * find available page
 * read page
 * insert format data in page
 * write page
 */
RC RecordBasedFileManager::insertFormatDataToFile(FileHandle fileHandle,
		const void * formatData, const unsigned short int &dataSize, RID &rid) {
// find available page
// suppose the following step would insert data and slot
	if (this->findAvailablePageInFile(fileHandle, dataSize, rid.pageNum) != 0) {
		cout
				<< "!!!fail to find suitable insert position in RecordBasedFileManager::insertRecord"
				<< endl;
		return -1;
	}

// get page data
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout << "!!!fail to read page in RecordBasedFileManager::insertRecord"
				<< endl;
		return -1;
	}

// insert format data into page data
	if (this->insertRecordToPage(pageData, formatData, dataSize, rid.slotNum)
			!= 0) {
		free(pageData);
		cout
				<< "!!!fail to insert record into page in RecordBasedFileManager::insertRecord"
				<< endl;
		return -1;
	}

//	cout << "slotNum:" << rid.slotNum << endl;

// write back page data to file
	if (fileHandle.writePage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout
				<< "!!!fail to write back page data in RecordBasedFileManager::insertRecord."
				<< endl;
	}

	free(pageData);
	return 0;
}

/**
 * param: fileHandle, dataSize, rid
 */
RC RecordBasedFileManager::updateFormatDataInFile(FileHandle fileHandle,
		void * formatData, unsigned short int &dataSize, const RID &rid) {
// read current page
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout << "!!!read record error in RecordBasedFileManager::updateFormatDataInFile"
				<< endl;
		return -1;
	}

// check if there are available space
	bool currentPageAvailableUpdate = false;
	// TODO: need re-factor
	{
		// delete linked record if current record is a link
		unsigned short int oldFormatDataSize;
		void * oldFormatData = malloc(PAGE_SIZE);
		if (this->getFormatDataInPage(pageData, rid.slotNum, oldFormatData,
				oldFormatDataSize) != 0) {
			free(oldFormatData);
			cout << "!!!fail to get format data in page in RecordBasedFileManager::updateFormatDataInFile." << endl;
			return -1;
		}
		// check if format data is a link
		bool isOldDataLink = false;
		if (this->isFormatDataLink(oldFormatData, oldFormatDataSize, isOldDataLink)
				!= 0) {
			free(oldFormatData);
			cout
					<< "!!!fail to isFormatDataLink in RecordBasedFileManager::updateFormatDataInFile."
					<< endl;
			return -1;
		}

		// if current data is a link, delete linked record
		if (isOldDataLink) {
			// delete linked record
			RID firstRid;
			this->getFirstRidInFormatData(oldFormatData, oldFormatDataSize, firstRid);
//			cout << "delete firstRid.pageNum:" << firstRid.pageNum << endl;
			if (this->deleteFormatDataInFile(fileHandle, firstRid) != 0) {
				free(oldFormatData);
				cout
						<< "!!!fail to deleteRecordInFile in RecordBasedFileManager::updateFormatDataInFile."
						<< endl;
				return -1;
			}
		}

		// check if there is available space to update
		if (this->isCurrentPageAvailableUpdate(pageData, dataSize,
				oldFormatDataSize, currentPageAvailableUpdate) != 0) {
			free(oldFormatData);
			cout
					<< "!!!fail to isCurrentPageAvailableUpdate in RecordBasedFileManager::updateFormatDataInFile."
					<< endl;
			return -1;
		}
		free(oldFormatData);
	}

// if current page available update
// rid.slotNum don't need change
// insert into record in slotNum
	if (currentPageAvailableUpdate) {
		// update record at page
//		cout << "update in current page:" << rid.pageNum << endl;
		// update record in current page
		if (this->updateRecordToPageAtSlotNumber(pageData, formatData, dataSize,
				rid.slotNum) != 0) {
			return -1;
		}
		// write back
		if (fileHandle.writePage(rid.pageNum, pageData) != 0) {
			return -1;
		}
//		cout << "finish update in page:" << rid.pageNum << ", slot:" << rid.slotNum << endl;
		return 0;
	}

//	cout << "update record in new page" << endl;
// else

	// reset firstRid in formatData and flag it is a linked
	RID newFirstRid;
	newFirstRid.pageNum = 0;
	newFirstRid.slotNum = 1;
	if (this->setFirstRidInFormatData(formatData, newFirstRid, dataSize) != 0) {
		return -1;
	}

	// insert format data in another place and get new first rid
	if (this->insertFormatDataToFile(fileHandle, formatData, dataSize,
			newFirstRid) != 0) {
		cout << "!!!fail to insert format data to file in InsertRecord" << endl;
		return -1;
	}

// save returned firstRid in current page

	// reset formatData only save newFirstRid
	dataSize = sizeof(RID);
//	cout << "newFirstRid" << newFirstRid.pageNum<< endl;
	if (this->setFirstRidInFormatData(formatData, newFirstRid, dataSize) != 0) {
		return -1;
	}

	// update formatData in page at slot number
	if (this->updateRecordToPageAtSlotNumber(pageData, formatData, dataSize,
			rid.slotNum) != 0) {
		return -1;
	}

	// write back
	if (fileHandle.writePage(rid.pageNum, pageData) != 0) {
		return -1;
	}

//	cout << "finish update record in old page" << endl;
	return 0;
}

/**
 * deleteRecordInFile
 * param: fileHandle, rid
 * delete record(include link record and linked record)
 */
RC RecordBasedFileManager::deleteFormatDataInFile(FileHandle &fileHandle,
		const RID rid) {

// get page data
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout << "!!!read record error in RecordBasedFileManager::deleteRecordInFile"
				<< endl;
		return -1;
	}

	// get formatData
	void * formatData = malloc(PAGE_SIZE);
	unsigned short int dataSize;
	if (this->getFormatDataInPage(pageData, rid.slotNum, formatData, dataSize)
			!= 0) {
		free(formatData);
		free(pageData);
		cout
				<< "!!!fail to find record in page in RecordBasedFileManager::deleteRecordInFile"
				<< endl;
		return -1;
	}

	// check firstRid
	bool isCurrentDataLink = false;
	if(this->isFormatDataLink(formatData, dataSize, isCurrentDataLink)!=0){
		free(formatData);
		free(pageData);
		cout
				<< "!!!fail to isFormatDataLink in RecordBasedFileManager::deleteRecordInFile"
				<< endl;
		return -1;
	}

	// if it is a link rid
	if (isCurrentDataLink) {
		RID firstRid;
		if (this->getFirstRidInFormatData(formatData, dataSize, firstRid)
				!= 0) {
			free(formatData);
			free(pageData);
			return -1;
		}
		if (this->deleteFormatDataInFile(fileHandle, firstRid) != 0) {
			free(formatData);
			free(pageData);
			return -1;
		}
	}

	free(formatData);

	// else delete record in current page
	if (this->deleteRecordInPage(pageData, rid.slotNum) != 0) {
		free(pageData);
		cout << "!!!fail to delete record in page" << endl;
		return -1;
	}

	// write back pageData
	if (fileHandle.writePage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout << "!!!write record error in RecordBasedFileManager::deleteRecordInFile"
				<< endl;
		return -1;
	}

	free(pageData);
	return 0;
}

/**
 * read format data in file
 * param: fileHandle, rid
 * return: formatData, formatDataSize
 */
RC RecordBasedFileManager::readFormatDataInFile(FileHandle fileHandle,
		const RID &rid, void * formatData, unsigned short int &formatDataSize) {
	// read page
	void * pageData = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, pageData) != 0) {
		free(pageData);
		cout << "!!!read record error in RecordBasedFileManager::readFormatDataInFile"
				<< endl;
		return -1;
	}

	// get format data
	if (this->getFormatDataInPage(pageData, rid.slotNum, formatData,
			formatDataSize) != 0) {
		free(pageData);
		cout
				<< "!!!fail to find data in page in RecordBasedFileManager::readRecord"
				<< endl;
		return -1;
	}
	free(pageData);

	// check first rid
	bool isCurrentDataLink = false;
	if (this->isFormatDataLink(formatData, formatDataSize, isCurrentDataLink)
			!= 0) {
		return -1;
	}

	if (!isCurrentDataLink) {
		return 0;
	}

	// else
	// attention: recursive
	RID firstRid;
	if (this->getFirstRidInFormatData(formatData, formatDataSize, firstRid)
			!= 0) {
		return -1;
	}
	if (this->readFormatDataInFile(fileHandle, firstRid, formatData,
			formatDataSize) != 0) {
		cout
				<< "!!!fail to find next record in RecordBasedFileManager::readRecord"
				<< endl;
		return -1;
	}
	return 0;
}

/**
 * find suitable slot position
 * param: fileHandle, dataSize
 * return: pageNum
 * find page until get one, or append a new signed page.
 * TODO: with a standard file management 1 + 1027 page
 */
RC RecordBasedFileManager::findAvailablePageInFile(FileHandle &fileHandle,
		const unsigned short int dataSize, unsigned &pageNum) {

	PageNum totalPage = fileHandle.getNumberOfPages();

	PageNum currentPageIndex = 0;
	// insert page should start from second page
	if (totalPage < 2) {
		cout << "!!!page not initial." << endl;
		return -1;
	}
	// a more easier strategy
	currentPageIndex = (totalPage - 1);

	// try to find slot without append from
	bool currentPageAvailable = false;
	void * pageData = malloc(PAGE_SIZE);
	while(currentPageIndex < totalPage){
		if(fileHandle.readPage(currentPageIndex, pageData)!=0){
			free(pageData);
			return -1;
		}
		if(this->isCurrentPageAvailableInsert(pageData, dataSize, currentPageAvailable)!=0){
			free(pageData);
			return -1;
		}
		if(currentPageAvailable){
			pageNum = currentPageIndex;
			free(pageData);
			return 0;
		}
		currentPageIndex++;
	}

	// else
	// append next page
	if (this->appendSignedPageInFile(fileHandle) != 0) {
		free(pageData);
		cout
				<< "!!!fail to append new signed page in RecordBasedFileManager::findSlotPosition."
				<< endl;
		return -1;
	}

	// read page
	if(fileHandle.readPage(currentPageIndex, pageData)!=0){
		free(pageData);
		return -1;
	}

	// check if page is available
	if(this->isCurrentPageAvailableInsert(pageData, dataSize, currentPageAvailable)!=0){
		free(pageData);
		return -1;
	}
	if(currentPageAvailable){
		// return pageNum
		pageNum = currentPageIndex;
		free(pageData);
		return 0;
	}

	free(pageData);
	cout << "!!!fail to find available page" << endl;
	return -1;
}
