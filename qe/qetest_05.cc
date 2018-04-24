#include <fstream>
#include <iostream>

#include <vector>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

RC testCase_5() {
	// Mandatory for all
	// 1. Filter -- IndexScan as input, on TypeReal attribute
	// SELECT * FROM RIGHT WHERE C >= 110.0
	cerr << endl << "***** In QE Test Case 5 *****" << endl;

	RC rc = success;
	IndexScan *is = new IndexScan(*rm, "right", "C");
	float compVal = 110.0;
	float valueC = 0;

	/*cerr*/
//	float compVal1 = 0.0;
//	float compVal2 = 1250.0;
//	void * ce = malloc(PAGE_SIZE);
//		if (is->iter->ix_ScanIterator.ixfileHandle->isOpen()) {
//			cout << "10 file handle is open" << endl;
//		}
//TODO: set iterator free a not malloc memory
//	is->setIterator(&compVal1, &compVal2, true, true);
//		if(is->iter->ix_ScanIterator.ixfileHandle->isOpen()){
//			cout << "end file handle is open"<< endl;
//		}
	//	for (int i = 0; i < 10; i++) {
//	while (is->getNextTuple(ce) != EOF) {
//					cerr<< "i:" << endl;
//		cerr << "ce0:" << *(int*) ((byte*) ce + 1) << endl;
//		cerr << "ce1:" << *(float*) ((byte*) ce + 1 + 4) << endl;
//		cerr << "ce2:" << *(int*) ((byte*) ce + 1 + 8) << endl;
//	}
	//	}
//	free(ce);

	// Set up condition
	Condition cond;
	cond.lhsAttr = "right.C";
	cond.op = GE_OP;
	cond.bRhsIsAttr = false;
	Value value;
	value.type = TypeReal;
	value.data = malloc(bufSize);
	*(float *) value.data = compVal;
	cond.rhsValue = value;

	int expectedResultCnt = 15; // 110.00 ~ 124.00;
	int actualResultCnt = 0;

	// Create Filter
	Filter *filter = new Filter(is, cond);

	// Go over the data through iterator
	void *data = malloc(bufSize);

	bool nullBit = false;
	
	int valueB = 0;
	int valueD = 0;

	while (filter->getNextTuple(data) != QE_EOF) {
		int offset = 0;
		
		// is an attribute B NULL?
		nullBit = *(unsigned char *)((char *)data) & (1 << 7);
		if (nullBit) {
			cerr << endl << "***** A returned value is not correct. *****" << endl;
			rc = fail;
			goto clean_up;
		}
		valueB = *(int *)((char *)data+1+offset);

		// Print right.B
		cerr << "right.B " << valueB;
		offset += sizeof(int);

		// is an attribute C NULL?
		nullBit = *(unsigned char *)((char *)data) & (1 << 6);
		if (nullBit) {
			cerr << endl << "***** A returned value is not correct. *****" << endl;
			rc = fail;
			goto clean_up;
		}
		valueC = *(float *)((char *)data+1+offset);
		
		// Print right.C
		cerr << "  right.C " << valueC;
		offset += sizeof(float);
		if (valueC < compVal) {
			rc = fail;
			goto clean_up;
		}

		// is an attribute D NULL?
		nullBit = *(unsigned char *)((char *)data) & (1 << 5);
		if (nullBit) {
			cerr << endl << "***** A returned value is not correct. *****" << endl;
			rc = fail;
			goto clean_up;
		}
		valueD = *(int *)((char *)data+1+offset);

		// Print right.D
		cerr << "  right.D " << valueD << endl;
		offset += sizeof(int);

		memset(data, 0, bufSize);
		actualResultCnt++;
	}
	if (expectedResultCnt != actualResultCnt) {
		cerr << "***** The number of returned tuple is not correct. *****" << endl;
		rc = fail;
	}

clean_up:
	delete filter;
	delete is;
	free(value.data);
	free(data);
	return rc;
}

int main() {
	// Tables created: none
	// Indexes created: none

	if (testCase_5() != success) {
		cerr << "***** [FAIL] QE Test Case 5 failed. *****" << endl;
		return fail;
	} else {
		cerr << "***** QE Test Case 5 finished. The result will be examined. *****" << endl;
		return success;
	}
}
