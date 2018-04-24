#include "qe.h"

// ... the rest of your implementations go here
/******************************** DataUtil ********************************/
class DataUtils {
public:

	static void pakageSingleData(void * data, float value, unsigned valueSize,
			bool isNull) {
		if (isNull) {
			// set null indicator
			byte nullIndicator = 0x80;
			memcpy(data, &nullIndicator, 1);
		}
		memset(data, 0, 1);
		memcpy((byte*) data + 1, &value, valueSize);
	}

	static void joinData(void * leftData, const vector<Attribute> &leftAttrs,
			void * rightData, const vector<Attribute> &rightAttrs,
			void * returnData) {

		unsigned leftSize = getDataSize(leftData, leftAttrs);
		unsigned rightSize = getDataSize(rightData, rightAttrs);

		unsigned leftNullIndicatorSize = 1
				+ ((leftAttrs.size() - 1) / CHAR_BIT);
		unsigned rightNullIndicatorSize = 1
				+ ((rightAttrs.size() - 1) / CHAR_BIT);
		unsigned nullIndicatorSize = 1
				+ ((leftAttrs.size() + rightAttrs.size() - 1) / CHAR_BIT);

		byte * leftNullFieldsIndicator;
		leftNullFieldsIndicator = (byte*) leftData;

		byte * rightNullFieldsIndicator;
		rightNullFieldsIndicator = (byte*) rightData;

		byte * nullFieldsIndicator;
		nullFieldsIndicator = (byte*) returnData;

		memset(returnData, 0, nullIndicatorSize);
		// combine null indicator
		int count = 0;
		for (int i = 0; i < leftAttrs.size(); i++) {
			bool bitIsNull = leftNullFieldsIndicator[i / 8]
					& (1 << (7 - i % 8));
			nullFieldsIndicator[count / 8] += (bitIsNull << (7 - count % 8));
			count++;
		}
		for (int i = 0; i < rightAttrs.size(); i++) {
			bool bitIsNull = rightNullFieldsIndicator[i / 8]
					& (1 << (7 - i % 8));
			nullFieldsIndicator[count / 8] += (bitIsNull << (7 - count % 8));
			count++;
		}

		// copy data
		unsigned offset = nullIndicatorSize;
		memcpy((byte*) returnData + offset,
				(byte*) leftData + leftNullIndicatorSize,
				leftSize - leftNullIndicatorSize);
		offset += leftSize - leftNullIndicatorSize;
		memcpy((byte*) returnData + offset,
				(byte*) rightData + rightNullIndicatorSize,
				rightSize - rightNullIndicatorSize);
		return;
	}

	static bool compare(const void* leftData, const bool leftNull,
			const void* rightData, const bool rightNull, CompOp &op,
			AttrType &type) {
		// check leftNull null
		if (leftNull) {
			if (op == EQ_OP) {
				return rightNull;
			} else if (op == NE_OP) {
				return !rightNull;
			}
		}
		// check rightData
		if (rightNull) {
			if (op == EQ_OP) {
				return false;
			} else if (op == NE_OP) {
				return true;
			}
		}

		if (type == TypeInt) { // compare int
			int left = *(int *) leftData;
			int right = *(int *) rightData;
			//			/*cerr*/
			//			cerr << "compare" << endl;
			//			cerr << "left" << left << endl;
			//			cerr << "right" << right << endl;
			if (op == EQ_OP) {
				return left == right;
			} else if (op == LT_OP) {
				return left < right;
			} else if (op == LE_OP) {
				return left <= right;
			} else if (op == GT_OP) {
				return left > right;
			} else if (op == GE_OP) {
				return left >= right;
			} else if (op == NE_OP) {
				return left != right;
			} else {
				cerr << "compare method error" << endl;
				return false;
			}
		} else if (type == TypeReal) {
			float left = *(float *) leftData;
			float right = *(float *) rightData;

			if (op == EQ_OP) {
				return left == right;
			} else if (op == LT_OP) {
				return left < right;
			} else if (op == LE_OP) {
				return left <= right;
			} else if (op == GT_OP) {
				return left > right;
			} else if (op == GE_OP) {
				return left >= right;
			} else if (op == NE_OP) {
				return left != right;
			} else {
				cerr << "compare method error" << endl;
				return false;
			}
		} else if (type == TypeVarChar) {
			bool result = false;

			int leftLen = *(int *) leftData;
			int rightLen = *(int *) rightData;

			char * leftChs = new char[leftLen + 1];
			char * rightChs = new char[rightLen + 1];

			memcpy(leftChs, (char*) leftData + sizeof(int), leftLen);
			leftChs[leftLen] = '\0';

			memcpy(rightChs, (char*) rightData + sizeof(int), rightLen);
			rightChs[rightLen] = '\0';

			if (op == EQ_OP) {
				result = (strcmp(leftChs, rightChs) == 0);
			} else if (op == LT_OP) {
				result = (strcmp(leftChs, rightChs) < 0);
			} else if (op == LE_OP) {
				result = (strcmp(leftChs, rightChs) <= 0);
			} else if (op == GT_OP) {
				result = (strcmp(leftChs, rightChs) > 0);
			} else if (op == GE_OP) {
				result = (strcmp(leftChs, rightChs) >= 0);
			} else if (op == NE_OP) {
				result = (strcmp(leftChs, rightChs) != 0);
			} else {
				cerr << "compare method error" << endl;
				result = false;
			}
			// free
			delete[] leftChs;
			delete[] rightChs;
			return result;

		} else {
			cerr << "compare method error" << endl;
			return false;
		}
	}

	static void getDataPositions(const void * data, unsigned * positions,
			const vector<Attribute> &attributes) {

		unsigned nullIndicatorSize = 1 + ((attributes.size() - 1) / CHAR_BIT);
		unsigned offset = 0;
		byte * nullFieldsIndicator = new byte[nullIndicatorSize];
		memcpy(nullFieldsIndicator, data, nullIndicatorSize);
		offset += nullIndicatorSize;

		positions[0] = offset;

		for (unsigned i = 0; i < attributes.size(); i++) {
			bool nullBit = nullFieldsIndicator[i / CHAR_BIT]
					& (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
			if (!nullBit) { // not null
				AttrType type = attributes[i].type;
				if (type == TypeInt) {
					offset += sizeof(int);
				} else if (type == TypeReal) {
					offset += sizeof(float);
				} else {
					int len = 0;
					memcpy(&len, (byte*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
			// record the position offset
			positions[i + 1] = offset;
		}
		delete[] nullFieldsIndicator;
		return;
	}

	static void getDataAtPosition(const void * data, void * returnData,
			bool &isNull, const unsigned &position,
			const vector<Attribute> &attributes) {
		isNull = true;
		unsigned * positions = new unsigned[attributes.size() + 1];

		getDataPositions(data, positions, attributes);

		unsigned startIndex = positions[position];
		unsigned endIndex = positions[position + 1];

		if (endIndex != startIndex) { // not null
			memcpy(returnData, (byte*) data + startIndex,
					endIndex - startIndex);
			isNull = false;
		}

		delete[] positions;
		return;
	}

	static unsigned getDataSize(const void * data,
			const vector<Attribute> &attributes) {
		unsigned nullIndicatorSize = 1 + ((attributes.size() - 1) / CHAR_BIT);
		unsigned offset = 0;
		byte * nullFieldsIndicator = new byte[nullIndicatorSize];
		memcpy(nullFieldsIndicator, data, nullIndicatorSize);
		offset += nullIndicatorSize;
		for (unsigned i = 0; i < attributes.size(); i++) {
			bool nullBit = nullFieldsIndicator[i / CHAR_BIT]
					& (1 << (CHAR_BIT - 1 - i % CHAR_BIT));
			if (!nullBit) { // not null
				AttrType type = attributes[i].type;
				if (type == TypeInt) {
					offset += sizeof(int);
				} else if (type == TypeReal) {
					offset += sizeof(float);
				} else {
					int len = 0;
					memcpy(&len, (byte*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				}
			}
		}
		delete[] nullFieldsIndicator;
		return offset;
	}

};

/******************************** Tuple ********************************/
Tuple::Tuple(void * data, int length) {
	length = PAGE_SIZE;
	this->length = length;
	this->data = malloc(length);
	memcpy(this->data, data, length);
}

Tuple::~Tuple() {
	free(this->data);
}

/******************************** Filter ********************************/
Filter::Filter(Iterator* input, const Condition &condition) {
	this->iterator = input;
	this->condition = condition;
	this->iterator->getAttributes(this->attributes);
	this->eof = false;

	this->leftPosition = 0;
	this->rightPosition = 0;

	// set value and position
	for (int i = 0; i < this->attributes.size(); i++) {
		// set left
		if (attributes[i].name == this->condition.lhsAttr) {
			this->leftPosition = i;
			this->leftValue.type = this->attributes[i].type;
		}
		// set right
		if (this->condition.bRhsIsAttr
				&& attributes[i].name == this->condition.rhsAttr) {
			this->rightPosition = i;
			this->rightValue.type = this->attributes[i].type;
		}
	}

	this->leftValue.data = malloc(PAGE_SIZE);

	// if right is attribute
	if (this->condition.bRhsIsAttr) {
		this->rightValue.data = malloc(PAGE_SIZE);
	} else {
		this->rightValue.data = this->condition.rhsValue.data;
		this->rightValue.type = this->condition.rhsValue.type;
	}
}

Filter::~Filter() {
	free(this->leftValue.data);
	if (this->condition.bRhsIsAttr) {
		free(this->rightValue.data);
	}
}

RC Filter::getNextTuple(void *data) {

	if (this->eof) {
		return QE_EOF;
	}

	// condition 1
	if (this->condition.op == NO_OP) {
		return this->iterator->getNextTuple(data);
	}

	// else condition 2
	// find next suitable tuple
	while (this->iterator->getNextTuple(data) != QE_EOF) {

		// get left value
		bool isLeftNull = true;
		DataUtils::getDataAtPosition(data, leftValue.data, isLeftNull,
				this->leftPosition, this->attributes);

		// get right value
		bool isRightNull = true;
		if (this->condition.bRhsIsAttr) {
			// in right value in data
			DataUtils::getDataAtPosition(data, rightValue.data, isRightNull,
					this->rightPosition, this->attributes);
		} else {
			if (this->rightValue.data != NULL) {
				isRightNull = false;
			}
		}

		if (DataUtils::compare(leftValue.data, isLeftNull, rightValue.data,
				isRightNull, condition.op, leftValue.type)) {
			// return data
//			cerr << "left" << *(float *)leftValue.data << endl;
//			cerr << "right" << *(float *)rightValue.data << endl;
			return 0;
		}
	}

	this->eof = true;
	return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	for (int i = 0; i < this->attributes.size(); i++) {
		attrs.push_back(this->attributes[i]);
	}
	return;
}

/******************************** Project ********************************/
Project::Project(Iterator *input, const vector<string> &attrNames) {
	this->iterator = input;
	this->eof = false;

	// get projection attributes
	this->iterator->getAttributes(this->allAttributes);

	// set attributes by all attributes
	// scan attrNames
	for (int i = 0; i < attrNames.size(); i++) {
		// scan all attributes
		for (int j = 0; j < this->allAttributes.size(); j++) {
			// find matching
			if (attrNames[i] == this->allAttributes[j].name) {
				// save positions
				this->attributesPositions.push_back(j);
				// save attributes
				this->attributes.push_back(this->allAttributes[j]);
				break;
			}
		}
	}
}

Project::~Project() {

}

RC Project::getNextTuple(void * data) {
	if (this->eof) {
		return QE_EOF;
	}

	// get all data
	void * allData = malloc(PAGE_SIZE);
	if (this->iterator->getNextTuple(allData) == QE_EOF) {
		this->eof = true;
		return QE_EOF;
	}

	// else

	// projection

	// all positions
	unsigned * allPositions = new unsigned[allAttributes.size() + 1];

	// get all data position
	DataUtils::getDataPositions(allData, allPositions, this->allAttributes);

	// data memory copy
	unsigned offset = 0;
	unsigned nullIndicatorSize = 1 + ((this->attributes.size() - 1) / CHAR_BIT);

	byte * nullIndicators = new byte[nullIndicatorSize];
	// set null indicators as all not null
	memset(nullIndicators, 0, nullIndicatorSize);

	offset += nullIndicatorSize;

	byte nullIndicatorStandard = 0x80;
	for (int i = 0; i < this->attributes.size(); i++) {

		unsigned curAttributePosition = this->attributesPositions[i];

		unsigned startIndex = allPositions[curAttributePosition];
		unsigned endIndex = allPositions[curAttributePosition + 1];

		if (endIndex == startIndex) { // null
			// set null indicator
			nullIndicators[i / CHAR_BIT] |= (nullIndicatorStandard
					>> (i % CHAR_BIT));
		} else { // not null
			// memory copy
			memcpy((byte*) data + offset, (byte*) allData + startIndex,
					endIndex - startIndex);
			// set offset
			offset += endIndex - startIndex;
		}
	}

	// copy nullIndicators
	memcpy(data, nullIndicators, nullIndicatorSize * sizeof(byte));
	delete[] nullIndicators;
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	for (int i = 0; i < this->attributes.size(); i++) {
		attrs.push_back(this->attributes[i]);
	}
	return;
}

/******************************** BNLJoin ********************************/
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn,
		const Condition &condition, const unsigned numPages) {

	this->outer = leftIn;
	this->inner = rightIn;
//	this->outer = leftIn;
//		this->inner = new TableScan(rightIn->rm, rightIn->tableName);

//	/*cerr*/
//		void * ce = malloc(PAGE_SIZE);
//		for (int i = 0; i < 10; i++) {
//			outer->getNextTuple(ce);
//			cerr << "ce0" << *(int*) ((byte*) this->buffer[i]->data + 1) << endl;
//			cerr << "ce1" << *(int*) ((byte*) this->buffer[i]->data + 1 + 4) << endl;
//		}
//		free(ce);

	this->bufferLimitation = numPages * PAGE_SIZE;
	this->bufferSize = 0;
	this->bufferIndex = 0;

	this->outer->getAttributes(this->outerAttributes);
	this->inner->getAttributes(this->innerAttributes);

	this->condition = condition;

	// set outer position
	for (int i = 0; i < this->outerAttributes.size(); i++) {
		if (this->outerAttributes[i].name == this->condition.lhsAttr) {
			this->outerPosition = i;
			this->compareType = this->outerAttributes[i].type;
			break;
		}
	}

	// set inner position
	if (!this->condition.bRhsIsAttr) {
		cerr << "error not join" << endl;
	}
	for (int i = 0; i < this->innerAttributes.size(); i++) {
		if (this->innerAttributes[i].name == this->condition.rhsAttr) {
			this->innerPosition = i;
			if (this->compareType != this->innerAttributes[i].type) {
				cerr << "error compare type not equal" << endl;
			}
			break;
		}
	}

	this->eof = false;
	this->bufferEof = false;

	// get firstBuffer
	this->loadBuffer();

	// get firstData
	this->innerData = malloc(PAGE_SIZE);
	this->eof = this->inner->getNextTuple(this->innerData);

	innerAttributeData = malloc(PAGE_SIZE);
	innerAttributeDataNull = true;
	outerAttributeData = malloc(PAGE_SIZE);
	outerAttributeDataNull = true;

}

BNLJoin::~BNLJoin() {
	free(this->outerAttributeData);
	free(this->innerAttributeData);
	free(this->innerData);
	this->clearBuffer();
}

RC BNLJoin::getNextTuple(void *data) {

//	if(this->eof){
//		return QE_EOF;
//	}

	while (!this->eof) {

		// at that time have innerData
		while (this->bufferIndex < this->buffer.size()) {
//			cerr << "this->bufferIndex" <<this->bufferIndex << endl;
			// get innerAttr
			DataUtils::getDataAtPosition(this->innerData, innerAttributeData,
					innerAttributeDataNull, this->innerPosition,
					this->innerAttributes);
			// get outerAttr
			DataUtils::getDataAtPosition(this->buffer[bufferIndex]->data,
					outerAttributeData, outerAttributeDataNull,
					this->outerPosition, this->outerAttributes);
			/*cerr*/
//			cerr << "outerPosition"<<this->outerPosition << endl;
//			cerr << "innerPosition"<<this->innerPosition << endl;
//			cerr << "compareType"<<this->compareType << endl;
			if (DataUtils::compare(outerAttributeData, outerAttributeDataNull,
					innerAttributeData, innerAttributeDataNull,
					this->condition.op, this->compareType)) {
				// join
//				cerr<< "joint" <<endl;
				DataUtils::joinData(this->buffer[bufferIndex]->data,
						this->outerAttributes, this->innerData,
						this->innerAttributes, data);

				this->bufferIndex++;
				return 0;
			}
			this->bufferIndex++;
		}
		this->bufferIndex = 0;

		if (this->inner->getNextTuple(this->innerData) == EOF) {

			// reset inner
			this->inner->setIterator();
//			cerr << "inner rescan start" << endl;

			// get first inner tuple
			if (this->inner->getNextTuple(this->innerData) == EOF) {
				cerr << "inner EOF error" << endl;
			}

			// load next buffer
			if (this->loadBuffer() == EOF) {
//				cerr << "buffer finished" << endl;
				this->eof = true;
				return QE_EOF;
			}
		}
	}
	return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	for (int i = 0; i < this->outerAttributes.size(); i++) {
		attrs.push_back(this->outerAttributes[i]);
	}
	for (int i = 0; i < this->innerAttributes.size(); i++) {
		attrs.push_back(this->innerAttributes[i]);
	}
}

RC BNLJoin::loadBuffer() {

//	cerr << "load buffer" << endl;

	this->clearBuffer();

	if (this->bufferEof) {
		return QE_EOF;
	}

	void * outerData = malloc(PAGE_SIZE);

//	cerr << "this->bufferLimitation" << this->bufferLimitation << endl;

	while (this->bufferSize < this->bufferLimitation) {
		// get data and check EOF
		if (outer->getNextTuple(outerData) == QE_EOF) {

//			cerr << "buffer current size" << this->buffer.size() << "bufferEof"
//					<< endl;
			this->bufferEof = true;
			break;
		}

//		cerr << "original attr0" << *(int*) ((byte*)outerData + 1)<<  endl;
//		cerr << "original attr1" << *(int*) ((byte*)outerData + 1 + sizeof(int))<<  endl;

		// calculate data size
		unsigned outerDataSize = DataUtils::getDataSize(outerData,
				this->outerAttributes);
		this->bufferSize += outerDataSize;

		// new Tuple
		Tuple * tuple = new Tuple(outerData, outerDataSize);
		// save in vector
		this->buffer.push_back(tuple);
	}

	// set bufferIndex
	this->bufferIndex = 0;
	free(outerData);
	return 0;
}

RC BNLJoin::clearBuffer() {
	if (!this->buffer.empty()) {
		this->buffer.clear();
	} else {
		for (int i = 0; i < this->buffer.size(); i++) {
			delete this->buffer[i];
		}
	}
	this->bufferSize = 0;
	return 0;
}

/******************************** INLJoin ********************************/
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition) {

	this->outer = leftIn;
//	this->inner = rightIn;
//	this->inner = new IndexScan(rightIn->rm, rightIn->tableName,
//			rightIn->attrName);
	this->inner = rightIn;

	/*cerr*/
//	float temp = 42;
//	void * ce = malloc(PAGE_SIZE);
//	memcpy(ce, &temp, sizeof(float));
//	inner->setIterator(ce, ce, true, true);
//	for (int i = 0; i < 10; i++) {
//		if (inner->getNextTuple(ce) != EOF) {
//			cerr << "ce0:" << *(int*) ((byte*) ce + 1) << endl;
//			cerr << "ce1:" << *(float*) ((byte*) ce + 1 + 4) << endl;
//			cerr << "ce2:" << *(int*) ((byte*) ce + 1 + 8) << endl;
//		}
//	}
//	free(ce);
	this->outer->getAttributes(this->outerAttributes);
	this->inner->getAttributes(this->innerAttributes);

	this->condition = condition;

	// set outer position
	for (int i = 0; i < this->outerAttributes.size(); i++) {
		if (this->outerAttributes[i].name == this->condition.lhsAttr) {
			this->outerPosition = i;
			this->compareType = this->outerAttributes[i].type;
			break;
		}
	}

	// set inner position
	if (!this->condition.bRhsIsAttr) {
		cerr << "error not join condition" << endl;
	}

	for (int i = 0; i < this->innerAttributes.size(); i++) {
		if (this->innerAttributes[i].name == this->condition.rhsAttr) {
			this->innerPosition = i;
			if (this->compareType != this->innerAttributes[i].type) {
				cerr << "error compare type not equal" << endl;
			}
			break;
		}
	}

	this->eof = false;

	innerData = malloc(PAGE_SIZE);
	outerData = malloc(PAGE_SIZE);

	innerAttributeData = malloc(PAGE_SIZE);
	innerAttributeDataNull = true;
	outerAttributeData = malloc(PAGE_SIZE);
	outerAttributeDataNull = true;

	this->eof = true;
	while (outer->getNextTuple(this->outerData) != QE_EOF) {
		DataUtils::getDataAtPosition(this->outerData, this->outerAttributeData,
				this->outerAttributeDataNull, this->outerPosition,
				this->outerAttributes);
		if (this->outerAttributeDataNull) {
			continue;
		} else {
			this->eof = false;
			inner->setIterator(this->outerAttributeData,
					this->outerAttributeData, true, true);
			break;
		}
//		void * leftKey = malloc(PAGE_SIZE);
//		void * rightKey = malloc(PAGE_SIZE);
//		memcpy(leftKey, this->outerAttributeData, PAGE_SIZE);
//		memcpy(rightKey, this->outerAttributeData, PAGE_SIZE);
//		free(leftKey);
//		free(rightKey);
	}

}

INLJoin::~INLJoin() {
	free(this->outerAttributeData);
	free(this->innerAttributeData);
	free(this->outerData);
	free(this->innerData);
}

/*
 * if(inner.next(data) != eof){
 * 	return 0;
 * }
 * while(outer.next(data) != eof){
 * 	set indexIterator;
 * 	if(inner.next(data)!=eof){
 * 		return 0;
 * 	}
 * }
 * set eof;
 * return eof;
 */
RC INLJoin::getNextTuple(void *data) {
	if (this->eof) {
		return QE_EOF;
	}

	// get inner rest
	if (inner->getNextTuple(this->innerData) != QE_EOF) {
		DataUtils::joinData(this->outerData, this->outerAttributes,
				this->innerData, this->innerAttributes, data);
		return 0;
	}

	// load next outer
	while (outer->getNextTuple(this->outerData) != QE_EOF) {

		DataUtils::getDataAtPosition(this->outerData, this->outerAttributeData,
				this->outerAttributeDataNull, this->outerPosition,
				this->outerAttributes);

		// point out attribute data as null
		if (this->outerAttributeDataNull) {
			continue;
//			free(this->outerAttributeData);
//			this->outerAttributeData = NULL;
		}

//		cerr << "set iterator" << endl;
		this->inner->setIterator(this->outerAttributeData, this->outerAttributeData,
				true, true);

		// reset out attribute data to memory
//		if(this->outerAttributeDataNull){
//			this->outerAttributeData = malloc(PAGE_SIZE);
//		}

		// get inner first
		if (inner->getNextTuple(this->innerData) != QE_EOF) {
			DataUtils::joinData(this->outerData, this->outerAttributes,
					this->innerData, this->innerAttributes, data);
			return 0;
		}
	}

	this->eof = true;
	return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	for (int i = 0; i < this->outerAttributes.size(); i++) {
		attrs.push_back(this->outerAttributes[i]);
	}
	for (int i = 0; i < this->innerAttributes.size(); i++) {
		attrs.push_back(this->innerAttributes[i]);
	}
}

/******************************** Aggregate ********************************/
// Mandatory
// Basic aggregation
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	this->iterator = input;

// /*cerr*/
//		void * cl = malloc(PAGE_SIZE);
//		cerr << "left" << endl;
//		for (int i = 0; i < 100; i++) {
//			cerr << "left:" << i << endl;
//			this->iterator->getNextTuple(cl);
//			cerr << "leftA:" << *(int*) ((byte*) cl + 1)
//					<< " leftB:" << *(int*) ((byte*) cl + 1 + 4)
//					<< " leftC:" << *(float*) ((byte*) cl + 1 + 8) << endl;
//		}
//		free(cl);

	this->eof = false;

	this->aggAttr = aggAttr;
	this->op = op;
	this->iterator->getAttributes(this->allAttributes);
//	this->gAttr.name = "INVALID";

	for (int i = 0; i < this->allAttributes.size(); i++) {
		// find matching
		if (this->aggAttr.name == this->allAttributes[i].name) {
//			cerr << "postion: " << i << endl;
			// save positions
			this->attrPosition = i;
			this->attrType = this->allAttributes[i].type;
			break;
		}
	}
//	if(this->gAttr.name == "INVALID"){
//		cerr << "error INVALID name" << endl;
//	}

//	this->allData = malloc(PAGE_SIZE);
//	this->attributeData = malloc(PAGE_SIZE);
//	this->attributeDataIsNull = true;
}
Aggregate::Aggregate(Iterator *input,             // Iterator of input R
		Attribute aggAttr, // The attribute over which we are computing an aggregate
		Attribute groupAttr, // The attribute over which we are grouping the tuples
		AggregateOp op              // Aggregate operation
		) {
	this->iterator = input;

	// /*cerr*/
	//		void * cl = malloc(PAGE_SIZE);
	//		cerr << "left" << endl;
	//		for (int i = 0; i < 100; i++) {
	//			cerr << "left:" << i << endl;
	//			this->iterator->getNextTuple(cl);
	//			cerr << "leftA:" << *(int*) ((byte*) cl + 1)
	//					<< " leftB:" << *(int*) ((byte*) cl + 1 + 4)
	//					<< " leftC:" << *(float*) ((byte*) cl + 1 + 8) << endl;
	//		}
	//		free(cl);

	this->eof = false;

	this->aggAttr = aggAttr;
	this->op = op;
	this->iterator->getAttributes(this->allAttributes);
	//	this->gAttr.name = "INVALID";

	for (int i = 0; i < this->allAttributes.size(); i++) {
		// find matching
		if (this->aggAttr.name == this->allAttributes[i].name) {
			//			cerr << "postion: " << i << endl;
			// save positions
			this->attrPosition = i;
			this->attrType = this->allAttributes[i].type;
			break;
		}
	}
	//	if(this->gAttr.name == "INVALID"){
	//		cerr << "error INVALID name" << endl;
	//	}

	//	this->allData = malloc(PAGE_SIZE);
	//	this->attributeData = malloc(PAGE_SIZE);
	//	this->attributeDataIsNull = true;
}

Aggregate::~Aggregate() {
}

// The attr being aggregated should only be of INT or REAL
RC Aggregate::getNextTuple(void *data) {
	if (this->eof) {
		return QE_EOF;
	}

	// deal with varchar
	if (this->attrType == TypeVarChar) {
		if (this->op != COUNT) {
			// set null indicator
			DataUtils::pakageSingleData(data, this->ngAggValue.countValue, 0,
					true);
		} else {

			void* allData = malloc(PAGE_SIZE);
			void* attributeData = malloc(PAGE_SIZE);
			bool attributeDataIsNull = true;

			// calculate count
			while (this->iterator->getNextTuple(allData) != QE_EOF) {
				DataUtils::getDataAtPosition(allData, attributeData,
						attributeDataIsNull, this->attrPosition,
						this->allAttributes);
				if (attributeDataIsNull) {
					continue;
				}
				this->ngAggValue.countValue++;
			}
			DataUtils::pakageSingleData(data, this->ngAggValue.countValue,
					sizeof(float), false);

			free(attributeData);
			free(allData);

		}
		this->eof = true;
		return 0;
	}

	// set all
	this->ngAggValue.maxValue = -9999999.9;
	this->ngAggValue.minValue = 9999999.9;
	this->ngAggValue.countValue = 0;
	this->ngAggValue.sumValue = 0;
	this->hasMax = false;
	this->hasMin = false;
	this->hasAvg = false;

	if (this->attrType == TypeInt) {

		// typeInt
		float temp;
		void* allData = malloc(PAGE_SIZE);
		void* attributeData = malloc(PAGE_SIZE);
		bool attributeDataIsNull = true;

		while (this->iterator->getNextTuple(allData) != QE_EOF) {

//			cerr << "ce0:" << *(int*) ((byte*) allData + 1) << endl;
//			cerr << "ce1:" << *(int*) ((byte*) allData + 1 + 4) << endl;
//			cerr << "ce2:" << *(float*) ((byte*) allData + 1 + 8) << endl;

			DataUtils::getDataAtPosition(allData, attributeData,
					attributeDataIsNull, attrPosition, this->allAttributes);

			if (attributeDataIsNull) {
				this->ngAggValue.countValue++;
				continue;
			}

			temp = (float) (*(int*) attributeData);
//			cerr << "int" << temp << endl;
			if (!this->hasAvg) {	// first not null time
				this->ngAggValue.maxValue = temp;
				this->hasMax = true;
				this->ngAggValue.minValue = temp;
				this->hasMin = true;
				this->hasAvg = true;
			}

			if (temp > this->ngAggValue.maxValue) {
				this->ngAggValue.maxValue = temp;
			}
			if (temp < this->ngAggValue.minValue) {
				this->ngAggValue.minValue = temp;
			}
			this->ngAggValue.sumValue += temp;
			this->ngAggValue.countValue++;
		}

		free(attributeData);
		free(allData);
	} else {
		float temp;

		void* allData = malloc(PAGE_SIZE);
		void* attributeData = malloc(PAGE_SIZE);
		bool attributeDataIsNull = true;

		while (this->iterator->getNextTuple(allData) != QE_EOF) {

			//		/*cerr*/
			//	//	float temp = 42;
			////		void * ce = malloc(PAGE_SIZE);
			//	//	memcpy(ce, &temp, sizeof(float));
			//	//	inner->setIterator(ce, ce, true, true);
			////		for (int i = 0; i < 10; i++) {
			////			if (inner->getNextTuple(ce) != EOF) {
			////				cerr << "ce0:" << *(int*) ((byte*) allData + 1) << endl;
			////				cerr << "ce1:" << *(int*) ((byte*) allData + 1 + 4) << endl;
			////				cerr << "ce2:" << *(float*) ((byte*) allData + 1 + 8) << endl;
			////			}
			////		}
			////		free(ce);
			DataUtils::getDataAtPosition(allData, attributeData,
					attributeDataIsNull, this->attrPosition,
					this->allAttributes);

			if (attributeDataIsNull) {
				this->ngAggValue.countValue++;
				continue;
			}

			temp = (float) (*(float*) attributeData);

//			cerr << "float" << temp << endl;
			if (!this->hasAvg) {			// first not null time
				this->ngAggValue.maxValue = temp;
				this->hasMax = true;
				this->ngAggValue.minValue = temp;
				this->hasMin = true;
				this->hasAvg = true;
			}

			if (temp > this->ngAggValue.maxValue) {
				this->ngAggValue.maxValue = temp;
			}
			if (temp < this->ngAggValue.minValue) {
				this->ngAggValue.minValue = temp;
			}
			this->ngAggValue.sumValue += temp;
			this->ngAggValue.countValue++;
		}

		free(attributeData);
		free(allData);
	}

	if (this->op == COUNT) {
		DataUtils::pakageSingleData(data, this->ngAggValue.countValue,
				sizeof(float), false);
	} else if (this->op == SUM) {
		if (!this->hasAvg) {
			DataUtils::pakageSingleData(data, this->ngAggValue.sumValue, 0,
					true);
		} else {
			DataUtils::pakageSingleData(data, this->ngAggValue.sumValue,
					sizeof(float), false);
		}
	} else if (this->op == AVG) {
		if (!this->hasAvg) {
			DataUtils::pakageSingleData(data, this->ngAggValue.avgValue, 0,
					true);
		} else {
			this->ngAggValue.avgValue = this->ngAggValue.sumValue
					/ this->ngAggValue.countValue;
			DataUtils::pakageSingleData(data, this->ngAggValue.avgValue,
					sizeof(float), false);
		}
	} else if (this->op == MIN) {
		// TODO: bug point
		if (!this->hasMin) {
			DataUtils::pakageSingleData(data, this->ngAggValue.minValue, 0,
					true);
		} else {
			DataUtils::pakageSingleData(data, this->ngAggValue.minValue,
					sizeof(float), false);
		}
	} else if (this->op == MAX) {
		if (!this->hasMax) {
			DataUtils::pakageSingleData(data, this->ngAggValue.maxValue, 0,
					true);
		} else {
			DataUtils::pakageSingleData(data, this->ngAggValue.maxValue,
					sizeof(float), false);
		}
	}

	this->eof = true;
	return 0;
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	Attribute attr = this->aggAttr;
	if(this->op == MAX){
		attr.name = "MAX(" + aggAttr.name + ")";
	} else if(this->op == MIN){
		attr.name = "MIN(" + aggAttr.name + ")";
	} else if(this->op == SUM){
		attr.name = "SUM(" + aggAttr.name + ")";
	} else if(this->op == AVG){
		attr.name = "AVG(" + aggAttr.name + ")";
	} else if(this->op == COUNT){
		attr.name = "COUNT(" + aggAttr.name + ")";
	}
	attrs.push_back(attr);
}

