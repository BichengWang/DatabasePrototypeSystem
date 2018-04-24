#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <cmath>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan


#define FLOAT_MIN -999999999.9;
#define FLOAT_MAX 999999999.9;

using namespace std;

typedef enum {
	MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
	AttrType type;          // type of value
	void *data;         // value
};

struct Condition {
	string lhsAttr;        // left-hand side attribute
	CompOp op;             // comparison operator
	bool bRhsIsAttr; // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
	string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
	Value rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
	// All the relational operators and access methods are iterators.
public:
	virtual RC getNextTuple(void *data) = 0;
	virtual void getAttributes(vector<Attribute> &attrs) const = 0;
	virtual ~Iterator() {
	}
	;
};

class TableScan: public Iterator {
	// A wrapper inheriting Iterator over RM_ScanIterator
public:
	RelationManager &rm;
	RM_ScanIterator *iter;
	string tableName;
	vector<Attribute> attrs;
	vector<string> attrNames;
	RID rid;

	TableScan(RelationManager &rm, const string &tableName, const char *alias =
			NULL) :
			rm(rm) {
		//Set members
		this->tableName = tableName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Get Attribute Names from RM
		unsigned i;
		for (i = 0; i < attrs.size(); ++i) {
			// convert to char *
			attrNames.push_back(attrs.at(i).name);
		}

		// Call RM scan to get an iterator
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new compOp and value
	void setIterator() {
		iter->close();
		delete iter;
		iter = new RM_ScanIterator();
		rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
	}
	;

	RC getNextTuple(void *data) {
		return iter->getNextTuple(rid, data);
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~TableScan() {
		iter->close();
	}
	;
};

class IndexScan: public Iterator {
	// A wrapper inheriting Iterator over IX_IndexScan
public:
	RelationManager &rm;
	RM_IndexScanIterator *iter;
	string tableName;
	string attrName;
	vector<Attribute> attrs;
	char key[PAGE_SIZE];
	RID rid;

	IndexScan(RelationManager &rm, const string &tableName,
			const string &attrName, const char *alias = NULL) :
			rm(rm) {
		// Set members
		this->tableName = tableName;
		this->attrName = attrName;

		// Get Attributes from RM
		rm.getAttributes(tableName, attrs);

		// Call rm indexScan to get iterator
		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

		// Set alias
		if (alias)
			this->tableName = alias;
	}
	;

	// Start a new iterator given the new key range
	void setIterator(void* lowKey, void* highKey, bool lowKeyInclusive,
			bool highKeyInclusive) {
		iter->close();
		delete iter;

		iter = new RM_IndexScanIterator();
		rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
				highKeyInclusive, *iter);
	}
	;

	RC getNextTuple(void *data) {
		int rc = iter->getNextEntry(rid, key);
		if (rc == 0) {
			rc = rm.readTuple(tableName.c_str(), rid, data);
		}
		return rc;
	}
	;

	void getAttributes(vector<Attribute> &attrs) const {
		attrs.clear();
		attrs = this->attrs;
		unsigned i;

		// For attribute in vector<Attribute>, name it as rel.attr
		for (i = 0; i < attrs.size(); ++i) {
			string tmp = tableName;
			tmp += ".";
			tmp += attrs.at(i).name;
			attrs.at(i).name = tmp;
		}
	}
	;

	~IndexScan() {
		iter->close();
	}
	;
};

class Filter: public Iterator {
	// Filter operator
public:
	Filter(Iterator *input,               // Iterator of input R
			const Condition &condition     // Selection condition
			);
	~Filter();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

protected:
//        bool filterCompare(const void* leftData, const void* rightData, CompOp &op, AttrType &type);
//
//        // get value from data
//        RC getDataPositions(const void * data, unsigned * positions);
//        RC getDataAtPosition(void * data, void * returnData, const unsigned & position);

private:
	Iterator * iterator;
	Condition condition;
	vector<Attribute> attributes;
	unsigned leftPosition;
	unsigned rightPosition;
	bool eof;
	Value leftValue;
	Value rightValue;
};

class Project: public Iterator {
	// Projection operator
public:
	Project(Iterator *input,                    // Iterator of input R
			const vector<string> &attrNames); // vector containing attribute names
	~Project();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;
protected:
//        RC getDataPositions(const void * data, unsigned * positions);
private:
	Iterator * iterator;
	vector<Attribute> allAttributes;
	vector<Attribute> attributes;
	vector<unsigned> attributesPositions;
	bool eof;
};

class Tuple {
public:
	Tuple(void * data, int length);
	~Tuple();
	void * data;
	int length;
};

class BNLJoin: public Iterator {
	// Block nested-loop join operator
public:
	BNLJoin(Iterator *leftIn,            // Iterator of input R
			TableScan *rightIn,           // TableScan Iterator of input S
			const Condition &condition,   // Join condition
			const unsigned numPages // # of pages that can be loaded into memory,
									//   i.e., memory block size (decided by the optimizer)
			);
	~BNLJoin();

	RC getNextTuple(void *data);
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

protected:

	RC loadBuffer();

	RC clearBuffer();

private:
	Iterator * outer;
	TableScan * inner;

	long bufferLimitation;
	long bufferSize;

	vector<Tuple *> buffer;
	void * innerData;

	unsigned bufferIndex;

	vector<Attribute> outerAttributes;
	vector<Attribute> innerAttributes;

	Condition condition;
	AttrType compareType;

	unsigned outerPosition;
	unsigned innerPosition;

	bool eof;
	bool bufferEof;

	void * innerAttributeData;
	bool innerAttributeDataNull;
	void * outerAttributeData;
	bool outerAttributeDataNull;
};

class INLJoin: public Iterator {
	// Index nested-loop join operator
public:
	INLJoin(Iterator *leftIn,           // Iterator of input R
			IndexScan *rightIn,          // IndexScan Iterator of input S
			const Condition &condition   // Join condition
			);

	~INLJoin();

	RC getNextTuple(void *data);

	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const;

protected:
private:
	Iterator * outer;
	IndexScan * inner;

	vector<Attribute> outerAttributes;
	vector<Attribute> innerAttributes;

	Condition condition;
	AttrType compareType;

	unsigned outerPosition;
	unsigned innerPosition;

	bool eof;

	void * innerData;
	void * outerData;

	void * innerAttributeData;
	bool innerAttributeDataNull;
	void * outerAttributeData;
	bool outerAttributeDataNull;
};

// Optional for everyone. 10 extra-credit points
class GHJoin: public Iterator {
	// Grace hash join operator
public:
	GHJoin(Iterator *leftIn,               // Iterator of input R
			Iterator *rightIn,               // Iterator of input S
			const Condition &condition,  // Join condition (CompOp is always EQ)
			const unsigned numPartitions // # of partitions for each relation (decided by the optimizer)
			) {
	}
	;
	~GHJoin() {
	}
	;

	RC getNextTuple(void *data) {
		return QE_EOF;
	}
	;
	// For attribute in vector<Attribute>, name it as rel.attr
	void getAttributes(vector<Attribute> &attrs) const {
	}
	;
};

// all of these values should be floats as described
struct AggValue {
	float minValue;
	float maxValue;
	float avgValue;
	float sumValue;
	float countValue;
};



class Aggregate: public Iterator {
	// Aggregation operator
public:
	// Mandatory
	// Basic aggregation
	Aggregate(Iterator *input,          // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			AggregateOp op            // Aggregate operation
			);

	// Optional for everyone: 5 extra-credit points
	// Group-based hash aggregation
	Aggregate(Iterator *input,             // Iterator of input R
			Attribute aggAttr, // The attribute over which we are computing an aggregate
			Attribute groupAttr, // The attribute over which we are grouping the tuples
			AggregateOp op              // Aggregate operation
			);

	~Aggregate();

	RC getNextTuple(void *data);
	// Please name the output attribute as aggregateOp(aggAttr)
	// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
	// output attrname = "MAX(rel.attr)"
	void getAttributes(vector<Attribute> &attrs) const;
private:
	bool eof;
	Iterator * iterator;              // input iterator
	Attribute aggAttr;                // attr to be aggregated
	AggregateOp op;                   // operand
	vector<Attribute> allAttributes;  // attrs fetched from iterator
	unsigned attrPosition;         // attr location in attributes if not grouped
	AggValue ngAggValue;              // AggValue struct if not grouped

	Attribute gAttr;                  // attr to be grouped (not for now)

	AttrType attrType;


	bool hasMax = false;
	bool hasMin = false;
	bool hasAvg = false;
};

#endif
