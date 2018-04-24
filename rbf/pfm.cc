#include "pfm.h"

using namespace std;

/**
 * class PagedFileManager
 */
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();
	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
	if (_pf_manager) {
		delete _pf_manager;
		_pf_manager = 0;
	}
}

/**
 * create file by file name
 * param: fileName
 * using C API manage file
 */
RC PagedFileManager::createFile(const string &fileName) {

	FILE * file = fopen(fileName.c_str(), "r");

	if (file != NULL) {
		fclose(file);
		cout
				<< "!!!fail to create file, file has already exist. in PagedFileManager::createFile"
				<< endl;
		return -1;
	}

	file = fopen(fileName.c_str(), "w+b");

	if (file == NULL) {
		cout << "!!!fail to create file in PagedFileManager::createFile"
				<< endl;
		return -1;
	}

	fclose(file);
	return 0;
}

/**
 * destroy file
 * param: fileName
 * using C API manage file
 */
RC PagedFileManager::destroyFile(const string &fileName) {
	if (remove(fileName.c_str()) == 0) {
		return 0;
	} else {
		cout << "!!!fail to remove file in PagedFileManager::destroyFile"
				<< endl;
		return -1;
	}
}

/**
 * open file
 * param: fileName
 * return: fileHandle
 */
RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return fileHandle.initFileHandle(fileName);
}

/**
 * close file
 * param: fileHandle
 * close fileHandle
 */
RC PagedFileManager::closeFile(FileHandle &fileHandle) {
	return fileHandle.closeFileHandle();
}

/**
 * class FileHandle
 */
FileHandle::FileHandle() {
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	filePointer = NULL;
	numberOfPages = 0;
}

FileHandle::~FileHandle() {
}

// copy the page from disk to memory
/**
 * read page
 * param: pageNum
 * return: data
 * read one page at a time from disk
 */
RC FileHandle::readPage(PageNum pageNum, void *data) {
	if (this->filePointer == NULL) {
		cout << "!!!file handle isn't init in FileHandle::readPage" << endl;
		return -1;
	}
	if (pageNum > this->getNumberOfPages()) {
		cout << "!!!page number out of bound in FileHandle::readPage" << endl;
		return -1;
	}
	if(fseek(this->filePointer, pageNum * PAGE_SIZE, SEEK_SET) != 0){
		cout << "!!!read page error in FileHandle::readPage" << endl;
		return -1;
	}
	if (fread(data, PAGE_SIZE, 1, this->filePointer) != 1) {
		cout << "!!!read page to memory not enough in FileHandle::readPage"
				<< endl;
		return -1;
	}
	this->readPageCounter++;
	return 0;
}

/**
 * write page
 * param: pageNum, data
 * write the page by page data from disk
 */
RC FileHandle::writePage(PageNum pageNum, const void *data) {
	if (pageNum > (this->getNumberOfPages() - 1)) {
		cout << "!!!page number out of bound in FileHandle::writePage" << endl;
		return -1;
	}
	if(fseek(this->filePointer, pageNum * PAGE_SIZE, SEEK_SET) != 0){
		cout << "!!!write page error in FileHandle::writePage" << endl;
		return -1;
	}
	if (fwrite(data, PAGE_SIZE, 1, this->filePointer) != 1) {
		cout << "!!!write page to disk not enough in FileHandle::writePage"
				<< endl;
		return -1;
	}
	this->writePageCounter++;
	return 0;
}

/**
 * append page
 * param: data
 * append a truely blank page at the tail of file
 */
RC FileHandle::appendPage(const void *data) {
	if (fseek(this->filePointer, 0, SEEK_END) != 0) {
		cout << "!!!append page error in FileHandle::appendPage" << endl;
		return -1;
	}
	if (fwrite(data, PAGE_SIZE, 1, this->filePointer) != 1) {
		cout << "!!!append page to disk error in FileHandle::appendPage"
				<< endl;
		return -1;
	}
	this->appendPageCounter++;
	this->setNumberOfPages();
	return 0;
}

/**
 * collect counter values
 * return: readPageCount, writePageCount, appendPageCount
 * test current state of file system.
 */
RC FileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}

/**
 * init file handler
 * param: fileName
 * initial the file handler
 */
RC FileHandle::initFileHandle(const string & fileName) {
	if ((this->filePointer = fopen(fileName.c_str(), "r+b")) == NULL) {
		cout << "!!!while trying to open " << fileName << endl;
		cout << "!!!fail to initial fileHandle in FileHandle::initFileHandle"
				<< endl;
		return -1;
	}
//	cout << "FILEHANDLE IS OPENING " << fileName << endl;
	// set number of page
	this->setNumberOfPages();
	return 0;
}

/**
 * close file handler
 * close current pointed file
 */
RC FileHandle::closeFileHandle() {
	return fclose(this->filePointer);
}

RC FileHandle::isOpen() {
	return (this->filePointer != NULL);
}

// getter & setter
// according to not mutli-thread visit system, prefer to not write it in get method
/**
 * set number of pages
 * set the number of pages according to current file length;
 * number of pages = file size / page size.
 */
void FileHandle::setNumberOfPages() {
	fseek(this->filePointer, 0, SEEK_END);
	this->numberOfPages = ftell(this->filePointer) / PAGE_SIZE;
	rewind(this->filePointer);
}

/**
 * return the number of pages
 */
/*TODO: why it will fail when not set at every time*/
unsigned FileHandle::getNumberOfPages() {
	this->setNumberOfPages();
	return this->numberOfPages;
}
