
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return getRangeIteratorAltHelper (low, high, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return getRangeIteratorAltHelper (low, high, false);
}

void MyDB_BPlusTreeReaderWriter :: printTree() {
	printTreeHelper(rootLocation, 0);
}

void MyDB_BPlusTreeReaderWriter :: printTreeHelper(int whichPage, int level) {
	MyDB_PageReaderWriter curPage = (*this)[whichPage];
	

	MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();
	switch (curPage.getType()) {
	case RegularPage:
	{
		MyDB_RecordPtr curRec = getEmptyRecord();

		cout << "Leaf node: \n";
		while(it->advance()) {
			it->getCurrent(curRec);
			cout << curRec << "\n";
		}
		cout << endl;
		return;
	}
	case DirectoryPage:
	{
		MyDB_INRecordPtr curInRec = getINRecord();

		cout << "Level: " << level << ", Interanal Node: \t";
		while(it->advance()) {
			it->getCurrent(curInRec);
			cout << curInRec->getKey() << endl;
			printTreeHelper(curInRec->getPtr(), level + 1);
		}

	}
	default:
		break;
	}		
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAltHelper (MyDB_AttValPtr low, MyDB_AttValPtr high, bool sort) { 
	vector <MyDB_PageReaderWriter> list;

	discoverPages(rootLocation, list, low, high);
	
	MyDB_RecordPtr lhs = getEmptyRecord(), rhs = getEmptyRecord(), myRec = getEmptyRecord();
	MyDB_INRecordPtr lowInRec = getINRecord(), highInRec = getINRecord();

	lowInRec->setKey(low);
	highInRec->setKey(high);

	function <bool ()> comparator = (*this).buildComparator (lhs, rhs);
	function <bool ()> lowComparator = (*this).buildComparator (myRec, lowInRec);
	function <bool ()> highComparator = (*this).buildComparator (highInRec, myRec);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt> (list, lhs, 
		rhs, comparator, myRec, lowComparator, 
		highComparator, sort);
}

bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, 
										MyDB_AttValPtr low, MyDB_AttValPtr high) {
	MyDB_PageReaderWriter curPage = (*this)[whichPage];
	MyDB_INRecordPtr curRec = getINRecord();

	switch (curPage.getType()) {
	case RegularPage:
		list.push_back(curPage);
		return true;
	case DirectoryPage:
	{
		MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();

		MyDB_INRecordPtr lowInRec = getINRecord();
		MyDB_INRecordPtr highInRec = getINRecord();
		
		lowInRec->setKey(low);
		highInRec->setKey(high);

		function <bool ()> myCompLow = buildComparator (curRec, lowInRec);
		function <bool ()> myCompHigh = buildComparator (highInRec, curRec);

		while(it->advance()) {
			it->getCurrent(curRec);
		
			if (!myCompLow()) {
				discoverPages(curRec->getPtr(), list, low, high);
			} 

			if(myCompHigh()) {
				break;
			}
		}

		break;
	}
	default:
		break;
	}		

	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr addMe) {
	if (rootLocation == -1) {
		getTable()->setLastPage(0);
		rootLocation = 0;

		MyDB_PageReaderWriter newRoot = (*this)[0];
		newRoot.clear();
		newRoot.setType(DirectoryPage);
		
		MyDB_INRecordPtr initInRec = getINRecord();
		getTable()->setLastPage(1);
		initInRec->setPtr(1);

		MyDB_PageReaderWriter newLeaf = (*this)[1];
		newLeaf.clear();
		newLeaf.setType(RegularPage);
		newRoot.append(initInRec);
	} 

	MyDB_RecordPtr newInRec = append(rootLocation, addMe);
	if (newInRec != nullptr) {
		MyDB_INRecordPtr prevRootInRec = getINRecord();
		prevRootInRec->setPtr(rootLocation);

		int newRootPageNum = getTable()->lastPage() + 1;
		getTable()->setLastPage(newRootPageNum);

		MyDB_PageReaderWriter newRoot = (*this)[newRootPageNum];
		newRoot.clear();
		newRoot.setType(DirectoryPage);
		newRoot.append(newInRec);
		newRoot.append(prevRootInRec);

		rootLocation = newRootPageNum;
	}
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr addMe) {
	MyDB_INRecordPtr newInRec = getINRecord();
	auto lhs = splitMe.getType() == RegularPage ? getEmptyRecord() : getINRecord();
	auto rhs = splitMe.getType() == RegularPage ? getEmptyRecord() : getINRecord();

	int newPageLoc = getTable()->lastPage() + 1, recNum = 0;
	getTable()->setLastPage(newPageLoc);

    MyDB_PageReaderWriter newPage = (*this)[newPageLoc];
    MyDB_PageReaderWriter tmpPage = (*this)[newPageLoc + 1];
    newPage.clear();
	tmpPage.clear();

	function <bool ()> myComp;
	function <bool ()> myComparator = buildComparator(lhs, rhs);

	MyDB_PageType splitMeType = splitMe.getType();
	tmpPage.setType(splitMeType);
	newPage.setType(splitMeType);

	if (splitMeType == RegularPage) {
		// Sort before start finding median
		splitMe.sortInPlace(myComparator, lhs, rhs);
	}

	// Count records num in page
	MyDB_RecordIteratorAltPtr it = splitMe.getIteratorAlt();
	while (it->advance()) {
		it->getCurrent(lhs);
		recNum++;
	}
	
	// Append lower 1/2 of the records to the new page
	int mid = recNum / 2;
	it = splitMe.getIteratorAlt();
	for (int i = 0; it->advance() && i < recNum; i++) {
		it->getCurrent(lhs);

		if (i <= mid) {
			newPage.append(lhs);
		} else if (i > mid) {
			tmpPage.append(lhs);
		} 
		
		if (i == mid) {
			newInRec->setKey(getKey(lhs));
			newInRec->setPtr(newPageLoc);
		}
	}

	myComp = buildComparator(newInRec, addMe);
	if (myComp()) {
		tmpPage.append(addMe);				
		tmpPage.sortInPlace(myComparator, lhs, rhs);
	} else {
		newPage.append(addMe);
		newPage.sortInPlace(myComparator, lhs, rhs);
	}

	// Only the upper 1/2 remains in the original page
	splitMe.clear();
	splitMe.setType(splitMeType);

	it = tmpPage.getIteratorAlt();
	while(it->advance()) {
		it->getCurrent(lhs);
		splitMe.append(lhs);
	}

	tmpPage.clear();
		
	return newInRec;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
	MyDB_PageReaderWriter curPage = (*this)[whichPage];

	// Inner node
	if (appendMe->getSchema() == nullptr) { 
		if (curPage.append(appendMe)) {
			MyDB_INRecordPtr lhs = getINRecord(), rhs = getINRecord();
			function<bool()> myComp = buildComparator(lhs, rhs);

			curPage.sortInPlace(myComp, lhs, rhs);
		} else {
			return split(curPage, appendMe);
		}
	//Regular node
	} else {
		switch (curPage.getType()) {
		case RegularPage:
		{
			if (!curPage.append(appendMe)) {
				return split(curPage, appendMe);
			}
			break;
		}
		case DirectoryPage: 
		{
			MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();
			MyDB_INRecordPtr curInRec = getINRecord();
			function<bool()> myComp = buildComparator(appendMe, curInRec);

			while (it->advance()) {
				it->getCurrent(curInRec);
				if (myComp()) {
					MyDB_RecordPtr newRec = append(curInRec->getPtr(), appendMe);
					if (newRec != nullptr) {
						return append(whichPage, newRec);
					} 

					break;
				}
			}
			break;
		}
		default:
			break;
		}
	}

	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

#endif
