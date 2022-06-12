/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

void BufMgr::advanceClock() {
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId& frame) {
  unsigned int count = 1;
  Page page;
  File file;
  while (1) {
    if (count > numBufs*2){
      throw BufferExceededException();
    }
    count++;
    advanceClock();
    BufDesc &desc = bufDescTable[clockHand];
    file = desc.file;
    page = bufPool[clockHand];
    if (desc.valid == true) { // a set has been used
      if (desc.refbit == false) { // refbit = 0, go check next pinCnt
        if (desc.pinCnt == 0) {
          if (desc.dirty == true) {
            file.writePage(page); // write the modify file to disk
          }
          desc.clear();
          hashTable.remove(file, page.page_number());
          frame = clockHand;
          return;
        }
      } else { // refbit = 1, clear refbit= 0
          desc.refbit = false;
      }
    } else { // invalid set, so use it
      frame = clockHand;
      return;
    }
  }
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId frameNo;
  try {
    hashTable.lookup(file, pageNo, frameNo);
    BufDesc &desc = bufDescTable[frameNo];
    desc.refbit = true;
    desc.pinCnt += 1;
    page = &bufPool[frameNo];
  } catch (HashNotFoundException &e) {
    allocBuf(frameNo);
    bufPool[frameNo] = file.readPage(pageNo);
    page = &bufPool[frameNo];
    hashTable.insert(file, pageNo, frameNo);
    BufDesc &desc = bufDescTable[frameNo];
    desc.Set(file, pageNo);
  }
}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId frameNo;
  hashTable.lookup(file, pageNo, frameNo);
  BufDesc &desc = bufDescTable[frameNo];
  if (desc.pinCnt == 0) {
    throw PageNotPinnedException("The page is not already pinned!", pageNo, frameNo);
  } else {
    if (dirty) {
      desc.dirty = dirty;
    }
    desc.pinCnt--;
  }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  FrameId frameNo;
  Page newPage = file.allocatePage();
  pageNo = newPage.page_number();
  allocBuf(frameNo);
  bufPool[frameNo] = newPage;
  page = &bufPool[frameNo];
  hashTable.insert(file, pageNo, frameNo);
  BufDesc &desc = bufDescTable[frameNo];
  desc.Set(file, pageNo);
}

void BufMgr::flushFile(File& file) {
  Page page;

  for (FrameId i = 0; i < numBufs; i++) {
    BufDesc &desc = bufDescTable[i];
    page = bufPool[i];
    if (desc.pinCnt != 0) {
      throw PagePinnedException("Some page of the file is pinned in the buffer pool!", page.page_number(), i);
    }
    if (!desc.valid) {
      throw BadBufferException(i, desc.dirty, desc.valid, desc.refbit);
    }
    if (desc.dirty) {
      file.writePage(page);
    }
    try {
      hashTable.remove(file, page.page_number());
    } catch (HashNotFoundException &e) {}
    desc.clear(); // dirty = false
  }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId frameNo;
  try {
    hashTable.lookup(file, PageNo, frameNo);
    bufDescTable[frameNo].clear();
    hashTable.remove(file, PageNo);
  } catch (HashNotFoundException &e) {}
  file.deletePage(PageNo);
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
