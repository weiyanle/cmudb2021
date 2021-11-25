//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_pages_num_ = static_cast<int>(num_pages); }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mtx_.lock();
  if (!list_.empty()) {
    auto vic = list_.front();
    list_.pop_front();
    map_.erase(map_.find(vic));
    if (frame_id != nullptr) {
      *frame_id = vic;
    }
    mtx_.unlock();
    return true;
  }
  frame_id = nullptr;
  mtx_.unlock();
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx_.lock();
  if (map_.find(frame_id) != map_.end()) {
    list_.erase(map_[frame_id]);
    map_.erase(frame_id);
  }
  mtx_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx_.lock();
  if (map_.find(frame_id) != map_.end()) {
    mtx_.unlock();
    return;
  }
  if (static_cast<int>(list_.size()) == max_pages_num_) {
    frame_id_t *vic = nullptr;
    if (!Victim(vic)) {
      mtx_.unlock();
      return;
    }
  }
  list_.emplace_back(frame_id);
  map_[frame_id] = --list_.end();
  mtx_.unlock();
}

size_t LRUReplacer::Size() { return list_.size(); }

}  // namespace bustub
