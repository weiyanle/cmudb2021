//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include <common/logger.h>

namespace bustub {

    ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                         LogManager *log_manager) {
        // Allocate and create individual BufferPoolManagerInstances
        for (uint32_t i = 0; i < num_instances; i++) {
            inss_.emplace_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
        }
        start_index_ = 0;
        pool_size_ = num_instances * pool_size;
    }

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
    ParallelBufferPoolManager::~ParallelBufferPoolManager() {
        for (auto ins : inss_) {
            delete (ins);
        }
    }

    size_t ParallelBufferPoolManager::GetPoolSize() {
        // Get size of all BufferPoolManagerInstances
        return pool_size_;
    }

    BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
        // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
        return inss_[page_id % inss_.size()];
    }

    Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
        // Fetch page for page_id from responsible BufferPoolManagerInstance
        return GetBufferPoolManager(page_id)->FetchPage(page_id);
    }

    bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
        // Unpin page_id from responsible BufferPoolManagerInstance
        return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->UnpinPgImp(page_id, is_dirty);
    }

    bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
        // Flush page_id from responsible BufferPoolManagerInstance
        return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->FlushPgImp(page_id);
    }

    Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
        // create new page. We will request page allocation in a round robin manner from the underlying
        // BufferPoolManagerInstances
        // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
        // starting index and return nullptr
        // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
        // is called

        Page *ret = nullptr;
        if (!inss_.empty()) {
            for (uint32_t tmp = 0; tmp < inss_.size(); ++tmp) {
                if ((ret = dynamic_cast<BufferPoolManagerInstance *>(inss_[(start_index_ + tmp) % inss_.size()])
                        ->NewPgImp(page_id)) != nullptr) {
                    break;
                }
            }
            start_index_ = (start_index_ + 1) % inss_.size();
        }
        return ret;
    }

    bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
        // Delete page_id from responsible BufferPoolManagerInstance
        return dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id))->DeletePgImp(page_id);
    }

    void ParallelBufferPoolManager::FlushAllPgsImp() {
        // flush all pages from all BufferPoolManagerInstances
        for (auto ins : inss_) {
            dynamic_cast<BufferPoolManagerInstance *>(ins)->FlushAllPgsImp();
        }
    }

}  // namespace bustub
