#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "storage/distributed/global_allocator.h"
#include "storage_backend.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Bucket-based global allocator for DFS.
 *
 * Uses fixed-size bucket files (default 256MB) with atomic offset append.
 * Keys are 4096-aligned within a bucket. Metadata is persisted to .meta files
 * for restart recovery. Eviction operates at bucket granularity via LRU.
 *
 * This is an alternative to DfsGlobalAllocator (shard-based).
 */
class BucketGlobalAllocator : public GlobalAllocator {
   public:
    BucketGlobalAllocator() = default;
    ~BucketGlobalAllocator() override;

    BucketGlobalAllocator(const BucketGlobalAllocator&) = delete;
    BucketGlobalAllocator& operator=(const BucketGlobalAllocator&) = delete;

    // GlobalAllocator interface
    tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) override;
    bool IsInitialized() const override {
        return initialized_.load(std::memory_order_acquire);
    }

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;
    void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
              const std::string& key) override;
    void UpdateAccess(const std::string& key, int shard_idx,
                      uint64_t offset) override;
    bool IsEvictionEnabled() const override { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const override {
        return eviction_check_interval_;
    }

    std::vector<EvictionCandidate> PrepareEviction() override;
    void ResolveEviction(std::vector<EvictionCandidate>& candidates,
                         const std::vector<bool>& accepted) override;

   private:
    struct BucketState {
        int64_t bucket_id = 0;
        std::atomic<uint64_t> append_offset{0};
        uint64_t capacity = 0;
        std::vector<std::string> keys;
        std::unordered_map<std::string, BucketObjectMetadata> key_metadata;

        std::atomic<int64_t> last_access_ns{0};
        std::atomic<int32_t> inflight_reads{0};
        std::atomic<bool> evicting{false};

        mutable std::shared_mutex mutex;

        BucketState() = default;
        BucketState(int64_t id, uint64_t cap)
            : bucket_id(id), capacity(cap) {}
    };

    std::string GetBucketDataPath(int64_t bucket_id) const;
    std::string GetBucketMetaPath(int64_t bucket_id) const;
    static std::string FormatBucketId(int64_t bucket_id);
    uint64_t AlignSize(uint64_t size) const;
    int64_t CreateNewBucket();
    void PersistMetadata(int64_t bucket_id);
    void RecoverFromDisk();
    int64_t SelectBucketForEviction() const;

    std::string fsdir_;
    uint64_t bucket_capacity_ = 256 * 1024 * 1024;
    uint64_t alignment_ = 4096;

    std::unordered_map<int64_t, std::shared_ptr<BucketState>> buckets_;
    std::unordered_map<std::string, int64_t> key_to_bucket_;
    int64_t next_bucket_id_ = 0;
    int64_t active_bucket_id_ = 0;

    mutable std::mutex allocator_mutex_;

    std::list<int64_t> lru_list_;
    std::unordered_map<int64_t, std::list<int64_t>::iterator> lru_index_;

    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::thread eviction_thread_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    std::chrono::seconds eviction_check_interval_{5};
    int initial_bucket_count_ = 5;
};

}  // namespace mooncake
