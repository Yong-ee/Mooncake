#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "types.h"

namespace mooncake {

struct DistributedStorageConfig;

/**
 * @brief Abstract interface for DFS global allocators.
 *
 * Provides a unified API for space allocation, LRU tracking, and eviction.
 * Two concrete implementations:
 *   - DfsGlobalAllocator (shard-based, OffsetAllocator per shard)
 *   - BucketGlobalAllocator (bucket-based, atomic offset append)
 */
class GlobalAllocator {
   public:
    struct EvictionCandidate {
        std::string key;
        int shard_idx;
        uint64_t offset;
    };

    virtual ~GlobalAllocator() = default;

    GlobalAllocator(const GlobalAllocator&) = delete;
    GlobalAllocator& operator=(const GlobalAllocator&) = delete;
    GlobalAllocator(GlobalAllocator&&) = delete;
    GlobalAllocator& operator=(GlobalAllocator&&) = delete;

    virtual tl::expected<void, ErrorCode> Init(
        const DistributedStorageConfig& config) = 0;

    virtual bool IsInitialized() const = 0;

    virtual tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) = 0;

    virtual void Free(uint64_t offset, uint64_t aligned_size, int shard_idx,
                      const std::string& key) = 0;

    virtual void UpdateAccess(const std::string& key, int shard_idx,
                              uint64_t offset) = 0;

    virtual bool IsEvictionEnabled() const = 0;
    virtual std::chrono::seconds GetEvictionCheckInterval() const = 0;

    virtual std::vector<EvictionCandidate> PrepareEviction() = 0;
    virtual void ResolveEviction(std::vector<EvictionCandidate>& candidates,
                                 const std::vector<bool>& accepted) = 0;

   protected:
    GlobalAllocator() = default;
};

}  // namespace mooncake
