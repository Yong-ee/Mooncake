#include "storage/distributed/dfs_global_allocator.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "utils.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

DfsGlobalAllocator::PendingEviction::~PendingEviction() {
    if (owner_ != nullptr) {
        owner_->RestorePreparedEviction(std::move(*this));
    }
}

DfsGlobalAllocator::PendingEviction::PendingEviction(
    PendingEviction&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      candidates_(std::move(other.candidates_)),
      prepared_(std::move(other.prepared_)) {}

DfsGlobalAllocator::~DfsGlobalAllocator() {
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DfsGlobalAllocator::Init(
    const DistributedStorageConfig& config) {
    if (initialized_.load(std::memory_order_acquire)) return {};
    if (!config.ValidateForAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    mount_path_ = config.fsdir;
    shard_count_ = config.shard_count;
    alignment_ = config.alignment;
    shards_.clear();
    shards_.resize(shard_count_);

    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    deferred_free_duration_ = config.deferred_free_duration;
    eviction_check_interval_ = config.eviction_check_interval;

    std::error_code ec;
    std::filesystem::create_directories(mount_path_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS mount path " << mount_path_ << ": "
                   << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (config.fs_adapter_type == "posix") {
        fs_adapter_ = std::make_unique<PosixFsAdapter>();
    } else if (config.fs_adapter_type == "hf3fs") {
#ifdef USE_3FS
        fs_adapter_ = std::make_unique<Hf3fsAdapter>();
#else
        LOG(ERROR) << "The hf3fs DFS adapter requires Mooncake to be built "
                      "with the USE_3FS compile-time option "
                      "(-DUSE_3FS=ON)";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
#endif
    }

    auto adapter_init = fs_adapter_->Init(mount_path_);
    if (!adapter_init) {
        LOG(ERROR) << "Failed to initialize DFS fs adapter "
                   << config.fs_adapter_type
                   << " for mount_path=" << mount_path_
                   << ", error=" << adapter_init.error();
        return tl::make_unexpected(adapter_init.error());
    }

    for (int i = 0; i < shard_count_; ++i) {
        std::string path = mount_path_ + "/dfs_shard_" +
                           FormatShardIdx(i, shard_count_) + ".data";
        auto prealloc =
            fs_adapter_->PreallocateFile(path, config.shard_capacity);
        if (!prealloc) {
            LOG(ERROR) << "Failed to preallocate DFS shard " << path << ": "
                       << prealloc.error();
            return tl::make_unexpected(prealloc.error());
        }

        auto shard = std::make_unique<ShardState>();
        shard->capacity = config.shard_capacity;
        uint32_t init_cap = static_cast<uint32_t>(std::max<uint64_t>(
            1, std::min<uint64_t>(config.shard_capacity / 4096, 64ULL * 1024)));
        uint32_t max_cap = static_cast<uint32_t>(std::max<uint64_t>(
            init_cap, std::min<uint64_t>(config.shard_capacity / 1024,
                                         64ULL * 1024 * 1024)));
        shard->allocator = OffsetAllocator::create(0, config.shard_capacity,
                                                   init_cap, max_cap);
        if (!shard->allocator) {
            LOG(ERROR) << "Failed to create offset allocator for DFS shard "
                       << i;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        shards_[i] = std::move(shard);
    }

    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<DistributedFSDescriptor, ErrorCode> DfsGlobalAllocator::Allocate(
    const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto key_lock = LockKey(key);
    int shard_idx = SelectShard(key);
    uint64_t aligned_size = AlignSize(size);
    auto& shard = *shards_[shard_idx];

    std::unique_lock handle_lock(shard.handle_mutex);
    ProcessPendingFrees(shard_idx);

    const uint64_t allocation_size = aligned_size + alignment_ - 1;
    const uint64_t reserved_bytes =
        shard.allocator->normalizedAllocationSize(allocation_size);
    auto handle = shard.allocator->allocate(allocation_size);
    if (!handle) return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);

    uint64_t raw_offset = handle->address();
    uint64_t alloc_offset = AlignSize(raw_offset);
    auto alloc_handle =
        std::make_shared<OffsetAllocationHandle>(std::move(*handle));
    shard.offset_to_handle[alloc_offset] = {key, std::move(alloc_handle),
                                            reserved_bytes};
    handle_lock.unlock();

    return DistributedFSDescriptor{
        mount_path_ + "/dfs_shard_" + FormatShardIdx(shard_idx, shard_count_) +
            ".data",
        alloc_offset,
        size,
        aligned_size,
        shard_idx,
    };
}

void DfsGlobalAllocator::Free(uint64_t offset, uint64_t /*aligned_size*/,
                              int shard_idx, const std::string& key) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    if (shard_idx < 0 || shard_idx >= shard_count_) return;

    auto& shard = *shards_[shard_idx];
    std::lock_guard lru_lock(shard.lru_mutex);
    std::lock_guard handle_lock(shard.handle_mutex);

    auto lru_it = shard.lru_index.find(key);
    if (lru_it != shard.lru_index.end() && lru_it->second->second == offset) {
        shard.lru_list.erase(lru_it->second);
        shard.lru_index.erase(lru_it);
    }

    auto it = shard.offset_to_handle.find(offset);
    if (it == shard.offset_to_handle.end()) return;
    if (it->second.key != key) return;

    QueuePendingFree(
        shard, it->second.handle, it->second.bytes,
        std::chrono::steady_clock::now() + deferred_free_duration_);
    shard.offset_to_handle.erase(it);
}

void DfsGlobalAllocator::UpdateAccess(const std::string& key, int shard_idx,
                                      uint64_t offset) {
    if (!initialized_.load(std::memory_order_acquire)) return;
    if (shard_idx < 0 || shard_idx >= shard_count_) return;

    auto& shard = *shards_[shard_idx];
    std::lock_guard lru_lock(shard.lru_mutex);
    {
        std::shared_lock handle_lock(shard.handle_mutex);
        auto handle_it = shard.offset_to_handle.find(offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != key ||
            handle_it->second.eviction_prepared) {
            return;
        }
    }
    auto lru_it = shard.lru_index.find(key);
    if (lru_it != shard.lru_index.end()) {
        lru_it->second->second = offset;
        shard.lru_list.splice(shard.lru_list.begin(), shard.lru_list,
                              lru_it->second);
    } else {
        shard.lru_list.push_front({key, offset});
        shard.lru_index[key] = shard.lru_list.begin();
    }
}

// === Internal eviction helpers ===

void DfsGlobalAllocator::ProcessPendingFrees(int shard_idx) {
    auto& shard = *shards_[shard_idx];
    CleanupExpiredPendingFrees(shard, std::chrono::steady_clock::now());
}

void DfsGlobalAllocator::QueuePendingFree(
    ShardState& shard, const std::shared_ptr<OffsetAllocationHandle>& handle,
    uint64_t bytes, std::chrono::steady_clock::time_point when) {
    if (!handle) return;
    std::lock_guard pending_lock(shard.pending_mutex);
    if (bytes == 0) bytes = handle->size();
    shard.pending_free.push_back({handle, bytes, when});
    shard.pending_free_bytes += bytes;
}

void DfsGlobalAllocator::CleanupExpiredPendingFrees(
    ShardState& shard, std::chrono::steady_clock::time_point now) {
    std::lock_guard pending_lock(shard.pending_mutex);
    while (!shard.pending_free.empty() &&
           shard.pending_free.front().when <= now) {
        const uint64_t bytes = shard.pending_free.front().bytes;
        shard.pending_free.pop_front();
        if (bytes > shard.pending_free_bytes) {
            shard.pending_free_bytes = 0;
        } else {
            shard.pending_free_bytes -= bytes;
        }
    }
}

double DfsGlobalAllocator::EffectiveUsage(ShardState& shard) {
    uint64_t physical_free = 0;
    {
        std::shared_lock lock(shard.handle_mutex);
        auto report = shard.allocator->storageReport();
        physical_free = report.totalFreeSpace;
    }

    uint64_t pending_free_bytes = 0;
    {
        std::lock_guard pending_lock(shard.pending_mutex);
        pending_free_bytes = shard.pending_free_bytes;
    }

    const uint64_t capped_physical_free =
        std::min<uint64_t>(physical_free, shard.capacity);
    const uint64_t remaining_capacity = shard.capacity - capped_physical_free;
    const uint64_t effective_free =
        capped_physical_free + std::min(pending_free_bytes, remaining_capacity);
    if (shard.capacity == 0) return 0.0;
    return 1.0 - static_cast<double>(effective_free) /
                     static_cast<double>(shard.capacity);
}

void DfsGlobalAllocator::PrepareEvictionFromShard(int shard_idx,
                                                  PendingEviction& pending) {
    auto& shard = *shards_[shard_idx];

    const double usage = EffectiveUsage(shard);
    uint64_t prepared_bytes = 0;
    std::lock_guard lru_lock(shard.lru_mutex);
    std::lock_guard handle_lock(shard.handle_mutex);

    if (usage >= eviction_high_watermark_) {
        shard.eviction_active = true;
    }
    if (!shard.eviction_active) return;
    if (usage < eviction_low_watermark_) {
        shard.eviction_active = false;
        return;
    }

    while (true) {
        if (shard.lru_list.empty()) break;
        auto lru_it = std::prev(shard.lru_list.end());
        const std::string evict_key = lru_it->first;
        const uint64_t evict_offset = lru_it->second;

        auto handle_it = shard.offset_to_handle.find(evict_offset);
        if (handle_it == shard.offset_to_handle.end() ||
            handle_it->second.key != evict_key ||
            handle_it->second.eviction_prepared) {
            shard.lru_list.erase(lru_it);
            shard.lru_index.erase(evict_key);
            continue;
        }

        EvictionCandidate candidate{evict_key, shard_idx, evict_offset};
        PendingEviction::PreparedAllocation prepared{
            candidate, handle_it->second.handle, handle_it->second.bytes};
        pending.prepared_.push_back(std::move(prepared));
        try {
            pending.candidates_.push_back(std::move(candidate));
        } catch (...) {
            pending.prepared_.pop_back();
            throw;
        }

        handle_it->second.eviction_prepared = true;
        prepared_bytes += handle_it->second.bytes;
        shard.lru_list.erase(lru_it);
        shard.lru_index.erase(evict_key);

        const double projected_usage =
            usage - static_cast<double>(prepared_bytes) /
                        static_cast<double>(shard.capacity);
        if (projected_usage < eviction_low_watermark_) break;
    }
}

int DfsGlobalAllocator::SelectShard(const std::string& key) const {
    return std::hash<std::string>{}(key) % shard_count_;
}

uint64_t DfsGlobalAllocator::AlignSize(uint64_t size) const {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

// === GlobalAllocator interface implementations ===

std::vector<GlobalAllocator::EvictionCandidate>
DfsGlobalAllocator::PrepareEviction() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return {};
    }

    PendingEviction pending(this);
    for (int i = 0; i < shard_count_; ++i) {
        auto& shard = *shards_[i];
        {
            std::unique_lock handle_lock(shard.handle_mutex);
            CleanupExpiredPendingFrees(shard, std::chrono::steady_clock::now());
        }
        PrepareEvictionFromShard(i, pending);
    }

    std::lock_guard lock(pending_mutex_);
    active_pending_ = std::move(pending);
    return active_pending_->candidates_;
}

void DfsGlobalAllocator::ResolveEviction(
    std::vector<GlobalAllocator::EvictionCandidate>& candidates,
    const std::vector<bool>& accepted) {
    std::lock_guard lock(pending_mutex_);
    if (!active_pending_.has_value()) {
        return;
    }

    ResolvePreparedEviction(std::move(*active_pending_), accepted);
    active_pending_.reset();
}

}  // namespace mooncake
