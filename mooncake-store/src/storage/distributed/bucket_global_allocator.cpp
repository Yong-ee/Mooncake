#include "storage/distributed/bucket_global_allocator.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "utils.h"
#ifdef USE_3FS
#include "storage/distributed/hf3fs_adapter.h"
#endif

namespace mooncake {

// === Helper functions ===

std::string BucketGlobalAllocator::FormatBucketId(int64_t bucket_id) {
    std::ostringstream oss;
    oss << std::setw(4) << std::setfill('0') << bucket_id;
    return oss.str();
}

uint64_t BucketGlobalAllocator::AlignSize(uint64_t size) const {
    return (size + alignment_ - 1) & ~(alignment_ - 1);
}

std::string BucketGlobalAllocator::GetBucketDataPath(
    int64_t bucket_id) const {
    return fsdir_ + "/bucket_" + FormatBucketId(bucket_id) + ".data";
}

std::string BucketGlobalAllocator::GetBucketMetaPath(
    int64_t bucket_id) const {
    return fsdir_ + "/bucket_" + FormatBucketId(bucket_id) + ".meta";
}

// === Lifecycle ===

BucketGlobalAllocator::~BucketGlobalAllocator() {
    {
        std::lock_guard<std::mutex> lock(cv_mutex_);
        running_.store(false, std::memory_order_release);
    }
    cv_.notify_all();
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
}

tl::expected<void, ErrorCode> BucketGlobalAllocator::Init(
    const DistributedStorageConfig& config) {
    if (initialized_.load(std::memory_order_acquire)) {
        return {};
    }
    if (!config.ValidateForAllocator()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    fsdir_ = config.fsdir;
    bucket_capacity_ = config.shard_capacity;
    alignment_ = config.alignment;
    eviction_enabled_ = config.eviction_enabled;
    eviction_high_watermark_ = config.eviction_high_watermark;
    eviction_low_watermark_ = config.eviction_low_watermark;
    eviction_check_interval_ = config.eviction_check_interval;
    initial_bucket_count_ = std::max(1, config.shard_count);

    std::error_code ec;
    std::filesystem::create_directories(fsdir_, ec);
    if (ec) {
        LOG(ERROR) << "BucketGlobalAllocator::Init failed to create dir "
                   << fsdir_ << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    RecoverFromDisk();

    if (buckets_.empty()) {
        for (int i = 0; i < initial_bucket_count_; ++i) {
            if (CreateNewBucket() < 0) {
                LOG(ERROR) << "BucketGlobalAllocator::Init failed to create "
                              "initial bucket";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        }
    }

    running_.store(true, std::memory_order_release);
    initialized_.store(true, std::memory_order_release);

    LOG(INFO) << "BucketGlobalAllocator initialized: fsdir=" << fsdir_
              << ", buckets=" << buckets_.size()
              << ", capacity=" << bucket_capacity_
              << ", eviction=" << eviction_enabled_;
    return {};
}

// === Allocation ===

tl::expected<DistributedFSDescriptor, ErrorCode>
BucketGlobalAllocator::Allocate(const std::string& key, uint64_t size) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (key.empty() || size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Each entry: [key_size(8B)][key][value], aligned to 4096
    uint64_t entry_size = sizeof(int64_t) + key.size() + size;
    uint64_t aligned_entry = AlignSize(entry_size);
    if (aligned_entry > bucket_capacity_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard lock(allocator_mutex_);

    // Check if key already allocated
    auto existing = key_to_bucket_.find(key);
    if (existing != key_to_bucket_.end()) {
        auto bucket_it = buckets_.find(existing->second);
        if (bucket_it != buckets_.end() &&
            !bucket_it->second->evicting.load()) {
            auto meta_it =
                bucket_it->second->key_metadata.find(key);
            if (meta_it != bucket_it->second->key_metadata.end()) {
                const auto& m = meta_it->second;
                // Return existing allocation (key_size field at offset,
                // data starts after)
                return DistributedFSDescriptor{
                    GetBucketDataPath(existing->second),
                    m.offset + static_cast<int64_t>(sizeof(int64_t)),
                    size,
                    aligned_entry,
                    static_cast<int>(existing->second)};
            }
        }
    }

    // Find active bucket
    auto bucket_it = buckets_.find(active_bucket_id_);
    if (bucket_it == buckets_.end()) {
        int64_t new_id = CreateNewBucket();
        if (new_id < 0) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        active_bucket_id_ = new_id;
        bucket_it = buckets_.find(new_id);
    }

    auto& bucket = bucket_it->second;
    uint64_t current_offset =
        bucket->append_offset.load(std::memory_order_relaxed);

    // If not enough space, create new bucket
    if (current_offset + aligned_entry > bucket_capacity_) {
        int64_t new_id = CreateNewBucket();
        if (new_id < 0) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        active_bucket_id_ = new_id;
        bucket_it = buckets_.find(new_id);
        bucket = bucket_it->second;
    }

    // Atomically claim space
    uint64_t claimed_offset = bucket->append_offset.fetch_add(
        aligned_entry, std::memory_order_acq_rel);

    // Handle overflow (race between check and fetch_add)
    if (claimed_offset + aligned_entry > bucket_capacity_) {
        int64_t new_id = CreateNewBucket();
        if (new_id < 0) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        active_bucket_id_ = new_id;
        auto new_bucket = buckets_[new_id];
        claimed_offset = new_bucket->append_offset.fetch_add(
            aligned_entry, std::memory_order_acq_rel);
        bucket = new_bucket;
        bucket_it = buckets_.find(new_id);
    }

    // Record key metadata
    {
        std::unique_lock bucket_lock(bucket->mutex);
        bucket->keys.push_back(key);
        bucket->key_metadata[key] = BucketObjectMetadata{
            static_cast<int64_t>(claimed_offset),
            static_cast<int64_t>(key.size()),
            static_cast<int64_t>(size)};
    }

    key_to_bucket_[key] = bucket->bucket_id;

    // Update LRU
    auto now =
        std::chrono::steady_clock::now().time_since_epoch().count();
    bucket->last_access_ns.store(now, std::memory_order_relaxed);
    auto lru_it = lru_index_.find(bucket->bucket_id);
    if (lru_it != lru_index_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_,
                         lru_it->second);
    } else {
        lru_list_.push_front(bucket->bucket_id);
        lru_index_[bucket->bucket_id] = lru_list_.begin();
    }

    // Data offset: after key_size field
    int64_t data_offset =
        static_cast<int64_t>(claimed_offset) +
        static_cast<int64_t>(sizeof(int64_t));

    return DistributedFSDescriptor{
        GetBucketDataPath(bucket->bucket_id),
        data_offset,
        size,
        aligned_entry,
        static_cast<int>(bucket->bucket_id)};
}

void BucketGlobalAllocator::Free(uint64_t offset, uint64_t aligned_size,
                                 int shard_idx, const std::string& key) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return;
    }

    std::lock_guard lock(allocator_mutex_);
    auto bucket_it = buckets_.find(shard_idx);
    if (bucket_it == buckets_.end()) {
        return;
    }

    auto& bucket = bucket_it->second;
    std::unique_lock bucket_lock(bucket->mutex);
    auto meta_it = bucket->key_metadata.find(key);
    if (meta_it != bucket->key_metadata.end()) {
        bucket->key_metadata.erase(meta_it);
    }
    auto keys_it =
        std::find(bucket->keys.begin(), bucket->keys.end(), key);
    if (keys_it != bucket->keys.end()) {
        bucket->keys.erase(keys_it);
    }
    bucket_lock.unlock();

    key_to_bucket_.erase(key);

    // Persist updated metadata
    PersistMetadata(shard_idx);
}

void BucketGlobalAllocator::UpdateAccess(const std::string& key,
                                         int shard_idx, uint64_t offset) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return;
    }

    std::lock_guard lock(allocator_mutex_);
    auto bucket_it = buckets_.find(shard_idx);
    if (bucket_it == buckets_.end()) {
        return;
    }

    auto& bucket = bucket_it->second;
    if (bucket->evicting.load()) {
        return;
    }

    auto now =
        std::chrono::steady_clock::now().time_since_epoch().count();
    bucket->last_access_ns.store(now, std::memory_order_relaxed);

    auto lru_it = lru_index_.find(shard_idx);
    if (lru_it != lru_index_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_,
                         lru_it->second);
    }
}

// === Persistence ===

void BucketGlobalAllocator::PersistMetadata(int64_t bucket_id) {
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) {
        return;
    }

    auto& bucket = bucket_it->second;
    std::shared_lock bucket_lock(bucket->mutex);

    BucketMetadata meta;
    meta.data_size = 0;
    meta.keys = bucket->keys;
    meta.metadatas.reserve(bucket->keys.size());
    for (const auto& key : bucket->keys) {
        auto it = bucket->key_metadata.find(key);
        if (it != bucket->key_metadata.end()) {
            meta.data_size += it->second.key_size + it->second.data_size;
            meta.metadatas.push_back(it->second);
        }
    }

    std::string meta_path = GetBucketMetaPath(bucket_id);
    std::ofstream ofs(meta_path, std::ios::binary);
    if (!ofs.is_open()) {
        LOG(ERROR) << "Failed to open meta file for write: " << meta_path;
        return;
    }

    std::string json = ylt::reflection::to_json(meta);
    ofs.write(json.data(), json.size());
    ofs.close();

    VLOG(1) << "Persisted metadata for bucket " << bucket_id
            << ", keys=" << bucket->keys.size();
}

void BucketGlobalAllocator::RecoverFromDisk() {
    std::error_code ec;

    for (auto& entry : std::filesystem::directory_iterator(fsdir_, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        auto& path = entry.path();
        if (path.extension() != ".meta") {
            continue;
        }

        std::string stem = path.stem().string();
        if (stem.size() < 8 || stem.substr(0, 7) != "bucket_") {
            continue;
        }
        int64_t bucket_id = 0;
        try {
            bucket_id = std::stoll(stem.substr(7));
        } catch (...) {
            LOG(WARNING) << "Failed to parse bucket ID from: " << stem;
            continue;
        }

        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.is_open()) {
            LOG(WARNING) << "Failed to open meta file: " << path;
            continue;
        }
        std::string json((std::istreambuf_iterator<char>(ifs)),
                         std::istreambuf_iterator<char>());
        ifs.close();

        BucketMetadata meta;
        try {
            ylt::reflection::from_json(json, meta);
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to deserialize meta for bucket "
                         << bucket_id << ": " << e.what();
            continue;
        }

        auto bucket = std::make_shared<BucketState>(
            bucket_id, bucket_capacity_);
        bucket->keys = std::move(meta.keys);
        bucket->key_metadata.reserve(meta.metadatas.size());
        for (size_t i = 0; i < meta.metadatas.size() &&
                            i < bucket->keys.size(); ++i) {
            bucket->key_metadata[bucket->keys[i]] = meta.metadatas[i];
        }

        // Calculate append_offset
        uint64_t max_end = 0;
        for (const auto& m : meta.metadatas) {
            uint64_t end = static_cast<uint64_t>(
                m.offset + sizeof(int64_t) + m.key_size + m.data_size);
            if (end > max_end) {
                max_end = end;
            }
        }
        bucket->append_offset.store(AlignSize(max_end),
                                    std::memory_order_relaxed);

        buckets_[bucket_id] = bucket;

        for (const auto& key : bucket->keys) {
            key_to_bucket_[key] = bucket_id;
        }

        auto lru_it = lru_list_.insert(lru_list_.begin(), bucket_id);
        lru_index_[bucket_id] = lru_it;

        if (bucket_id >= next_bucket_id_) {
            next_bucket_id_ = bucket_id + 1;
        }

        LOG(INFO) << "Recovered bucket " << bucket_id
                  << ", keys=" << bucket->keys.size();
    }

    if (!buckets_.empty()) {
        active_bucket_id_ = next_bucket_id_ - 1;
    }

    // Cleanup orphan .data files
    for (auto& entry : std::filesystem::directory_iterator(fsdir_, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        auto& path = entry.path();
        if (path.extension() != ".data") {
            continue;
        }
        std::string stem = path.stem().string();
        if (stem.size() < 8 || stem.substr(0, 7) != "bucket_") {
            continue;
        }
        std::string meta_path = path.string();
        meta_path.replace(meta_path.rfind(".data"), 5, ".meta");
        if (!std::filesystem::exists(meta_path)) {
            LOG(WARNING) << "Removing orphan data file: " << path;
            std::filesystem::remove(path);
        }
    }
}

// === Bucket management ===

int64_t BucketGlobalAllocator::CreateNewBucket() {
    int64_t new_id = next_bucket_id_++;
    auto bucket =
        std::make_shared<BucketState>(new_id, bucket_capacity_);

    std::string data_path = GetBucketDataPath(new_id);
    std::ofstream ofs(data_path, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        LOG(ERROR) << "Failed to create bucket data file: " << data_path;
        return -1;
    }
    ofs.seekp(static_cast<std::streamoff>(bucket_capacity_ - 1));
    ofs.write("\0", 1);
    ofs.close();

    buckets_[new_id] = bucket;

    lru_list_.push_front(new_id);
    lru_index_[new_id] = lru_list_.begin();

    PersistMetadata(new_id);

    LOG(INFO) << "Created new bucket " << new_id;
    return new_id;
}

int64_t BucketGlobalAllocator::SelectBucketForEviction() const {
    if (lru_list_.empty()) {
        return -1;
    }
    return lru_list_.back();
}

// === Eviction ===

std::vector<GlobalAllocator::EvictionCandidate>
BucketGlobalAllocator::PrepareEviction() {
    std::vector<EvictionCandidate> candidates;
    if (!initialized_.load(std::memory_order_acquire)) {
        return candidates;
    }

    std::lock_guard lock(allocator_mutex_);

    uint64_t total_used = 0;
    uint64_t total_capacity = 0;
    for (const auto& [id, bucket] : buckets_) {
        total_used +=
            bucket->append_offset.load(std::memory_order_relaxed);
        total_capacity += bucket->capacity;
    }
    if (total_capacity == 0) {
        return candidates;
    }

    double usage = static_cast<double>(total_used) /
                   static_cast<double>(total_capacity);
    if (usage < eviction_high_watermark_) {
        return candidates;
    }

    // Select buckets from LRU tail
    while (usage >= eviction_high_watermark_) {
        int64_t victim_id = SelectBucketForEviction();
        if (victim_id < 0) {
            break;
        }
        auto bucket_it = buckets_.find(victim_id);
        if (bucket_it == buckets_.end()) {
            break;
        }
        auto& bucket = bucket_it->second;
        if (bucket->evicting.load()) {
            break;
        }

        // Collect keys for eviction
        std::shared_lock bucket_lock(bucket->mutex);
        for (const auto& key : bucket->keys) {
            auto meta_it = bucket->key_metadata.find(key);
            if (meta_it != bucket->key_metadata.end()) {
                candidates.push_back(
                    {key, static_cast<int>(victim_id),
                     static_cast<uint64_t>(meta_it->second.offset)});
            }
        }
        bucket->evicting.store(true, std::memory_order_release);

        // Remove from LRU
        auto lru_it = lru_index_.find(victim_id);
        if (lru_it != lru_index_.end()) {
            lru_list_.erase(lru_it->second);
            lru_index_.erase(lru_it);
        }

        // Recalculate usage
        total_used = 0;
        total_capacity = 0;
        for (const auto& [id, b] : buckets_) {
            if (!b->evicting.load()) {
                total_used +=
                    b->append_offset.load(std::memory_order_relaxed);
            }
            total_capacity += b->capacity;
        }
        if (total_capacity == 0) {
            break;
        }
        usage = static_cast<double>(total_used) /
                static_cast<double>(total_capacity);
    }

    return candidates;
}

void BucketGlobalAllocator::ResolveEviction(
    std::vector<GlobalAllocator::EvictionCandidate>& candidates,
    const std::vector<bool>& accepted) {
    std::lock_guard lock(allocator_mutex_);

    std::set<int64_t> buckets_to_delete;

    for (size_t i = 0; i < candidates.size(); ++i) {
        if (!accepted[i]) {
            continue;
        }
        const auto& candidate = candidates[i];
        int64_t bucket_id = candidate.shard_idx;

        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end()) {
            continue;
        }

        auto& bucket = bucket_it->second;
        std::unique_lock bucket_lock(bucket->mutex);

        // Remove key metadata
        bucket->key_metadata.erase(candidate.key);
        auto keys_it = std::find(bucket->keys.begin(),
                                 bucket->keys.end(), candidate.key);
        if (keys_it != bucket->keys.end()) {
            bucket->keys.erase(keys_it);
        }

        key_to_bucket_.erase(candidate.key);

        if (bucket->keys.empty()) {
            buckets_to_delete.insert(bucket_id);
        }
    }

    // Delete empty buckets
    for (int64_t bucket_id : buckets_to_delete) {
        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end()) {
            continue;
        }

        auto& bucket = bucket_it->second;

        // Wait for inflight reads
        while (bucket->inflight_reads.load(
                   std::memory_order_acquire) > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1));
        }

        // Delete files
        std::error_code ec;
        std::filesystem::remove(GetBucketDataPath(bucket_id), ec);
        std::filesystem::remove(GetBucketMetaPath(bucket_id), ec);

        // Reset bucket
        bucket->append_offset.store(0, std::memory_order_relaxed);
        bucket->keys.clear();
        bucket->key_metadata.clear();
        bucket->evicting.store(false, std::memory_order_release);

        // Add back to LRU as available bucket
        lru_list_.push_front(bucket_id);
        lru_index_[bucket_id] = lru_list_.begin();
    }
}

}  // namespace mooncake
