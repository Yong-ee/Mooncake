# DFS 副本 GlobalAllocator 抽象与 Bucket 实现方案

## 0. 方案概述

本方案对 Mooncake Store 的 DFS 副本存储进行重构：

1. **抽象 `GlobalAllocator` 接口层**：将现有 `DfsGlobalAllocator`（基于固定分片 + OffsetAllocator）的公共接口提取为抽象基类
2. **新增 `BucketGlobalAllocator` 实现**：基于动态 bucket 文件 + atomic offset 追加的分配方式，作为 `GlobalAllocator` 的第二种实现
3. **元数据持久化**：通过 `.meta` 文件持久化 bucket 状态，支持重启恢复
4. **LRU 淘汰**：以 bucket 为粒度进行淘汰（与 Shard 方式的 key 级淘汰并存）

**核心思路**：DFS 副本有两种文件组织方式可选：
- **Shard 模式**（现有）：固定 N 个分片文件，OffsetAllocator 管理空间，key 级 LRU 淘汰
- **Bucket 模式**（新增）：动态 M 个 bucket 文件（预分配 256MB），atomic offset 追加分配，bucket 级 LRU 淘汰，元数据持久化到 `.meta` 文件

两种模式通过 `GlobalAllocator` 接口统一，Master 侧通过配置选择使用哪种模式。

## 1. 背景与动机

当前 DFS 副本（`ReplicaType::DFS`）使用 Shard 模式：
- 固定数量的分片文件（`dfs_shard_XX.data`）
- `OffsetAllocator` 管理每个分片内的空间分配（支持碎片整理）
- LRU 淘汰粒度为 key 级别（细粒度，复杂）
- 元数据仅在 Master 内存中，重启后丢失

**Shard 模式的问题**：
| 问题 | 说明 |
|------|------|
| 无重启恢复 | 元数据仅在内存中，Master 重启后所有 DFS 副本失效 |
| 分片文件覆盖写 | 对于分布式文件系统，多次写入同一分片的不同偏移可能触发覆盖写 |
| 空间利用率 | OffsetAllocator 的碎片整理开销 |

**Bucket 模式的优势**：
| 优势 | 说明 |
|------|------|
| 元数据持久化 | `.meta` 文件记录 bucket 内的 key 布局，支持重启恢复 |
| 追加写 | 每个 bucket 内顺序追加，对分布式文件系统友好 |
| 实现简单 | atomic offset 追加，无需碎片整理 |

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Master Service                              │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              GlobalAllocator (抽象接口)                    │   │
│  │                                                            │   │
│  │  Allocate(key, size) → Descriptor                         │   │
│  │  Free(key, descriptor)                                    │   │
│  │  UpdateAccess(key, descriptor)                            │   │
│  │  PrepareEviction() → PendingEviction                      │   │
│  │  CommitEviction(pending)                                  │   │
│  │  IsInitialized()                                          │   │
│  └──────────────────┬───────────────────────────────────────┘   │
│                     │                                            │
│          ┌──────────┴──────────┐                                 │
│          │                     │                                  │
│          ▼                     ▼                                  │
│  ┌───────────────┐    ┌───────────────────┐                     │
│  │ DfsGlobal     │    │ BucketGlobal      │                     │
│  │ Allocator     │    │ Allocator          │                     │
│  │ (Shard 模式)  │    │ (Bucket 模式)     │                     │
│  └───────┬───────┘    └────────┬──────────┘                     │
│          │                      │                                 │
│          ▼                      ▼                                 │
│  ┌───────────────┐    ┌───────────────────┐                     │
│  │ dfs_shard_    │    │ bucket_NNNN.data  │                     │
│  │ 00.data       │    │ bucket_NNNN.meta  │                     │
│  │ dfs_shard_    │    │ bucket_NNNN+1.data│                     │
│  │ 01.data       │    │ bucket_NNNN+1.meta│                     │
│  │ ...           │    │ ...               │                     │
│  └───────────────┘    └───────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```

## 3. GlobalAllocator 接口设计

```cpp
class GlobalAllocatorInterface {
public:
    virtual ~GlobalAllocatorInterface() = default;

    // 初始化
    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual bool IsInitialized() const = 0;

    // 空间分配
    virtual tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) = 0;

    // 空间释放
    virtual void Free(const std::string& key,
                      const DistributedFSDescriptor& descriptor) = 0;

    // 访问更新（LRU）
    virtual void UpdateAccess(const std::string& key,
                              const DistributedFSDescriptor& descriptor) = 0;

    // 淘汰接口
    virtual bool IsEvictionEnabled() const = 0;
    virtual std::chrono::seconds GetEvictionCheckInterval() const = 0;

    struct EvictionCandidate {
        std::string key;
        DistributedFSDescriptor descriptor;
    };

    struct PendingEviction {
        std::vector<EvictionCandidate> candidates;
        // ... pending state
    };

    virtual PendingEviction PrepareEviction() = 0;
    virtual void CommitEviction(PendingEviction&& pending) = 0;
    virtual void AbortEviction(PendingEviction&& pending) = 0;

    // 配置
    virtual uint64_t GetTotalCapacity() const = 0;
    virtual uint64_t GetUsedBytes() const = 0;
};
```

### 3.1 DistributedFSDescriptor 扩展

现有 `DistributedFSDescriptor` 已包含 `file_path`、`offset`、`object_size`、`aligned_size`、`shard_idx`。Bucket 模式复用相同结构：

```cpp
struct DistributedFSDescriptor {
    std::string file_path;     // 文件路径（Shard: dfs_shard_XX.data, Bucket: bucket_NNNN.data）
    uint64_t offset = 0;       // 文件内偏移
    uint64_t object_size = 0;  // 原始数据大小
    uint64_t aligned_size = 0; // 对齐后大小
    int shard_idx = 0;         // Shard 模式: 分片索引; Bucket 模式: bucket_id
    YLT_REFL(DistributedFSDescriptor, file_path, offset, object_size,
             aligned_size, shard_idx);
};
```

**约定**：
- Shard 模式：`shard_idx` 表示分片编号，`file_path` 为 `dfs_shard_XX.data`
- Bucket 模式：`shard_idx` 复用为 bucket_id，`file_path` 为 `bucket_NNNN.data`

## 4. DfsGlobalAllocator（Shard 模式）重构

现有 `DfsGlobalAllocator` 保持功能不变，改为继承 `GlobalAllocatorInterface`：

```cpp
class DfsGlobalAllocator : public GlobalAllocatorInterface {
public:
    tl::expected<void, ErrorCode> Init() override;
    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;
    void Free(const std::string& key,
              const DistributedFSDescriptor& descriptor) override;
    void UpdateAccess(const std::string& key,
                      const DistributedFSDescriptor& descriptor) override;
    // ... 其他接口实现

private:
    // 现有实现保持不变
    std::string mount_path_;
    int shard_count_ = 0;
    uint64_t alignment_ = 4096;
    std::vector<std::unique_ptr<ShardState>> shards_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    // ...
};
```

## 5. BucketGlobalAllocator（Bucket 模式）设计

```cpp
class BucketGlobalAllocator : public GlobalAllocatorInterface {
public:
    BucketGlobalAllocator() = default;
    ~BucketGlobalAllocator() override;

    tl::expected<void, ErrorCode> Init() override;
    bool IsInitialized() const override;

    tl::expected<DistributedFSDescriptor, ErrorCode> Allocate(
        const std::string& key, uint64_t size) override;
    void Free(const std::string& key,
              const DistributedFSDescriptor& descriptor) override;
    void UpdateAccess(const std::string& key,
                      const DistributedFSDescriptor& descriptor) override;

    bool IsEvictionEnabled() const override { return eviction_enabled_; }
    std::chrono::seconds GetEvictionCheckInterval() const override;

    PendingEviction PrepareEviction() override;
    void CommitEviction(PendingEviction&& pending) override;
    void AbortEviction(PendingEviction&& pending) override;

    uint64_t GetTotalCapacity() const override;
    uint64_t GetUsedBytes() const override;

    // 元数据持久化
    void PersistMetadata(int64_t bucket_id);
    void RecoverFromDisk();

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
    };

    std::string fsdir_;
    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    uint64_t bucket_capacity_ = 256 * 1024 * 1024;  // 256MB
    uint64_t alignment_ = 4096;

    std::unordered_map<int64_t, std::shared_ptr<BucketState>> buckets_;
    std::unordered_map<std::string, int64_t> key_to_bucket_;
    int64_t next_bucket_id_ = 0;
    int64_t active_bucket_id_ = 0;

    mutable std::mutex allocator_mutex_;

    // LRU
    std::list<int64_t> lru_list_;
    std::unordered_map<int64_t, std::list<int64_t>::iterator> lru_index_;

    // 淘汰配置
    bool eviction_enabled_ = true;
    double eviction_high_watermark_ = 0.9;
    double eviction_low_watermark_ = 0.7;
    std::thread eviction_thread_;
    std::atomic<bool> initialized_{false};
};
```

### 5.1 Bucket 文件结构

```
{fsdir}/
├── bucket_0000.data    # 数据文件（预分配 256MB）
├── bucket_0000.meta    # 元数据文件（YLT 序列化）
├── bucket_0001.data
├── bucket_0001.meta
└── ...
```

### 5.2 Data 文件内部布局

与现有 `BucketStorageBackend` 一致：

```
bucket_0000.data:
┌────────────────────────────────────────────────────────────┐
│ [key_size(8B)][key][value] ← 4096 对齐                    │
│ [key_size(8B)][key][value] ← 4096 对齐                    │
│ ...                                                        │
└────────────────────────────────────────────────────────────┘
```

### 5.3 Meta 文件内容

复用 `BucketMetadata` 序列化格式（`YLT_REFL`）：

```cpp
struct BucketMetadata {
    int64_t meta_size;
    int64_t data_size;
    std::vector<std::string> keys;
    std::vector<BucketObjectMetadata> metadatas;
    // 每个 BucketObjectMetadata: { offset, key_size, data_size }
};
YLT_REFL(BucketMetadata, data_size, keys, metadatas);
```

### 5.4 并发控制

- **分配**：`allocator_mutex_` 保护 bucket 状态变更（创建/分配）
- **写入**：多客户端可并发写同一 bucket 的不同 offset（atomic offset 保证不重叠）
- **淘汰**：`evicting` 标记 + `inflight_reads` 计数器（复用 `BucketReadGuard` 模式）

## 6. Master 集成

### 6.1 配置选择

```cpp
// master_service.h
enum class DfsAllocatorType {
    SHARD,    // 现有 DfsGlobalAllocator
    BUCKET,   // 新增 BucketGlobalAllocator
};

// master_service.cpp
if (config.dfs_allocator_type == DfsAllocatorType::BUCKET) {
    dfs_allocator_ = std::make_unique<BucketGlobalAllocator>();
} else {
    dfs_allocator_ = std::make_unique<DfsGlobalAllocator>();
}
dfs_allocator_->Init(config);
```

### 6.2 Put 流程（不变）

```
1. Client → Master: PutStart(key, size)
2. Master: dfs_allocator_->Allocate(key, size) → DistributedFSDescriptor
3. Master → Client: 返回 descriptor (file_path, offset, shard_idx)
4. Client: pwritev(fd, slices, offset)  // 异步写入
5. Client → Master: PutEnd(key, DFS)
```

### 6.3 Get 流程（不变）

```
1. Client → Master: Query(key)
2. Master: 返回 DistributedFSDescriptor
3. Client: preadv(fd, offset, size)  // 读取数据
```

## 7. 重启恢复（仅 Bucket 模式）

```
Master 启动
  │
  ▼
扫描 {fsdir}/bucket_*.meta 文件
  │
  ▼
加载每个 bucket 的 BucketMetadata
  │  - keys 列表
  │  - metadatas (offset, key_size, data_size)
  │  - 计算 append_offset
  │
  ▼
重建内存状态
  │  - buckets_ map
  │  - key_to_bucket_ 映射
  │  - LRU 链表
  │
  ▼
清理孤立文件（.data 无对应 .meta）
```

## 8. 文件变更清单

### 8.1 新增文件

| 文件 | 内容 |
|------|------|
| `mooncake-store/include/storage/distributed/global_allocator_interface.h` | 抽象接口 |
| `mooncake-store/include/storage/distributed/bucket_global_allocator.h` | Bucket 实现头文件 |
| `mooncake-store/src/storage/distributed/bucket_global_allocator.cpp` | Bucket 实现 |

### 8.2 修改文件

| 文件 | 修改内容 |
|------|----------|
| `mooncake-store/include/storage/distributed/dfs_global_allocator.h` | 继承 GlobalAllocatorInterface |
| `mooncake-store/src/storage/distributed/dfs_global_allocator.cpp` | 适配接口 |
| `mooncake-store/include/master_service.h` | `dfs_allocator_` 类型改为 `unique_ptr<GlobalAllocatorInterface>` |
| `mooncake-store/src/master_service.cpp` | 根据配置选择 allocator 类型 |
| `mooncake-store/include/storage/distributed/distributed_storage_backend.h` | 新增 Bucket 模式的 BatchWrite/BatchRead |

## 9. 复用关系

| 组件 | 复用方式 |
|------|----------|
| `BucketMetadata` | 直接复用，描述 bucket 内的 key-value 布局 |
| `BucketReadGuard` | 直接复用，管理 in-flight 读计数 |
| `DistributedFSDescriptor` | 扩展使用，`shard_idx` 复用为 bucket_id |
| `FileSystemAdapter` | 直接复用，提供 pwritev/preadv 抽象 |
| `DfsGlobalAllocator` 的 LRU 模式 | 参考实现 Bucket 模式的淘汰逻辑 |

## 10. Shard 模式 vs Bucket 模式对比

```
┌──────────────────┬────────────────────────┬────────────────────────┐
│      维度        │    Shard 模式          │   Bucket 模式          │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 文件布局         │ 固定 N 个分片          │ 动态 M 个 bucket       │
│                  │ dfs_shard_XX.data      │ bucket_NNNN.data       │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 空间分配         │ OffsetAllocator        │ atomic offset 追加     │
│                  │ (支持碎片整理)         │ (仅追加, 无碎片整理)   │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 淘汰粒度         │ key 级别               │ bucket 级别            │
│                  │ (细粒度, 复杂)         │ (粗粒度, 简单)         │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 元数据持久化     │ ❌ 仅内存              │ ✅ .meta 文件          │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 重启恢复         │ ❌ 不支持              │ ✅ 支持                │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 写入模式         │ 随机写（不同 offset）  │ 追加写（顺序分配）     │
├──────────────────┼────────────────────────┼────────────────────────┤
│ 实现复杂度       │ 高（碎片整理）         │ 低（atomic append）    │
└──────────────────┴────────────────────────┴────────────────────────┘
```

## 11. 验证方案

### 11.1 单元测试

- `BucketGlobalAllocator::Allocate` 正确分配 bucket_id + offset
- `BucketGlobalAllocator::Free` 正确释放空间
- LRU 淘汰正确触发和回收
- 元数据持久化和恢复正确性
- `GlobalAllocatorInterface` 多态调用正确性

### 11.2 集成测试

使用 `scripts/run_ci_test.sh` 运行完整 CI

### 11.3 功能验证

1. Put → Get 读写一致性（两种模式）
2. Bucket 满后自动切换新 bucket
3. LRU 淘汰正确释放空间
4. 重启后元数据恢复（仅 Bucket 模式）
5. Shard 模式与 Bucket 模式可配置切换
