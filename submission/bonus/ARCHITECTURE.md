# Architecture Brief — LLM Observability Lakehouse at 1 Billion Requests/Day

**Topic A** · Tác giả: Nguyễn Thu Hằng · MSSV: 2A202602463

---

## 1. Problem Statement

Một foundation-model API team cần log **toàn bộ** request/response. Scale:

- **1 tỉ request/ngày** = ~11,600 req/giây average; peak 3× = ~35,000 req/giây
- **~5 KB/request** (prompt + response + metadata) → **5 TB raw/ngày**
- **Retention tiered:** prompt/response đầy đủ 7 ngày → chỉ aggregates 1 năm
- **Dashboard SLA:** cost & latency theo tenant, refresh mỗi 5 phút
- **PII:** số điện thoại, email trong prompt phải redact trước khi AI/analyst đọc
- **Budget:** storage ≤ **$5,000/tháng**

Tại sao khó: throughput đủ để phá bất kỳ OLTP nào; 5 KB × 11,600 → column store phải handle row-group tiếp nhận real-time; PII và retention yêu cầu kiểm soát truy cập theo tier.

---

## 2. Architecture Diagram

```
                    INGESTION PATH
                    ─────────────
 API Gateway ──► Kafka (3 partitions × tenant_id)
                       │
          ┌────────────▼────────────┐
          │   Bronze Landing Zone    │  5 TB/ngày raw
          │   Delta Table            │  partition: date, tenant_id
          │   Retention: 7 ngày      │  PII TOKENIZED tại đây ★
          │   Format: Parquet + LZ4  │
          └────────────┬────────────┘
                       │  Spark Structured Streaming (5-min trigger)
          ┌────────────▼────────────┐
          │   Silver — Cleansed     │  ~4.8 TB/ngày (sau dedup ~3%)
          │   Delta Table            │  partition: date, tenant_id, model
          │   Retention: 30 ngày     │  Z-ORDER by tenant_id ★ (hot path)
          │   Schema enforcement ★   │
          └────────────┬────────────┘
                       │  Micro-batch aggregation (5-min)
          ┌────────────▼────────────┐
          │   Gold — Metrics        │  ~50 GB/ngày aggregates
          │   Delta Table            │  partition: date, model
          │   Retention: 1 năm       │  p50/p95 latency, cost_usd, error_rate
          └─────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   Dashboard (Superset)  │  Query Gold via DuckDB/Trino
          │   Refresh 5 phút        │  SLA: < 10s per query
          └─────────────────────────┘

★ = Day 18 concepts applied
```

**4 Day-18 concepts áp dụng:**
1. **Bronze→Silver→Gold medallion**: tách raw/cleansed/aggregated, mỗi tier có retention riêng
2. **Z-ORDER by tenant_id** tại Silver: file-skipping cho dashboard filter-by-tenant
3. **Schema enforcement** tại Bronze landing: block malformed JSON ngay lúc ingest
4. **Time travel + RESTORE**: khi phát hiện lỗi tokenization, rollback Bronze về version trước incident

---

## 3. Quyết Định Chính & Alternatives Đã Loại

### Decision 1: Table Format — Delta Lake

**Chọn:** Delta Lake (delta-rs 1.x)

| Alternative | Lý do loại |
|---|---|
| Apache Iceberg | REST catalog thêm latency; đội nhỏ chưa có catalog server; Delta phổ biến hơn trong Spark ecosystem |
| Apache Hudi | Copy-on-write overhead lớn hơn Delta khi append-heavy; community nhỏ hơn |
| Raw Parquet | Không có ACID, không time travel, không schema enforcement — không thể rollback khi PII leak |

**Reasoning:** 1B req/ngày là append-dominant (>99% writes). Delta's log-structured append + transaction log tốt hơn cho workload này. Time travel cần thiết cho compliance audit.

### Decision 2: PII Tokenization Strategy — Tokenize tại Bronze, không tại Silver

**Chọn:** Tokenize ngay tại Kafka consumer trước khi ghi Bronze

| Alternative | Lý do loại |
|---|---|
| Tokenize tại Silver | Raw PII tồn tại trong Bronze → vi phạm data minimization; nếu Bronze bị leak thì GDPR/Nghị định 13 vi phạm |
| Tokenize tại query time | Quá chậm ở scale 5 TB/ngày; vault calls trở thành bottleneck |
| Không tokenize, dùng column-level encryption | Complexity cao; DuckDB/Trino không hỗ trợ native; key rotation phức tạp |

**Reasoning:** Defense-in-depth — Bronze đã an toàn ngay từ đầu. Token vault lưu mapping, chỉ team PII có thể đổi lại.

### Decision 3: Partitioning Strategy — date + tenant_id tại Silver

**Chọn:** Partition by `(date, tenant_id)` tại Silver; Z-ORDER by `tenant_id`

| Alternative | Lý do loại |
|---|---|
| Partition by hour | 24× nhiều partition files/ngày; Hive metastore LIST operation tốn kém |
| Partition by model only | Dashboard filter-by-tenant phải full-scan mọi partition của ngày |
| Không partition | Query Gold cần scan toàn bộ 4.8 TB Silver/ngày — không khả thi |

**Reasoning:** Tenant là chiều filter hot nhất (multi-tenant API). Z-ORDER trong partition → file-skipping 10× cho point-tenant query (đo trong NB2 lab: pruning ratio ≥ 10×).

### Decision 4: Streaming Trigger — 5-minute micro-batch

**Chọn:** Spark Structured Streaming với `trigger(processingTime="5 minutes")`

| Alternative | Lý do loại |
|---|---|
| Continuous streaming (< 1s) | Small-file problem nghiêm trọng — 5 TB/ngày ÷ 86,400 = ~58 MB/giây → hàng nghìn files nhỏ/ngày |
| Hourly batch | Dashboard refresh 5-phút không đạt được |
| Event-time watermark 30s | Overhead checkpointing Kafka offset mỗi 30s; Kafka lag không giảm đủ |

**Reasoning:** 5-minute batch cân bằng freshness vs file-size (5 min × 58 MB/s ≈ 17 GB/batch → file size hợp lý sau compaction).

### Decision 5: Retention Enforcement — Delta VACUUM + TTL policy

**Chọn:** VACUUM tự động qua cron, retain 7 ngày Bronze, 30 ngày Silver, 365 ngày Gold

| Alternative | Lý do loại |
|---|---|
| Manual delete | Không scalable; human error xóa nhầm version đang được query |
| S3 Lifecycle only | S3 Lifecycle không biết Delta log structure → orphan files; Delta time travel bị broken |
| Iceberg snapshot expiry | Cần switch format; migration cost từ Delta → Iceberg cao |

**Reasoning:** Delta VACUUM kết hợp với S3 Intelligent-Tiering cho aging data. Cron chạy 02:00 UTC daily sau khi confirm dashboard queries đã xong.

### Decision 6 (bonus): Catalog — Unity Catalog (Databricks) vs Self-hosted Polaris

**Chọn:** Unity Catalog trong giai đoạn đầu; roadmap migrate sang Polaris 12 tháng sau

| Alternative | Lý do loại |
|---|---|
| Apache Polaris ngay | Còn mới (2024); thiếu enterprise support; team cần ramp-up |
| Hive Metastore | Không hỗ trợ column-level access control cần thiết cho PII |
| Không catalog | Không có fine-grained permission; tất cả analyst đọc được Bronze raw |

---

## 4. Failure Modes

### Failure 1: PII Tokenization Service Down (3AM)

**Scenario:** Token vault (HashiCorp Vault) timeout → Kafka consumer panic → Bronze write fails

**Detection:**
```
Alert: kafka_consumer_lag{topic="llm-raw"} > 1,000,000  # 5 phút lag
Alert: error_rate{service="tokenizer"} > 0.01%
```

**Rollback:**
1. Consumer switch sang "passthrough mode" — ghi raw records vào quarantine topic `llm-raw-pii-quarantine`
2. Sau khi vault recover: replay từ quarantine, tokenize lại
3. Delta **time travel**: Bronze version trước incident không bao giờ bị overwrite → audit clean

**Day-18 tie:** Time travel cho phép kiểm tra Bronze version trước/sau incident mà không cần backup riêng.

### Failure 2: Small-File Explosion (3AM)

**Scenario:** Compaction job fail → 200,000+ files trong Silver partition ngày hôm qua → dashboard query timeout

**Detection:**
```
Alert: delta_file_count{table="silver", date="today"} > 50000
Alert: query_duration_p95{dashboard="tenant_cost"} > 60s
```

**Rollback:**
1. Chạy emergency compact: `dt.optimize.compact(target_size=256*1024*1024)`
2. Không cần rollback — compaction là idempotent, không mất data
3. Post-mortem: tại sao cron job fail (OOM? permission?)

### Failure 3: Schema Evolution Breaking Dashboard (3AM)

**Scenario:** API team thêm field `reasoning_tokens` vào response → Silver schema enforcement block → Kafka consumer crash

**Detection:**
```
Alert: kafka_consumer_lag > 500000
Log: "DeltaWriterError: Schema mismatch: field 'reasoning_tokens' not in table schema"
```

**Rollback:**
1. `dt.alter.add_columns([Field("reasoning_tokens", pa.int64())])` — schema evolution opt-in
2. Không cần RESTORE vì consumer đã stop ghi, không có corrupt data
3. **Day-18 tie:** Schema enforcement (NB1) ngăn corrupt data vào table; schema evolution cho phép thêm cột an toàn

### Failure 4 (bonus): Orphan Files Từ Failed Compaction

**Scenario:** Compaction job killed midway → temporary files không có trong Delta log → storage leak

**Detection:**
```python
# Weekly audit
log_files = set(dt.file_uris())
disk_files = set(glob("silver/**/*.parquet"))
orphans = disk_files - log_files
# Alert if orphans > 100 GB
```

**Rollback:** Delete orphans manually (như NB6 lab). VACUUM không catch orphans chưa từng commit.

---

## 5. Ước Lượng Chi Phí (Back-of-envelope)

### Storage Cost (AWS S3 Standard & S3 Standard-IA)

*Lưu ý đơn vị chuẩn AWS: S3 Standard là **$0.023/GB-tháng** = **$23/TB-tháng**; S3-IA là **$0.0125/GB-tháng** = **$12.50/TB-tháng**.*

| Tier | Volume | Duration | Compression | Size lưu trữ | Đơn giá/TB-tháng | Chi phí/tháng |
|---|---|---|---|---|---|---|
| Bronze (Hot) | 5 TB/ngày | 7 ngày | LZ4 1.5× | 23.3 TB | $23.00 (S3 Standard) | **$536** |
| Silver (Hot) | 4.8 TB/ngày | 30 ngày | Snappy 2× | 72.0 TB | $23.00 (S3 Standard) | **$1,656** |
| Silver (Aged) | 30–90 ngày | 60 ngày | Zstd 3× | 96.0 TB | $12.50 (S3-IA) | **$1,200** |
| Gold | 50 GB/ngày | 365 ngày | Zstd 4× | 4.6 TB | $23.00 (S3 Standard) | **$106** |
| **Tổng Storage** | | | | **~196 TB** | | **~$3,498/tháng** |

### Compute Cost (Spot Instances & Stream Processing)

Ngân sách compute còn lại: `$5,000 - $3,500 = $1,500/tháng`. Để không vượt ngân sách $5K, pipeline sử dụng Flink và Spark trên Kubernetes Spot Instances:

| Job | Cơ chế & Tần suất | Cấu hình & Instance | Đơn giá Spot | Chi phí/tháng |
|---|---|---|---|---|
| Bronze Ingestion | Apache Flink streaming (24/7) | 2× c5.2xlarge spot (K8s) | ~$0.17/giờ × 2 | **$245** |
| Silver Micro-batch | Spark Streaming (trigger 5-phút, 2 min runtime) | 2× r5.xlarge spot | ~$0.08/giờ × 2 × 40% duty | **$46** |
| Gold Aggregation | Spark micro-batch (5-phút, 1 min runtime) | 2× r5.xlarge spot | ~$0.08/giờ × 2 × 20% duty | **$23** |
| Compaction & VACUUM | Cron daily (1 giờ/ngày) | 4× r5.2xlarge spot | ~$0.17/giờ × 4 × 1h/ngày | **$20** |
| **Tổng Compute** | | | | **~$334/tháng** |

### S3 API & Data Transfer Costs

- **PUT/POST requests:** Flink flush mỗi 5 phút (~8,640 PUTs/ngày) + Silver/Gold writes ≈ 100K PUTs/ngày → ~$15/tháng ($0.005/1K PUT).
- **LIST/GET operations:** Compacted partitions (~800K files) + Dashboard queries → ~$120/tháng.
- **Tổng API/Transfer:** **~$135/tháng**.

### Tổng Hợp Ngân Sách Hàng Tháng

$$\text{Tổng chi phí} = \text{Storage } (\$3,498) + \text{Compute } (\$334) + \text{S3 API } (\$135) = \mathbf{\$3,967/\text{tháng}}$$

> ✅ **Đạt yêu cầu ngân sách:** **$3,967/tháng** $\le$ **$5,000/tháng cap**, dự phòng an toàn ~20% ($1,033) cho lưu lượng spike vào các dịp cao điểm.

---

## 6. MVP Một Tuần — Slice Nhỏ Nhất Shippable

### Mục tiêu MVP

Chứng minh pipeline Bronze → Gold hoạt động end-to-end với 1 tenant, 1 model, 10M requests/ngày (1% scale).

### Acceptance Criteria

1. ✅ Bronze nhận Kafka events, tokenize PII, ghi Delta với schema enforcement
2. ✅ Silver chạy mỗi 5 phút, dedup, partition đúng
3. ✅ Gold aggregate đủ: p50/p95 latency, cost_usd, error_rate
4. ✅ Dashboard query Gold trong < 5s với filter `tenant_id = 'acme'`
5. ✅ VACUUM xóa Bronze > 7 ngày mà không break Gold query

### Hardest Mechanism — Kiểm tra thế nào?

**PII tokenization tại Bronze không block throughput:** đây là rủi ro kiến trúc lớn nhất.

Test:
```python
# Load test: gửi 10K req/giây trong 5 phút
# Measure: tokenizer p99 latency < 10ms
# Measure: Kafka consumer lag < 30 seconds
# Verify: Bronze files không chứa plaintext PII (regex scan 10 random files)
```

### Week Plan

| Ngày | Việc |
|---|---|
| Day 1-2 | Setup Delta tables schema, Kafka topic, tokenizer mock |
| Day 3-4 | Bronze ingest pipeline + schema enforcement test |
| Day 5 | Silver micro-batch + Gold aggregate |
| Day 6 | Dashboard query + VACUUM test |
| Day 7 | Load test 10M req/ngày, fix bottlenecks, viết runbook |

---

## Kết Luận

Thiết kế này ưu tiên **correctness over cost** ở giai đoạn đầu: PII tokenized ngay từ đầu, Delta time travel cho audit, Z-ORDER cho dashboard performance. Trade-off chính là Spark compute cost — có thể giảm 70% bằng Flink sau khi prove out pipeline. Biggest risk là tokenizer becoming a bottleneck — MVP sẽ stress-test điều này trước tiên.

---

*Tham khảo:*
- *Delta Lake paper (Armbrust et al., VLDB 2020)*
- *AWS S3 pricing (Oct 2026)*
- *Nghị định 13/2023/NĐ-CP về bảo vệ dữ liệu cá nhân (Việt Nam)*
- *K4-Track02-Day18 lab — NB2 Z-ORDER benchmark, NB6 VACUUM behavior*
