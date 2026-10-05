# Reflection — K4-Track02-Day18 Lakehouse Lab

## Anti-Pattern: Small-File Problem trong Streaming Pipeline

Trong hệ thống LLM observability mà tôi quan tâm — nơi mỗi API call được log thành một record — **small-file problem** là anti-pattern nguy hiểm nhất.

### Tại sao dễ gặp?

Streaming ingest thường ghi từng micro-batch nhỏ (5–30 giây/batch), mỗi batch tạo một file Parquet riêng. Với hệ thống nhận 1M request/ngày, sau 1 tuần có thể tích lũy hàng triệu file nhỏ dưới 1 MB. Khi query "p95 latency theo model hôm qua", engine phải mở hàng nghìn file — overhead metadata lớn hơn thời gian đọc dữ liệu thực.

### Hậu quả thực tế

- Query dashboard chậm từ < 1s lên 60–120s
- S3/GCS LIST operation tốn kém hơn GET ở quy mô lớn
- File count explosion khiến transaction log của Delta tăng nhanh, làm chậm metadata reads

### Cách phòng tránh

1. **Compact theo lịch**: OPTIMIZE mỗi giờ với `target_size=256MB`
2. **Z-ORDER by model**: giúp dashboard filter theo model prune 10× files
3. **Micro-batch size**: tăng batch interval lên 5 phút thay vì 30 giây
4. **Partition by date**: tránh toàn bộ scan khi filter theo ngày

Lab này đo thực tế: 200 files → compact còn ~50 files, speedup 10× pruning ratio — con số đủ thuyết phục để ưu tiên scheduled OPTIMIZE trong mọi streaming pipeline.

---

*Phạm vi dùng AI: Antigravity IDE hỗ trợ đọc tài liệu, chạy và debug các notebook. Tất cả phân tích, giải thích số liệu và reflection là của bản thân học viên.*
