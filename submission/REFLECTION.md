# Reflection — K4-Track02-Day18 Lakehouse Lab

## Anti-Pattern: Small-File Problem trong Streaming Pipeline

Trong hệ thống LLM Observability (log request/response API), **small-file problem** là anti-pattern nguy hiểm nhất.

### Nguyên nhân và rủi ro

Micro-batching ngắn (5–30s) liên tục tạo hàng nghìn file Parquet nhỏ (< 1 MB). Khi dữ liệu tích lũy hàng triệu file, chi phí đọc metadata của Delta log và LIST API trên object storage vượt xa thời gian đọc dữ liệu. Dashboard phân tích latency/cost dễ bị timeout (tăng từ < 1s lên 60–120s).

### Giải pháp phòng tránh

1. **Compaction định kỳ**: Chạy `OPTIMIZE` hàng giờ gom file về kích thước chuẩn (~256 MB).
2. **Clustering với Z-ORDER**: Sắp xếp theo `tenant_id` hoặc `model` giúp engine skip ≥ 50–90% files khi truy vấn.
3. **Phân vùng hợp lý**: Partition theo `date` tránh full scan.
4. **Tối ưu batch interval**: Tăng micro-batch lên 3–5 phút.

Thực tế lab chứng minh: compaction kết hợp Z-ORDER giúp số file giảm mạnh và tỉ lệ pruning đạt ≥ 10×, phục hồi hoàn toàn tốc độ truy vấn.

---

*Khai báo AI: Chi tiết tại [AI_USAGE.md](AI_USAGE.md).*
