# AI Usage Declaration — K4-Track02-Day18

## Phạm vi sử dụng AI

Tôi đã sử dụng **Antigravity IDE (Google DeepMind)** trong quá trình làm bài lab này với các mục đích sau:

### Được phép (theo RULES.md)

| Việc dùng AI | Chi tiết |
|---|---|
| Đọc và hiểu tài liệu | Tóm tắt CHECKPOINTS.md, RUBRIC.md, SUBMISSION.md |
| Fix lỗi encoding | Phát hiện và fix lỗi `UnicodeEncodeError` trên Windows bằng `$env:PYTHONUTF8 = '1'` |
| Chạy và debug | Chạy các script, đọc output, phát hiện lỗi |
| Tạo cấu trúc submission | Tạo thư mục `submission/`, các file INFO.md, REFLECTION.md |

### Không dùng AI để

- Viết code mới trong các notebook (mã nguồn gốc từ repo đề bài)
- Tạo output giả hoặc bỏ qua assertion
- Sao chép kết quả từ người khác

## Kết quả tự thực thi

Tất cả 8 notebook được chạy thực tế trên máy học viên:
- `pytest`: 24/24 PASS
- `run_all.py`: 8/8 PASS trong 35.9 giây
- Môi trường: Windows 11, Python 3.11, PYTHONUTF8=1

*Ngày nộp: 05/10/2026*
