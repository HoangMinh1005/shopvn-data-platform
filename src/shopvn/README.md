# Application modules

| Module | Trách nhiệm |
|---|---|
| ingestion | Đọc PostgreSQL, SFTP và Logistics API |
| transformations/bronze | Ghi nguồn và audit metadata |
| transformations/silver | Chuẩn hóa, dedup, bảo vệ PII |
| transformations/gold | Dimensions, facts, marts theo nghiệp vụ đã duyệt |
| streaming | Generator, consumer/Structured Streaming và DLQ |
| quality | Data checks và reconciliation |
| common | Cấu hình, logging, tiện ích dùng chung |

Các module hiện mới là thư mục dự phòng, chưa có pipeline implementation. Khi thêm Python module, tạo package tương ứng và kiểm thử theo contract.
