# Quy trình làm việc

1. Tạo nhánh từ `main`: `feat/<ten-viec>`, `fix/<ten-loi>` hoặc `docs/<ten-tai-lieu>`.
2. Mỗi thay đổi tập trung một mục đích; ghi rõ input/output và cách kiểm tra.
3. Đọc [nghiệp vụ đã duyệt](docs/business/business-rules-v1.md) trước khi sửa logic tính KPI.
4. Chạy `python scripts/check_repository.py`; nếu sửa Compose, chạy thêm lệnh validate trong README.
5. Khi có code xử lý, thêm kiểm thử với kết quả kỳ vọng độc lập; tránh fixture chứa dữ liệu khách hàng thật.
6. Đề nghị một thành viên khác review trước khi merge.

## Quy ước

- Dùng Python 3.11 cho application; không cài Airflow, Spark, Superset chung một môi trường Python host.
- Business logic đặt trong `src/shopvn/`, DAG chỉ điều phối.
- Tên Python: `snake_case`; tên bảng: `dim_*`, `fact_*`, `mart_*`; source IDs giữ riêng theo nguồn.
- Timestamp lưu kèm quy ước timezone; ngày nghiệp vụ dùng Asia/Ho_Chi_Minh.
- Dùng biến môi trường/secret cho credential; `.env.example` chỉ chứa placeholder.
- Cập nhật tài liệu mapping và KPI contract cùng thay đổi logic.
- Thay đổi nghiệp vụ đã duyệt phải có decision mới, lý do và ảnh hưởng đến backfill.

## Trạng thái dependency

`pyproject.toml` mới khai báo package và quy tắc lint. Dependency runtime/provider/JAR và image digest sẽ được khóa sau technical spike; không xem repo hiện tại là một build platform đã kiểm chứng.
