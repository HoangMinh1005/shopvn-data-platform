# Nguồn dữ liệu

Compose ở đây khởi động PostgreSQL, Logistics API và SFTP do bộ đề cung cấp. Dùng lệnh từ [README gốc](../../README.md) để env file được nạp đúng.

Network dùng tên `shopvn-net`. Cấu hình platform sau này phải khai báo network đó là external. Host ports có thể đổi qua `.env`; internal service ports vẫn là postgres:5432, api:8000 và sftp:22.

Nguồn chưa được chạy hay thay đổi dữ liệu trong bước khởi tạo repo. Không dùng `down -v` hoặc reset nguồn như thao tác dừng thông thường.
