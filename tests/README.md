# Kế hoạch kiểm thử

Chưa có test xử lý dữ liệu; CI hiện chỉ kiểm tra repository và cú pháp Compose. Không xem CI xanh là bằng chứng pipeline/KPI đã đúng.

- `fixtures/`: dữ liệu nhỏ được giả lập và kết quả tính tay.
- `unit/`: transformation/business rule tests khi có implementation.
- `integration/`: Spark/Iceberg/catalog/Kafka và kiểm thử idempotency.

Các trường hợp cần có: đơn bình thường/voucher/cancelled, nhiều return/refund một phần, discount vượt subtotal, quantity 0, marketplace returned/disputed, API no shipment/fetch failure, Kafka duplicate, và inventory cutoff.

Mỗi lần rerun cùng input phải so sánh cả count và amount; thử tối thiểu 3 lần theo yêu cầu đề bài. Fixture không chứa PII thật, endpoint riêng hoặc credential thực.
