# Kế hoạch kiểm thử

Đã có 23 unit/reference checks cho [bộ số giả lập](fixtures/business_acceptance.json), gồm BR31/BR32 và fractional source discount. Runtime integration runners cho storage và direct Finance đã chạy 3 fresh Spark processes, nguồn/Spark/Trino đối soát khớp; xem [evidence](../docs/verification/README.md). CI hiện chỉ chạy repository checks, lint, synthetic tests và Compose validation, chưa tự chạy integration stack hoặc nguồn thật.

- `fixtures/`: dữ liệu nhỏ được giả lập và kết quả tính tay.
- `unit/`: transformation/business rule tests khi có implementation.
- `integration/`: Spark/Iceberg/catalog/Kafka và kiểm thử idempotency.

Các trường hợp cần có: đơn bình thường/voucher/cancelled, nhiều return/refund một phần, discount vượt subtotal, quantity 0, marketplace returned/disputed, API no shipment/fetch failure, Kafka duplicate, và inventory cutoff.

Mỗi lần rerun cùng input phải so sánh cả count và amount; thử tối thiểu 3 lần theo yêu cầu đề bài. Fixture không chứa PII thật, endpoint riêng hoặc credential thực.
