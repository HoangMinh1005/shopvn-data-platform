# Công việc trước triển khai diện rộng

Kết quả thực hiện ngày 2026-11-11: [báo cáo trạng thái](preimplementation-status.md). Checkbox chỉ đánh dấu phần đã quan sát/kiểm tra, không đánh dấu spike pass khi mới có script.

## Đã hoàn thành

- [x] Tổ chức repository và chuyển tài liệu cũ vào vị trí phù hợp.
- [x] Ghi nhận 30 quyết định nghiệp vụ được duyệt.
- [x] Ghi bổ sung BR31/BR32 đã được duyệt sau profiling: Finance FIN02 chính, inventory chỉ từ go-live; rule version 1.1.
- [x] Ghi baseline stack, model và các giới hạn chưa kiểm chứng.
- [x] Tách cấu hình nguồn và chỗ dành cho platform.
- [x] Thêm hướng dẫn Git, env template và CI kiểm tra repository.

## Giai đoạn 1 — Profiling nguồn

- [x] Kết nối nguồn có sẵn và xác nhận quyền SELECT của ingestion.
- [x] Kiểm tra schema/PK/FK/null/status, cardinality; ghi timezone chưa được source owner xác nhận.
- [x] Đối soát SUM line với order subtotal, amount với công thức nguồn.
- [x] Xác minh khóa dòng marketplace trong file hợp lệ và số return trên order.
- [x] Phát hiện giới hạn refund composition, inventory business time và warehouse lineage; không tự suy các thông tin thiếu.
- [x] Lưu báo cáo aggregate vào docs/profiling, không commit dữ liệu khách hàng thật.

## Giai đoạn 2 — Contract và bộ số nghiệm thu

- [x] Tạo data dictionary từng cột/nhóm field: nguồn, công thức, type, nullable, grain.
- [x] Ghi identity, update/delete, incremental, backfill/idempotency; chặn phần thiếu nguồn.
- [x] Tạo KPI contracts với business-rule IDs, phân biệt approved với chi tiết pending.
- [x] Vẽ ERD hiện hành bằng Mermaid; không dùng ERD legacy làm bản cuối.
- [x] Tạo fixture giả lập và chạy 23 reference/unit tests, gồm BR31/BR32 và fractional discount remainder.

## Giai đoạn 3 — Technical spike

- [x] Spark ghi Iceberg trên MinIO, Trino đọc cùng catalog.
- [x] MERGE và schema addition hoạt động như kỳ vọng.
- [x] Cùng input chạy 3 process mới có count/sum không đổi: storage spike và direct Finance slice.
- [x] Restart catalog/MinIO/Trino giữ dữ liệu storage probe; chỉ dùng volumes của spike.
- [x] Kiểm tra core Spark/Scala/JDBC/S3FileIO/JVM và ghi image/JAR identities.
- [x] Ghi verified-lock cho core đã test và pin host preparation/chart dependencies.
- [ ] Kiểm chứng Airflow Spark provider/submit, Kafka và Superset ở giai đoạn tiếp theo.

Core spike và direct Finance đã pass, xem [bằng chứng thực thi](../verification/README.md). Airflow provider/Kafka/Superset vẫn cần kiểm chứng riêng; không gắn nhãn full MVP production-ready.

## Luồng triển khai đầu tiên

Đã triển khai direct orders + order_items + returns -> Bronze/Silver -> fact_order + fact_sales_line + fact_return -> DQ/source reconciliation -> mart FIN02 -> Trino -> biểu đồ. Bước tiếp theo: Airflow orchestration cho slice này, rồi marketplace/Logistics/inventory/Kafka theo phạm vi đã duyệt.

Definition of Done phải có cả kết quả đúng, khả năng chạy lại, lỗi nguồn được xử lý và visibility trên dashboard/log.
