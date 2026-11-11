# Công việc trước triển khai diện rộng

Kết quả thực hiện ngày 2026-11-11: [báo cáo trạng thái](preimplementation-status.md). Checkbox chỉ đánh dấu phần đã quan sát/kiểm tra, không đánh dấu spike pass khi mới có script.

## Đã hoàn thành

- [x] Tổ chức repository và chuyển tài liệu cũ vào vị trí phù hợp.
- [x] Ghi nhận 30 quyết định nghiệp vụ được duyệt.
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
- [x] Tạo fixture giả lập và chạy 15 reference/unit tests; không coi là production integration tests.

## Giai đoạn 3 — Technical spike

- [ ] Spark ghi Iceberg trên MinIO, Trino đọc cùng catalog.
- [ ] MERGE và schema addition hoạt động như kỳ vọng.
- [ ] Cùng input chạy 3 lần có count/sum không đổi.
- [ ] Restart giữ metadata/data; chỉ dùng volume mới của platform.
- [ ] Kiểm tra Spark provider, JAR Scala version, JDBC driver, S3 FileIO và JVM từng image.
- [ ] Khóa image tags/digests và dependency sau kiểm chứng.

Spike đã có Compose/runner, chưa đạt vì image/artifact download chưa hoàn tất. Xem runbook và evidence trong báo cáo trạng thái. Airflow provider/Kafka/Superset vẫn cần kiểm chứng riêng sau core spike.

## Luồng triển khai đầu tiên

Direct orders + order_items -> Bronze -> Silver -> fact_order + fact_sales_line -> DQ/reconciliation -> Trino -> một chart doanh thu. Sau đó mở rộng marketplace, Logistics API, inventory, Kafka.

Definition of Done phải có cả kết quả đúng, khả năng chạy lại, lỗi nguồn được xử lý và visibility trên dashboard/log.
