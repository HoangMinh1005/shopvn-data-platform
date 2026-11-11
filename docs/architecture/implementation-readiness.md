# Công việc trước triển khai diện rộng

## Đã hoàn thành

- [x] Tổ chức repository và chuyển tài liệu cũ vào vị trí phù hợp.
- [x] Ghi nhận 30 quyết định nghiệp vụ được duyệt.
- [x] Ghi baseline stack, model và các giới hạn chưa kiểm chứng.
- [x] Tách cấu hình nguồn và chỗ dành cho platform.
- [x] Thêm hướng dẫn Git, env template và CI kiểm tra repository.

## Giai đoạn 1 — Profiling nguồn

- [ ] Chạy nguồn và xác nhận quyền SELECT của ingestion.
- [ ] Kiểm tra schema/PK/FK/null/status, cardinality và timezone.
- [ ] Đối soát SUM line với order subtotal, amount với công thức nguồn.
- [ ] Xác minh khóa dòng marketplace và số return trên order.
- [ ] Xác minh refund có shipping hay không, inventory semantics và warehouse lineage.
- [ ] Lưu báo cáo tổng hợp vào docs/profiling, không commit dữ liệu khách hàng thật.

## Giai đoạn 2 — Contract và bộ số nghiệm thu

- [ ] Hoàn thiện data dictionary từng cột: nguồn, công thức, type, nullable, grain.
- [ ] Chốt identity, update/delete, incremental cursor, backfill và idempotency từng bảng.
- [ ] Hoàn thiện KPI contracts với business-rule IDs.
- [ ] Vẽ ERD hiện hành; không dùng ERD legacy làm bản cuối.
- [ ] Tạo fixture giả lập có kết quả tính tay cho sale, voucher, refund, lỗi và duplicate.

## Giai đoạn 3 — Technical spike

- [ ] Spark ghi Iceberg trên MinIO, Trino đọc cùng catalog.
- [ ] MERGE và schema addition hoạt động như kỳ vọng.
- [ ] Cùng input chạy 3 lần có count/sum không đổi.
- [ ] Restart giữ metadata/data; chỉ dùng volume mới của platform.
- [ ] Kiểm tra Spark provider, JAR Scala version, JDBC driver, S3 FileIO và JVM từng image.
- [ ] Khóa image tags/digests và dependency sau kiểm chứng.

## Luồng triển khai đầu tiên

Direct orders + order_items -> Bronze -> Silver -> fact_order + fact_sales_line -> DQ/reconciliation -> Trino -> một chart doanh thu. Sau đó mở rộng marketplace, Logistics API, inventory, Kafka.

Definition of Done phải có cả kết quả đúng, khả năng chạy lại, lỗi nguồn được xử lý và visibility trên dashboard/log.
