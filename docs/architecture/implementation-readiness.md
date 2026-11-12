# Công việc trước triển khai diện rộng

Thứ tự stage MVP và checklist tiến độ được quản lý duy nhất tại [kế hoạch triển khai MVP](mvp-implementation-plan.md). **Stage 2 là SFTP + Finance marketplace**, không phải Kafka. Tài liệu readiness này giữ các bước chuẩn bị và bằng chứng nền tảng; P1–P3 bên dưới không phải S1–S3 của lộ trình MVP.

Kết quả thực hiện ngày 2026-11-11: [báo cáo trạng thái](preimplementation-status.md). Checkbox chỉ đánh dấu phần đã quan sát/kiểm tra, không đánh dấu spike pass khi mới có script.

## Đã hoàn thành

- [x] Tổ chức repository và chuyển tài liệu cũ vào vị trí phù hợp.
- [x] Ghi nhận 30 quyết định nghiệp vụ được duyệt.
- [x] Ghi bổ sung BR31/BR32 đã được duyệt sau profiling: Finance FIN02 chính, inventory chỉ từ go-live; rule version 1.1.
- [x] Ghi baseline stack, model và các giới hạn chưa kiểm chứng.
- [x] Tách cấu hình nguồn và chỗ dành cho platform.
- [x] Thêm hướng dẫn Git, env template và CI kiểm tra repository.
- [x] Refactor package theo layer/domain, bỏ runtime dependency vào scripts, đóng gói SQL và kiểm chứng wheel độc lập. Xem [bản đồ code](code-structure.md).

## Tiền triển khai P1 — Profiling nguồn

- [x] Kết nối nguồn có sẵn và xác nhận quyền SELECT của ingestion.
- [x] Kiểm tra schema/PK/FK/null/status, cardinality; ghi timezone chưa được source owner xác nhận.
- [x] Đối soát SUM line với order subtotal, amount với công thức nguồn.
- [x] Xác minh khóa dòng marketplace trong file hợp lệ và số return trên order.
- [x] Phát hiện giới hạn refund composition, inventory business time và warehouse lineage; không tự suy các thông tin thiếu.
- [x] Lưu báo cáo aggregate vào docs/profiling, không commit dữ liệu khách hàng thật.

## Tiền triển khai P2 — Contract và bộ số nghiệm thu

- [x] Tạo data dictionary từng cột/nhóm field: nguồn, công thức, type, nullable, grain.
- [x] Ghi identity, update/delete, incremental, backfill/idempotency; chặn phần thiếu nguồn.
- [x] Tạo KPI contracts với business-rule IDs, phân biệt approved với chi tiết pending.
- [x] Vẽ ERD hiện hành bằng Mermaid; không dùng ERD legacy làm bản cuối.
- [x] Tạo fixture giả lập và chạy 23 reference/unit tests, gồm BR31/BR32 và fractional discount remainder.

## Tiền triển khai P3 — Technical spike

- [x] Spark ghi Iceberg trên MinIO, Trino đọc cùng catalog.
- [x] MERGE và schema addition hoạt động như kỳ vọng.
- [x] Cùng input chạy 3 process mới có count/sum không đổi: storage spike và direct Finance slice.
- [x] Restart catalog/MinIO/Trino giữ dữ liệu storage probe; chỉ dùng volumes của spike.
- [x] Kiểm tra core Spark/Scala/JDBC/S3FileIO/JVM và ghi image/JAR identities.
- [x] Ghi verified-lock cho core đã test và pin host preparation/chart dependencies.
- [x] Airflow LocalExecutor + Spark provider submit thực tế; candidate validation gate, retry, audit và local monitoring. Xem [runbook](../../infra/platform/airflow/README.md).
- [ ] Kafka và Superset chưa triển khai/kiểm chứng runtime.

Core spike, direct Finance và Airflow/monitoring đã có [bằng chứng thực thi](../verification/README.md). Kafka/Superset vẫn cần kiểm chứng riêng; không gắn nhãn full MVP production-ready.

## Luồng triển khai đầu tiên

Đã triển khai direct orders + order_items + returns -> Bronze/Silver -> fact_order + fact_sales_line + fact_return -> DQ/source reconciliation -> mart FIN02 -> Trino -> biểu đồ, được Airflow điều phối và Prometheus/Grafana giám sát.

## Chặng 1 MVP — hoàn thành ngày 12/11/2026

- [x] Full PostgreSQL extraction 9 bảng; inventory, transactions và reviews vào Bronze/Silver, không export review comment.
- [x] SCD2 observed-time customer/product; day-as-of joins cho orders sau go-live; chặn overlap, nhiều current, stale apply và same-clock conflicts.
- [x] Warehouse mapping, current stock đối soát nguồn và actual EOD từ go-live 11/11, không dựng lại June stock.
- [x] DAG inventory riêng lúc 00:00 VN; cửa sổ 60 giây được duyệt; audit, Trino verification, missing/failed alerts và Grafana.
- [x] Actual scheduled EOD ngày 11/11: capture 00:00:02,154 ngày 12/11 VN; replay giữ nguyên Iceberg daily snapshot.
- [x] 84 unit tests, 19 Spark cases, 3 Airflow clock checks, 3 Finance process replays, DQ failure/recovery; FIN02 không đổi.

Chi tiết: [runbook chặng 1](../runbooks/stage1-postgres-inventory.md), [bằng chứng aggregate](../verification/stage1.json). Stage 1 không triển khai Gold transaction/review marts, Kafka, SFTP/API hoặc Superset. **Bước tiếp theo đã xác nhận là S2 — SFTP + Finance marketplace**, bắt đầu S2-01 theo [kế hoạch chính](mvp-implementation-plan.md). Dòng định hướng Kafka trước đây là ghi nhận sai đã được sửa ngày 12/11/2026, không phải một thay đổi kế hoạch được duyệt. Thứ tự S3–S7 trong kế hoạch là đề xuất chờ duyệt; không dùng readiness để tự đổi thứ tự stage.

Definition of Done phải có cả kết quả đúng, khả năng chạy lại, lỗi nguồn được xử lý và visibility trên dashboard/log.

## S2 — Implementation và self-verification PASS ngày 12/11/2026

SFTP/marketplace Finance đã triển khai theo [kế hoạch S2](mvp-implementation-plan.md), không phải Kafka. Có file version/manifest, checksum-before-parse, Bronze/Silver, conformed marketplace sales lines, FIN04–07/FIN10–12, candidate/source/Trino gate, DAG 04:00 VN và local monitoring. 85 valid/3 missing/2 checksum mismatch, 63103 rows; source thiếu/corrupt vẫn có coverage partial.

122 unit tests, 29 Spark cases, 3 fresh-process SFTP replays và actual Airflow failure/recovery đã kiểm chứng. Direct-after-marketplace giữ FIN02 không đổi. Chi tiết tại [runbook S2](../runbooks/stage2-marketplace-finance.md) và [verification index](../verification/README.md). Nhóm còn review/duyệt đóng stage; không tự mở S3–S7.
