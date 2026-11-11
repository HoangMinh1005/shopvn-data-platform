# Platform — chưa triển khai

Thư mục dành cho Compose của platform. Đã có [core readiness spike](spike/README.md) cho MinIO source-build, PostgreSQL catalog, Spark và Trino; chưa có full platform Compose gồm Airflow/Kafka/Superset, core spike cũng chưa pass.

Xây từng phần theo [technical spike](../../docs/architecture/implementation-readiness.md); khóa image và dependency sau kiểm chứng. Tách profile core/batch/streaming/serving khi implementation có thật; không ghi lệnh chạy profile chưa tồn tại.

Platform dùng network external `shopvn-net`; không gộp metadata vào PostgreSQL nguồn. Dữ liệu volume, logs và checkpoint phải nằm ngoài Git.
