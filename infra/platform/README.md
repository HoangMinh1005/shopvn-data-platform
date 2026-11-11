# Platform — chưa triển khai

Đã có [core lakehouse Compose đã pass](spike/README.md) và [direct Finance slice đã kiểm chứng](../../docs/verification/README.md). Chưa có full platform Compose gồm Airflow/Kafka/Superset. Khi chạy core, dùng --project-name shopvn-readiness để không bị COMPOSE_PROJECT_NAME của nguồn trong .env lấn át.

Xây từng phần theo [technical spike](../../docs/architecture/implementation-readiness.md); khóa image và dependency sau kiểm chứng. Tách profile core/batch/streaming/serving khi implementation có thật; không ghi lệnh chạy profile chưa tồn tại.

Platform dùng network external `shopvn-net`; không gộp metadata vào PostgreSQL nguồn. Dữ liệu volume, logs và checkpoint phải nằm ngoài Git.
