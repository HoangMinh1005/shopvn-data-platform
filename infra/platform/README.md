# Platform — triển khai từng phần

Đã có [core lakehouse](spike/README.md), [Airflow + monitoring](airflow/README.md) và [direct Finance slice đã kiểm chứng](../../docs/verification/README.md). Kafka/Superset chưa triển khai. Dùng --project-name shopvn-readiness cho core và --project-name shopvn-ops cho Airflow/monitoring để không bị COMPOSE_PROJECT_NAME của nguồn trong .env lấn át.

Xây từng phần theo [technical spike](../../docs/architecture/implementation-readiness.md); khóa image và dependency sau kiểm chứng. Tách profile core/batch/streaming/serving khi implementation có thật; không ghi lệnh chạy profile chưa tồn tại.

Core và ops hiện chia sẻ network external `shopvn-readiness_default`; truy cập nguồn qua host.docker.internal:5434. `shopvn-net` trong baseline là định hướng hợp nhất sau này, không phải network của Compose local đã kiểm chứng. Metadata Airflow dùng database riêng trên PostgreSQL platform; không gộp vào PostgreSQL nguồn. Dữ liệu volume, logs và checkpoint phải nằm ngoài Git.
