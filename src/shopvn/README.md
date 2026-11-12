# Application package

Code hiện hành theo [bản đồ cấu trúc](../../docs/architecture/code-structure.md).

| Module | Trách nhiệm |
|---|---|
| contracts | Schema nguồn, PK, allowlist export và rule version |
| ingestion | Snapshot PostgreSQL read-only và SQL baseline được đóng gói |
| transformations/bronze | Kiểm tra landing/checksum và ghi snapshot chống trùng |
| transformations/silver | Chuẩn hóa dữ liệu theo grain nguồn |
| transformations/gold | Observed dimensions và fact builders |
| marts | FIN02 aggregation và controlled publication |
| pipelines | Kết hợp các tầng, DQ, writer lock và persistence |
| jobs | CLI/Spark-submit entry points, import-safe |
| common/config/clients | Runtime paths, env, Spark/Iceberg helpers, Trino client |
| orchestration | Airflow task adapters, operational audit và callbacks |
| monitoring | Exporter Prometheus và service health |
| quality | Data checks, exact Decimal reconciliation, acceptance references |

Không import từ scripts/. CLI wrappers ở scripts/ chỉ giữ tương thích lệnh cũ; application logic ở package này. S1 có observed-time SCD2/day-as-of và actual inventory current/EOD, không phải complete source event-time history. S2 có SFTP/marketplace ingestion, conformed lines và gated Finance publication. Kafka/simulator, API ingestion và Superset chưa triển khai. Không tạo package rỗng cho phần chưa có code.
