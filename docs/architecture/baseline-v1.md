# Kiến trúc baseline v1

Ngày ghi nhận: 2026-11-11. **Core Spark–Iceberg–MinIO–PostgreSQL JDBC Catalog–Trino và Airflow/monitoring local đã được kiểm chứng**, xem [runtime evidence](../verification/README.md), [core image/JAR lock](../../infra/platform/spike/verified-lock.json) và [Airflow runbook](../../infra/platform/airflow/README.md). Kafka và Superset trong bảng vẫn là baseline mục tiêu, chưa phải full-stack runtime certification.

Kết quả chuẩn bị và giới hạn thực tế: [preimplementation status](preimplementation-status.md). Generic Spark image khác runtime mục tiêu nên đã build image riêng Python3.11.15/Java17.0.21; MinIO dùng source-build pinned commit cho demo. Core đã chạy được từ cấu hình repo; không tự suy tương thích của các thành phần chưa test.

## Kiến trúc

```text
PostgreSQL / SFTP / Logistics API
                  |
          Airflow batch jobs
                  |
             Spark / PySpark
                  |
    Bronze -> Silver -> Gold -> KPI marts
        Iceberg v2 / Parquet / MinIO
                  |
           Trino -> Superset

Python simulator -> Kafka KRaft -> Spark Structured Streaming
                                      |
                            Bronze/Silver simulated

PostgreSQL platform: Iceberg JDBC catalog + Airflow/Superset metadata
```

## Stack đã chọn

| Thành phần | Baseline |
|---|---|
| Python application | 3.11 |
| Spark | 3.5.9, Scala 2.12; Java 17 cho Spark |
| Table format | Iceberg 1.12.0, table format v2 |
| File format | Parquet + ZSTD |
| Storage | MinIO single-node; image tag/digest chốt sau spike |
| Catalog | Iceberg JDBC Catalog trên PostgreSQL 16 |
| Broker | Apache Kafka 4.3.2 KRaft, một node cho MVP |
| Orchestrator | Airflow 3.4.0 LocalExecutor |
| Spark provider | 6.3.4; submit thực tế với Spark 3.5.9, Airflow virtualenv tách Spark Python |
| Query engine | Trino 484; sử dụng JVM đi kèm image Trino, không áp Java 17 của Spark cho Trino |
| BI | Superset 6.0.0, PostgreSQL metadata; cấu hình tối thiểu cho demo |
| Deployment | Docker Compose, Linux containers trên WSL2 |
| DQ | PySpark/SQL checks + audit tables |
| Monitoring local | Prometheus 3.16.0, Grafana 13.3.1, Python operational exporter |
| Development | pytest, Ruff, SQLFluff, GitHub Actions; dependency khóa sau spike |

Airflow, Spark và Superset dùng các image/môi trường riêng. Provider Spark mới có dependency client khác Spark engine; technical spike phải kiểm tra chính xác thay vì cài toàn bộ package mới nhất chung môi trường.

## Ranh giới triển khai

- Giữ PostgreSQL nguồn riêng với PostgreSQL platform.
- Platform PostgreSQL có database/user riêng cho iceberg_catalog, airflow_meta, superset_meta.
- Spark là writer chính cho analytics tables. Trino phục vụ certified views/BI; tránh nhiều writer sửa cùng bảng trong MVP.
- Bronze/Silver giữ riêng theo nguồn; hợp nhất sales line sau khi conformed.
- Batch công bố báo cáo chính thức. Kafka mô phỏng tách data_origin để tránh trộn với dữ liệu nguồn.
- Streaming chạy service dài hạn riêng; Airflow điều phối batch, reconciliation và maintenance.
- Nguồn chỉ được ingestion đọc; API extraction tập trung, có rate limit/retry, không gọi từ Spark executor.
- Chưa đưa CDC/Debezium, Kafka Connect, Schema Registry hoặc Kubernetes vào MVP.

## Monitoring là yêu cầu MVP

Phải có giao diện/log trả lời được thời điểm hoàn thành, tình trạng service và đối soát doanh thu; có cảnh báo deadline trước 08:00 và runbook phục hồi trong 2 giờ theo đề bài. Airflow UI + audit dashboard + service health là baseline tối thiểu cần chứng minh. Có thể bổ sung Prometheus/Grafana, nhưng không lùi yêu cầu observability sang phase 2.

## Cổng kiểm chứng

Thứ tự stage và checklist nghiệm thu nằm ở [kế hoạch triển khai MVP](mvp-implementation-plan.md); **S2 là SFTP + Finance marketplace**, không phải simulator/Kafka. Kiến trúc có Kafka không đồng nghĩa phải triển khai Kafka ngay sau S1. [Readiness checklist](implementation-readiness.md) giữ các bước tiền triển khai. Mỗi image phải khóa tag/digest và mỗi JAR/provider phải có version sau khi spike thành công. Không sử dụng `latest` để triển khai baseline đã nghiệm thu.
