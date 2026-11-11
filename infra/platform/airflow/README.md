# Airflow và giám sát — direct Finance local

Phạm vi: đưa slice PostgreSQL → Spark/Iceberg → Trino hiện có vào Airflow, thêm audit, retry, publication gate và giám sát. Không triển khai Kafka/Superset, không thay đổi DB nguồn và không gửi email/Slack.

## Kiến trúc thực thi

Airflow 3.4.0 LocalExecutor và Spark provider 6.3.4 chạy trên image Spark đã kiểm chứng. Airflow dùng Python virtualenv riêng; Spark driver/executor dùng Python gốc 3.11 và Spark 3.5.9, không dùng pyspark-client đi kèm provider làm engine. Không mount Docker socket, không cần Celery/Redis hoặc cluster Spark riêng ở bước local này.

PostgreSQL platform có database riêng `airflow_meta`, role `airflow`, chứa metadata Airflow và schema `ops`. Không tạo schema/tables trong DB nguồn. Prometheus thu metrics từ exporter; Grafana được provision dashboard **ShopVN — Pipeline & Data Quality**. Tất cả cổng host chỉ bind loopback. SimpleAuth chỉ phù hợp demo local, không phải RBAC production.

## Dựng và chạy

Chạy tại root repository, sau khi source containers và core lakehouse đã hoạt động theo [spike runbook](../spike/README.md). Không tạo bộ nguồn thứ hai nếu bộ được cung cấp đang chạy.

```powershell
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml up -d catalog minio trino
.venv/Scripts/python.exe scripts/bootstrap_airflow.py
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml build airflow-api
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml up -d
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml ps -a
```

Bootstrap tạo secrets thiếu hoặc placeholder trong `.env`; không in password. Đọc thông tin đăng nhập trên máy tại `runtime/airflow/local-access.json` (Git ignore). Không commit, chụp màn hình hoặc chia sẻ file này. Không sửa password `.env` riêng lẻ để xoay credential DB/Grafana đã tồn tại; rotation cần cập nhật cả service tương ứng.

| Dịch vụ | Địa chỉ |
|---|---|
| Airflow | http://localhost:18081 |
| Grafana operations | http://localhost:13000/d/shopvn-operations |
| Prometheus / cảnh báo | http://localhost:19090/alerts |
| Exporter status | http://localhost:19108/api/status |
| Trino | http://localhost:18080 |

Máy đã kiểm tra: Docker khoảng 6.77 GiB RAM, chỉ một Spark job tại một thời điểm. Scheduler có giới hạn 3 GiB bao gồm Spark con; giảm concurrency trước khi tăng input. Đây không phải chứng nhận load test hay HA.

## DAG và lịch

`shopvn_direct_finance` chạy **03:00 Asia/Ho_Chi_Minh**, catchup tắt, không tạo backfill tự động khi deploy. Lịch dùng CronTriggerTimetable, không dùng default data interval D−1 để rồi trừ ngày thêm lần nữa.

```text
initialize → extract_snapshot → transform_candidate → validate_candidate
           → publish_finance → verify_and_finish
all tasks  → finalize (ALL_DONE; upstream lỗi thì finalizer cũng fail)
```

Mỗi task tối đa 2 retries, delay 20 giây có exponential backoff tối đa 2 phút. Một DAG run hoạt động và pool `spark_local` một slot; advisory lock tiếp tục ngăn writer CLI chạy song song. Task timeout 15 phút; deadline Airflow 30 phút kể từ queued. `ops.pipeline_run` và `ops.alert` giữ dấu vết lỗi/retry/phục hồi. Callback không lưu raw exception/source PII vào bảng cảnh báo.

`business_date` để trống → ngày D−1 tại Việt Nam. **Nguồn hiện chỉ từ 01–30/06/2026**: để demo, Trigger DAG với `{"business_date":"2026-06-30"}`. Ngày ngoài coverage sẽ fail ở extract trước Spark. Không mặc định đổi lịch sang tháng 6 để làm xanh dashboard. Lịch thực ngày hiện tại sẽ báo thiếu nguồn cho đến khi nhóm bổ sung nguồn giả lập cập nhật hoặc chủ động pause DAG trong thời gian chưa chạy demo.

Extraction vẫn là full snapshot 6 bảng trong một transaction READ ONLY/REPEATABLE READ, không phải CDC/incremental. Retry extract trong cùng run tái sử dụng manifest đã hoàn tất; manual run mới tạo snapshot mới. Giữ nguyên `runtime/direct/hmac.key` để không đổi customer identity.

## Publication gate và giới hạn

- Spark tạo candidate riêng `lakehouse.staging.mart_fin02_<run_key>`.
- Trino đọc **đúng Iceberg snapshot ID** của candidate, đối soát toàn bộ ngày × kênh, count và Decimal amounts với manifest nguồn. Chỉ khi pass mới publish.
- Publisher giữ writer lock, từ chối candidate đã đổi sau validation và snapshot quan sát cũ hơn publication hiện có. Retry cùng publication/snapshot là no-op.
- Mart công bố bằng một Iceberg atomic replace, có `publication_id`, `data_as_of`, `source_candidate_snapshot_id`. Post-check xác nhận lineage và FIN02 của ngày yêu cầu.
- Đây chỉ là gate cho **certified mart**, không phải multi-table atomic transaction cho toàn bộ Bronze/Silver/Gold. Transform có thể đã cập nhật facts/dimensions khi DQ ngoài Spark thất bại. Analytics consumer chỉ dùng certified mart.
- Candidate tables, landing snapshots và logs được giữ để debug; chưa tự xóa/expire snapshot/orphan files. Cần chính sách retention trước khi chạy dài hạn; không xóa S3 files thủ công.

## Giám sát

Dashboard tách ngày xử lý, ngày nghiệp vụ và source coverage; hiển thị service health, workflow state, FIN02 source/candidate, delta, DQ, last completion và alert. Spark là batch task, không bị báo down khi không có job. Số tiền trên Prometheus là float phục vụ hiển thị, **không** thay thế đối soát Decimal chính xác.

7 rules: service down, pipeline failed, DQ gate failed, reconciliation mismatch, exporter down, polling stale, deadline 08:00. Monitor đọc PostgreSQL nguồn bằng SELECT 1 và kiểm tra Airflow API/scheduler/dag processor/metadata, platform PostgreSQL, MinIO, Trino. Không giám sát host CPU/disk/cAdvisor ở bước này.

Deadline 08:00 độc lập scheduler: sau 08:00 VN nếu chưa có scheduled success hoàn thành trước 08:00 hôm đó thì cảnh báo. Manual demo không thay thế scheduled success. `MONITOR_START_DATE` được bootstrap thành ngày mai để không phát cảnh báo hồi tố trước go-live. Hoàn thành trễ vẫn ghi nhận missed trong ngày. Cảnh báo chỉ hiển thị qua Prometheus/Grafana và audit; chưa có Alertmanager hay delivery ra ngoài.

## Kiểm chứng và vận hành

```powershell
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml exec -T airflow-scheduler airflow dags list-import-errors -o json
.venv/Scripts/python.exe scripts/verify_airflow.py --business-date 2026-06-30
.venv/Scripts/python.exe -m pytest -q
```

Verifier tạo một run cố ý DQ fail (mart không đổi, finalizer/DAG fail, Prometheus firing), sau đó một run DQ fail lần đầu và pass lần hai để kiểm tra retry, publication và alert recovery. Không chạy smoke test trong production hoặc đồng thời manual run khác. Xem [runbook sự cố](../../../docs/runbooks/direct-finance-airflow.md) và [bằng chứng thực thi](../../../docs/verification/README.md). Native deadline 30 phút chỉ được cấu hình/parse, smoke test không đợi expiry; chính sách 08:00 được kiểm tra fixed-clock unit tests.

```powershell
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml stop
```

`stop` giữ toàn bộ volumes. Không dùng `down -v`. Dừng ops trước core lakehouse; không dừng source containers không thuộc project.

Tham chiếu: [Airflow deadlines](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/deadline-alerts.html), [Spark provider](https://airflow.apache.org/registry/providers/apache-spark/6.3.4/), [timetables](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/timetable.html).
