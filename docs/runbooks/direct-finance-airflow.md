# Xử lý sự cố direct Finance trên Airflow

Phạm vi local đã triển khai: [Airflow runbook](../../infra/platform/airflow/README.md). Không có Kafka, Superset, API ingestion/SFTP ingestion ở giai đoạn này.

## 1. Task/DAG fail hoặc service down

Phát hiện tại Airflow task logs, Grafana operations hoặc Prometheus `/alerts`. Xem task đầu tiên lỗi; `finalize` fail sau upstream là hành vi chủ đích, không phải nguyên nhân gốc.

1. Kiểm tra `docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml ps -a`.
2. Với service health: kiểm tra source PostgreSQL, catalog, MinIO, Trino và các Airflow component. Không restart toàn bộ Docker Desktop hoặc container ngoài project chỉ để xử lý một task.
3. Với OOM/timeout: kiểm tra Docker memory, scheduler/Spark logs, giới hạn pool và nguồn dữ liệu. Không tăng concurrency trên máy RAM hạn chế. Task đã có tối đa 2 retries.
4. Khi nguyên nhân tạm thời đã khắc phục, clear task thất bại và downstream trong cùng DAG run. Extract tái dùng snapshot của run đó. Muốn đọc lại trạng thái nguồn mới thì trigger run mới với business_date rõ ràng.
5. Xác nhận publish và verify thành công, FIN02 delta = 0, DQ pass, publication ID đúng run, alert được resolve. Không chỉ nhìn DAG xanh.

## 2. SourceCoverageError / chưa có dữ liệu ngày yêu cầu

Source fixture hiện chỉ tháng 6/2026. Lịch D−1 thực tế ở tháng khác phải fail; không backfill dữ liệu giả hoặc sửa manifest để vượt gate. Demo có chủ đích dùng business_date 2026-06-30, còn vận hành daily cần nguồn simulator cập nhật theo contract.

Ảnh hưởng: certified mart trước đó được giữ, nhưng không đáp ứng freshness ngày mới. Ghi rõ source coverage trên dashboard. Nếu chưa sẵn sàng vận hành daily, nhóm có thể pause DAG ở UI; chính sách 08:00 vẫn kiểm tra nguồn daily khi đã go-live và không bị manual run làm xanh.

## 3. DQ/reconciliation fail

Candidate không được publish. Xem `runtime/orchestration/runs/<run_key>/validation.json`, `transform.json`, `context.json`; không đưa CSV/HMAC key/PII vào issue.

1. Đối chiếu snapshot nguồn, rule_version, row counts, eligible orders, refund status và amount. Dùng Decimal, không dùng số float từ chart để kết luận lệch tiền.
2. Kiểm tra có bật `force_validation_failure` hoặc `validation_fail_once` từ smoke test hay không. Chúng chỉ gây lỗi, không có nút bypass DQ.
3. Sửa nguyên nhân trong code/contract theo quy trình review. Không sửa manifest/result JSON để phê chuẩn candidate.
4. Nếu candidate snapshot đã thay đổi sau approval, chạy lại validation; nếu data_as_of cũ hơn publication thì dùng snapshot mới, không cưỡng ép ghi đè mart mới.
5. Run thành công cho cùng business_date resolve cảnh báo của lần thất bại, nhưng lịch sử run fail vẫn giữ trong metadata/audit. Chỉ certified mart được bảo vệ atomic; facts/dimensions có thể đã được cập nhật trong transform.

## 4. Deadline và monitor

Runtime quá 30 phút và thiếu scheduled publication trước 08:00 VN là hai điều kiện khác nhau. Manual run tháng 6 không chứng minh daily freshness hiện tại. Kiểm tra lịch, timezone, scheduler heartbeat, input coverage và tài nguyên.

Nếu exporter còn HTTP 200 nhưng DB poll không tiến triển, `ShopVNMonitorStale` phải phát cảnh báo. Nếu exporter ngừng, `ShopVNExporterUnavailable` phải phát cảnh báo từ Prometheus. Bộ local này không có thông báo email/Slack; người vận hành phải xem UI. Không hứa notification delivery khi chưa cấu hình receiver.

## 5. Khôi phục sau restart

Giữ `.env`, `runtime/airflow`, landing snapshots, `runtime/direct/hmac.key`, catalog/object-store volumes và monitoring volumes. Không xóa volumes hay S3 objects để chữa task lỗi. Khởi động core trước, rồi `shopvn-ops up -d`; init migration và ops schema idempotent. Kiểm tra import errors, service health và chạy một business_date đã có nguồn. Chưa có kiểm thử disaster recovery/restore backup hoàn chỉnh.
