# Kết quả kiểm chứng core lakehouse, direct Finance và Airflow/monitoring

Ngày thực hiện: 2026-11-11. Core storage spike, direct Finance và Airflow/monitoring đều đã chạy thực tế, không chỉ validate Compose. [Storage evidence](storage-spike.json), [direct evidence](direct-finance.json), [Airflow evidence](airflow-observability.json), [core image/JAR lock](../../infra/platform/spike/verified-lock.json), [ops lock](../../infra/platform/airflow/verified-lock.json).

## Storage spike — PASS

- Spark 3.5.9 / Scala 2.12.18 / Python 3.11.15 / Java 17.0.21.
- Iceberg 1.12.0 format v2, S3FileIO, MinIO build từ commit upstream, PostgreSQL JDBC Catalog.
- MERGE update/insert và thêm schema column hoạt động.
- Ba Spark process mới: mỗi lần 2 keys, tổng 325.00.
- Trino 484 đọc cùng count/sum trước và sau restart catalog + MinIO + Trino.
- Image/JAR identities đã được ghi ở core verified-lock.json; scope chỉ core đã test. Airflow có bằng chứng/lock riêng bên dưới; Kafka và Superset chưa được chứng nhận runtime.

## Direct Finance — PASS

Nguồn được đọc trong **một transaction PostgreSQL REPEATABLE READ, READ ONLY** bằng shopvn_reader. CSV streaming nằm trong runtime/ bị Git ignore. Không export full_name/phone/email/DOB. Customer IDs trong raw/silver là dữ liệu hạn chế quyền truy cập; Gold dùng customer_ref HMAC với key cục bộ không commit.

Giữ và backup runtime/direct/hmac.key cùng dữ liệu/catalog của môi trường. Không xóa runtime rồi tạo key mới khi vẫn muốn giữ customer identity đã nạp; key rotation cần quy trình riêng. Không đưa key hoặc raw CSV vào Git.

| Nội dung | Kết quả |
|---|---:|
| orders / fact_order | 211806 |
| order_items / fact_sales_line | 444142 |
| returns / fact_return | 4215 |
| customers / products / vouchers nguồn | 50000 / 2000 / 200 |
| Eligible orders theo BR02/BR27 | 110911 |
| FIN02 PostgreSQL = Spark = Trino | 1648602104364.00 VND |
| Mart ngày × kênh | 60 dòng |
| Discount > subtotal bị loại revenue | 622 orders |
| Quantity 0 không tính units hợp lệ | 4420 lines |

Ba process Spark độc lập chạy lại cùng snapshot: fact counts và FIN02 không đổi. Bronze cùng snapshot không append lại; facts MERGE theo key/hash. Mart chỉ thay thế sau khi count, identity, line/header allocation và daily FIN02 khớp nguồn. Advisory lock trên catalog database ngăn hai writer đồng thời cấp surrogate keys.

![Direct Finance FIN02](images/direct-fin02.svg)

Biểu đồ được render từ kết quả query Trino đã đối soát, không dùng số nhập tay. FIN02 bao gồm shipping khách trả, chỉ trừ return refunded, gán về order_date và restate theo trạng thái nguồn hiện quan sát. Không phải cash nhận vào từng ngày hoặc net merchandise chính xác.

## Phạm vi implementation hiện tại

- Bronze/Silver cho 6 nguồn direct: orders, order_items, returns, customers (chỉ thuộc tính cho phép), products, vouchers.
- Gold: dim_date, dim_channel direct, dim_customer/dim_product observed versions; fact_order, fact_sales_line, fact_return.
- Initial customer/product versions và stable SK map đã được nạp; June facts gắn initial_snapshot_fallback. Không chứng minh tier/cost lịch sử tại lúc mua. Complete SCD2/as-of behavior cho các kỳ sau go-live cần kiểm thử riêng.
- dim_date hiện là date spine của kỳ direct đã nạp, chưa phải calendar hoàn chỉnh. Voucher source có ở Bronze/Silver; dim_voucher/campaign analytics chưa thuộc slice này. Một số field mục tiêu trong data dictionary chưa triển khai ở slice Finance; không coi toàn bộ ERD đích là đã deploy.
- Certified output là bảng vật lý lakehouse.analytics.mart_fin02_daily. Atomic replace của mart không phải multi-table transaction toàn bộ Gold. Analytics consumer chỉ dùng mart đã phê chuẩn.
- Đây là triển khai local đơn node, có CLI và DAG Airflow, ops pipeline/alert audit và Prometheus/Grafana. Chưa có Kafka ingestion, Superset dashboard, RBAC production hay toàn bộ ops tables trong model đích. Host ports chỉ bind loopback.

## Airflow + observability — PASS cho scope local

- Airflow 3.4.0 LocalExecutor + Spark provider 6.3.4 submit Spark 3.5.9 thực tế, không chỉ DAG parse hoặc `airflow dags test`. Airflow và Spark dùng Python environments tách biệt.
- Lịch 03:00 VN, catchup tắt, max_active_runs=1, Spark pool một slot, hai retries. Native runtime deadline 30 phút đã cấu hình/parse; **chưa đợi quá hạn 30 phút trong smoke test**.
- Run cố ý DQ fail đã retry hết rồi fail; publish/verify upstream_failed, finalizer fail. Iceberg snapshot certified mart trước/sau không đổi; Prometheus đã có alert firing.
- Run phục hồi với DQ fail lần đầu đã tự retry lần hai rồi success; publish, post-check và finalizer success. FIN02 requested day 30/06 là 49737232800.00 VND, delta 0.00; toàn kỳ vẫn 1648602104364.00 VND. Alert cũ được resolve, không xóa lịch sử failed run.
- Grafana dashboard 13 panels được provision; kết nối datasource được kiểm tra qua Grafana proxy tới Prometheus. 8 service/component health metrics được thu thập. 7 alert rules qua promtool validation.
- Tổng 45 unit/reference tests, gồm deadline 08:00 với UTC/VN/boundary, source coverage guard, identity callback Airflow, enum run_type và finalizer không che upstream failure/không đọc nhầm retry history.

Nguồn chỉ tháng 6/2026; job success tháng 6 **không** là bằng chứng daily freshness tháng 11. Dashboard hiển thị riêng requested day/source coverage/processing time. Chính sách 08:00 chỉ chấp nhận scheduled success cho ngày D−1 hiện hành, manual demo không thay thế. Chưa có Alertmanager/email/Slack delivery, Kafka/Superset, HA/load test, full restore test hoặc multi-table atomic Gold publication. Chưa tự động retention cho candidate/landing/snapshots.

Setup/replay: [Airflow runbook](../../infra/platform/airflow/README.md); xử lý fail/rerun/deadline: [incident runbook](../runbooks/direct-finance-airflow.md). Cảnh báo hiển thị local, không hứa gửi thông báo ra ngoài.

## Các lỗi thật đã xử lý

1. COMPOSE_PROJECT_NAME trong .env lấn át name trong YAML: runner dùng --project-name shopvn-readiness rõ ràng. Các container thử ban đầu có prefix shopvn-sources được xác minh thuộc spike rồi stop; dữ liệu/containers nguồn của người dùng không bị xóa.
2. Seed MERGE INSERT * không tương thích khi bảng đã thêm cột: đổi thành explicit insert column list; rerun sau schema evolution thành công.
3. PostgreSQL COPY boolean là t/f: Bronze giữ wire value, Silver chuyển sang boolean có kiểm tra accepted encodings; không dùng permissive parser để nuốt lỗi.
4. Có 403 order source discount lẻ phần VND. Vẫn phân bổ integer VND theo rank ổn định, phần dư dưới 1 VND được cộng vào rank 1 để giữ đúng precision nguồn DECIMAL(20,2). Không truncate source amount hoặc thay FIN02. Có thêm 3 regression cases; tổng hiện 23 unit/reference tests pass.
5. Airflow dùng virtualenv có psycopg nhưng Spark Python gốc không có: tách run artifact helpers thành module chỉ dùng standard library để publisher không kéo theo dependency orchestration.
6. Airflow 3.4 giữ archived retry attempts trong task_instance: finalizer chỉ xét working_set=true. Không được coi retry lịch sử là trạng thái hiện tại; đồng thời vẫn phải fail nếu task hiện hành upstream fail.

## Chạy lại trên máy hiện tại

```powershell
.venv/Scripts/python.exe scripts/run_lakehouse_spike.py --keep-running
.venv/Scripts/python.exe scripts/run_direct_slice.py --keep-running
.venv/Scripts/python.exe scripts/render_finance_chart.py
.venv/Scripts/python.exe scripts/export_verification.py
.venv/Scripts/python.exe scripts/verify_airflow.py --business-date 2026-06-30
.venv/Scripts/python.exe scripts/export_airflow_verification.py
```

Direct runner mặc định extract snapshot mới. Muốn replay snapshot cũ, thêm --snapshot-id với ID trong runtime/direct/latest-snapshot.json; không sửa CSV/manifest để tạo số khớp. Nếu gặp mismatch, mart không được publish, xem runtime/direct/direct-result.json và direct-slice-report.json.

Hướng dẫn dựng từ đầu: [spike runbook](../../infra/platform/spike/README.md). Source containers phải đang sẵn sàng theo [README gốc](../../README.md); không dựng thêm bộ nguồn trùng port nếu đã có container được cung cấp đang chạy.

## Truy vấn và dừng

Trino UI: http://localhost:18080 (monitoring query, không phải dashboard Finance). Kết nối SQL client: jdbc:trino://localhost:18080/lakehouse/analytics, user readiness cho demo cục bộ. Query [direct Finance mart](../../sql/views/direct_finance.sql).

```powershell
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml stop
```

Lệnh stop giữ data/catalog volumes. Không dùng down -v hoặc xóa object files. Các service nguồn có project/network riêng không nằm trong lệnh dừng này.
