# Kết quả kiểm chứng core lakehouse, direct Finance và Airflow/monitoring

Ngày thực hiện: 2026-11-11. Core storage spike, direct Finance và Airflow/monitoring đều đã chạy thực tế, không chỉ validate Compose. [Storage evidence](storage-spike.json), [direct evidence](direct-finance.json), [Airflow evidence](airflow-observability.json), [core image/JAR lock](../../infra/platform/spike/verified-lock.json), [ops lock](../../infra/platform/airflow/verified-lock.json).

Cập nhật sau refactor: [refactor evidence](refactor.json) và [bản đồ code](../architecture/code-structure.md). Các kết quả Airflow hiện đã được chạy lại với package/tầng xử lý mới.

## Chặng 1 — PASS ngày 12/11/2026

[Bằng chứng aggregate chặng 1](stage1.json) là trạng thái mới nhất; các báo cáo storage/direct/refactor bên dưới giữ làm mốc lịch sử.

- Full snapshot đủ 9 bảng PostgreSQL trong transaction READ ONLY/REPEATABLE READ. Thêm inventory (2.000), inventory_transactions (439.722), product_reviews (41.226); không export review comment.
- SCD2 observed-time customer/product, khoảng nửa mở và stable SK; day-as-of joins theo business key + order_date, không làm nhân fact rows. Chặn overlap, nhiều current, same-clock conflict và stale Finance apply. June facts vẫn có nhãn initial_snapshot_fallback, không suy tier/cost tại thời điểm mua.
- Current stock đối soát nguồn theo warehouse: 2.000 sản phẩm, tổng 5.053.681 đơn vị. Policy go-live thực 11/11/2026, cửa sổ 60 giây đã được duyệt và persist immutable.
- Actual scheduled EOD ngày 11/11 được chụp lúc **00:00:02,153998 ngày 12/11 giờ Việt Nam** (lag 2,153998 giây), DAG success. Replay capture thật giữ nguyên daily Iceberg snapshot ID/count/sum. Không chỉnh đồng hồ/manifest để làm test EOD; không backfill stock tháng 6.
- 84 unit tests, 4 base Spark + 15 temporal Spark cases, 3 kiểm thử clock Pendulum trong runtime Airflow. Temporal fixtures nằm trong namespace riêng, không ghi lịch sử giả vào actual tables.
- Ba fresh-process Finance replays trên manifest 9 bảng: FIN02 vẫn 1.648.602.104.364,00 VND, 60 ngày × kênh; certified mart không đổi trong candidate-only replays. Actual Airflow DQ failure/recovery cũng pass với 9 bảng.
- Inventory audit/DAG, 6 panels mới (tổng 19) và 2 rules mới (tổng 9). Thiếu EOD sau 00:10 không được làm xanh bằng manual current run; notifications vẫn chỉ local.

EOD là **near-cutoff source observation**, không chứng minh stock tuyệt đối tại 00:00. SCD2 là observed-time/day-grain, không full source event-time history. Transactions/reviews chỉ ở Bronze/Silver, chưa phải Gold marts hoàn chỉnh. Runtime Finance theo lịch hiện vẫn báo thiếu source coverage vì orders chỉ có tháng 6; inventory success không làm Finance November freshness thành đạt. Xem [runbook chặng 1](../runbooks/stage1-postgres-inventory.md).

Trạng thái môi trường khi tiếp tục bàn giao ngày 12/11: Docker đã được bật lại; các container nguồn PostgreSQL/API/SFTP không còn tồn tại vào cuối lượt kiểm tra. Không tự tạo nguồn thay thế hoặc sửa/xóa audit. Evidence giữ kết quả EOD/Finance đã hoàn thành trước đó, đồng thời ghi runtime health tại lúc export. Cần khôi phục đúng nguồn trước các lần extract/scheduled EOD tiếp theo; alert source down/Finance coverage/deadline không được che để báo toàn hệ thống xanh.

Theo yêu cầu kiểm tra của người dùng, đã tìm thấy volume cũ có cấu trúc PG16 và shutdown sạch; có khả năng khôi phục cao nhưng chưa start/restore. Xem [kết quả kiểm tra và giới hạn](../runbooks/source-postgres-recovery.md).

Cập nhật sau đó ngày 12/11: người dùng đã tự phục hồi nguồn; kiểm tra read-only lại xác nhận PostgreSQL 9 bảng/counts/keys/FIN02/stock khớp, API health và SFTP authentication thành công, monitor 8/8 components healthy tại lúc kiểm tra. Không ghi đè health snapshot lịch sử trong stage1.json và không coi đây là full DAG rerun. Thứ tự công việc tiếp theo theo [kế hoạch MVP](../architecture/mvp-implementation-plan.md): **S2 — SFTP + Finance marketplace**.

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

- Bronze/Silver cho 9 bảng PostgreSQL: orders, order_items, returns, customers (chỉ thuộc tính cho phép), products, vouchers, inventory, inventory_transactions, product_reviews.
- Gold: dim_date, dim_channel direct, dim_customer/dim_product observed versions, dim_warehouse; fact_order, fact_sales_line, fact_return, fact_inventory_daily_snapshot. Analytics có current_stock và certified mart_fin02_daily.
- Initial customer/product versions và stable SK map đã được nạp; June facts gắn initial_snapshot_fallback. Observed-time SCD2/day-as-of đã kiểm thử trong chặng 1; không chứng minh tier/cost lịch sử chính xác tại lúc mua.
- dim_date giữ ngày direct và inventory đã nạp, chưa phải calendar hoàn chỉnh. Voucher source có ở Bronze/Silver; dim_voucher/campaign analytics chưa thuộc slice này. Một số field mục tiêu trong data dictionary chưa triển khai ở slice Finance; không coi toàn bộ ERD đích là đã deploy.
- Certified output là bảng vật lý lakehouse.analytics.mart_fin02_daily. Atomic replace của mart không phải multi-table transaction toàn bộ Gold. Analytics consumer chỉ dùng mart đã phê chuẩn.
- Đây là triển khai local đơn node, có CLI và DAG Airflow, ops pipeline/alert audit và Prometheus/Grafana. Chưa có Kafka ingestion, Superset dashboard, RBAC production hay toàn bộ ops tables trong model đích. Host ports chỉ bind loopback.

## Airflow + observability — PASS cho scope local

- Airflow 3.4.0 LocalExecutor + Spark provider 6.3.4 submit Spark 3.5.9 thực tế, không chỉ DAG parse hoặc `airflow dags test`. Airflow và Spark dùng Python environments tách biệt.
- Lịch 03:00 VN, catchup tắt, max_active_runs=1, Spark pool một slot, hai retries. Native runtime deadline 30 phút đã cấu hình/parse; **chưa đợi quá hạn 30 phút trong smoke test**.
- Run cố ý DQ fail đã retry hết rồi fail; publish/verify upstream_failed, finalizer fail. Iceberg snapshot certified mart trước/sau không đổi; Prometheus đã có alert firing.
- Run phục hồi với DQ fail lần đầu đã tự retry lần hai rồi success; publish, post-check và finalizer success. FIN02 requested day 30/06 là 49737232800.00 VND, delta 0.00; toàn kỳ vẫn 1648602104364.00 VND. Alert cũ được resolve, không xóa lịch sử failed run.
- Grafana dashboard 13 panels được provision; kết nối datasource được kiểm tra qua Grafana proxy tới Prometheus. 8 service/component health metrics được thu thập. 7 alert rules qua promtool validation.
- Tổng hiện tại 59 unit/reference tests, gồm deadline 08:00/runtime callback, source coverage guard, identity callback Airflow, enum run_type, finalizer không che upstream failure/không đọc nhầm retry history, schema contract và package boundaries.

Nguồn chỉ tháng 6/2026; job success tháng 6 **không** là bằng chứng daily freshness tháng 11. Dashboard hiển thị riêng requested day/source coverage/processing time. Chính sách 08:00 chỉ chấp nhận scheduled success cho ngày D−1 hiện hành, manual demo không thay thế. Chưa có Alertmanager/email/Slack delivery, Kafka/Superset, HA/load test, full restore test hoặc multi-table atomic Gold publication. Chưa tự động retention cho candidate/landing/snapshots.

Setup/replay: [Airflow runbook](../../infra/platform/airflow/README.md); xử lý fail/rerun/deadline: [incident runbook](../runbooks/direct-finance-airflow.md). Cảnh báo hiển thị local, không hứa gửi thông báo ra ngoài.

## Refactor application — PASS

- Tách Bronze/Silver/Gold, marts, ingestion, jobs, pipeline, quality, Trino client và exporter. Không đổi catalog/table/bucket/volume names, HMAC identity hay công thức Finance.
- 59 unit/reference tests, không skip; thêm 4 Spark integration tests chạy các hàm application thật trên fixture tổng hợp.
- Replay snapshot đã lưu qua toàn bộ các tầng: fact counts và FIN02 không đổi, Trino đối soát 60 rows, certified mart snapshot trước/sau replay không đổi. Đây là bài test candidate-only, không phải fresh extraction.
- Sau khi PostgreSQL nguồn được bật lại theo xác nhận người dùng, actual Airflow smoke test được chạy lại từ fresh extraction. DQ failure injection fail đúng validation sau khi extract/transform success; publish bị chặn. Recovery validate ở attempt 2, các task khác attempt 1, DAG success và alert được resolve.
- Wheel có SQL resource và không chứa monolith đã bỏ; cài vào virtualenv độc lập, import bằng Python -I không cần Spark/Airflow cho job CLI. Hash wheel/SQL được ghi trong evidence.
- Container runtime đã bỏ scripts khỏi PYTHONPATH; application không import helper từ deployment/verification scripts. Runtime image lock được cập nhật sau rebuild/restart.

Giới hạn tại mốc refactor: full-snapshot extraction, dimensions pre-go-live cohort, chưa có complete SCD2/as-of. Chặng 1 đã bổ sung observed-time/day-as-of như phần đầu báo cáo; Kafka/Superset, immutable production application image và load/HA/restore certification vẫn chưa có. Local source mount vẫn dùng cho phát triển; packaging proof không đồng nghĩa production deployment đã hoàn tất.

## Các lỗi thật đã xử lý

1. COMPOSE_PROJECT_NAME trong .env lấn át name trong YAML: runner dùng --project-name shopvn-readiness rõ ràng. Các container thử ban đầu có prefix shopvn-sources được xác minh thuộc spike rồi stop; dữ liệu/containers nguồn của người dùng không bị xóa.
2. Seed MERGE INSERT * không tương thích khi bảng đã thêm cột: đổi thành explicit insert column list; rerun sau schema evolution thành công.
3. PostgreSQL COPY boolean là t/f: Bronze giữ wire value, Silver chuyển sang boolean có kiểm tra accepted encodings; không dùng permissive parser để nuốt lỗi.
4. Có 403 order source discount lẻ phần VND. Vẫn phân bổ integer VND theo rank ổn định, phần dư dưới 1 VND được cộng vào rank 1 để giữ đúng precision nguồn DECIMAL(20,2). Không truncate source amount hoặc thay FIN02. Có thêm 3 regression cases; tổng hiện 23 unit/reference tests pass.
5. Airflow dùng virtualenv có psycopg nhưng Spark Python gốc không có: tách run artifact helpers thành module chỉ dùng standard library để publisher không kéo theo dependency orchestration.
6. Airflow 3.4 giữ archived retry attempts trong task_instance: finalizer chỉ xét working_set=true. Không được coi retry lịch sử là trạng thái hiện tại; đồng thời vẫn phải fail nếu task hiện hành upstream fail.
7. Deadline callback có thể đến sau khi một run đã fail/kết thúc sớm. Callback runtime giờ đối chiếu finished_at với deadline_time để không gán nhầm run ngắn thành chạy quá 30 phút; có regression tests cho trước/đúng/sau hạn và run chưa kết thúc.
8. Pendulum DateTime của Airflow khi kết hợp stdlib ZoneInfo và timedelta bị lệch thêm ngày trong runtime đã quan sát. Chuẩn hóa sang native UTC datetime trước phép tính ngày; 3 regression checks chạy ngay trong Airflow container. Scheduled inventory giữ business date theo logical cutoff của run, không lấy ngày capture trễ để đổi nhãn EOD.

## Chạy lại trên máy hiện tại

```powershell
.venv/Scripts/python.exe scripts/run_lakehouse_spike.py --keep-running
.venv/Scripts/python.exe scripts/run_direct_slice.py --keep-running
.venv/Scripts/python.exe scripts/render_finance_chart.py
.venv/Scripts/python.exe scripts/export_verification.py
.venv/Scripts/python.exe scripts/verify_airflow.py --business-date 2026-06-30
.venv/Scripts/python.exe scripts/export_airflow_verification.py
.venv/Scripts/python.exe tests/integration/verify_direct_replay.py
.venv/Scripts/python.exe scripts/export_refactor_verification.py
```

Direct runner mặc định extract snapshot mới. Muốn replay snapshot cũ, thêm --snapshot-id với ID trong runtime/direct/latest-snapshot.json; không sửa CSV/manifest để tạo số khớp. Nếu gặp mismatch, mart không được publish, xem runtime/direct/direct-result.json và direct-slice-report.json.

Hướng dẫn dựng từ đầu: [spike runbook](../../infra/platform/spike/README.md). Source containers phải đang sẵn sàng theo [README gốc](../../README.md); không dựng thêm bộ nguồn trùng port nếu đã có container được cung cấp đang chạy.

## Truy vấn và dừng

Trino UI: http://localhost:18080 (monitoring query, không phải dashboard Finance). Kết nối SQL client: jdbc:trino://localhost:18080/lakehouse/analytics, user readiness cho demo cục bộ. Query [direct Finance mart](../../sql/views/direct_finance.sql).

```powershell
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml stop
```

Lệnh stop giữ data/catalog volumes. Không dùng down -v hoặc xóa object files. Các service nguồn có project/network riêng không nằm trong lệnh dừng này.
