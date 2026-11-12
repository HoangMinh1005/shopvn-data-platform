# S2 — SFTP và Finance marketplace

Phạm vi theo [kế hoạch MVP](../architecture/mvp-implementation-plan.md): file contract/manifest, Bronze/Silver, conformed sales lines, Finance/settlement marts, Airflow, DQ/source reconciliation và local monitoring. Không triển khai API/Kafka/Superset trong S2.

## Contract và cách dùng số liệu

Marketplaces cấu hình tập trung tại `contracts/marketplace.py`: Lazada/Shopee/TikTok, stable channel keys 3/4/5; direct web/app vẫn 1/2. Thêm partner cùng schema bằng thay registry này và kiểm thử/review contract, không viết lại pipeline. Không tự đổi keys đã cấp.

Một file: `/marketplace/incoming/<partner>/<partner>_YYYYMMDD.csv` + `.md5`. File date = order_date; settlement_date = order_date+15. Required schema theo COLUMNS; extra columns được ignore có audit. Missing required columns, malformed rows/type/overflow/unknown status/date mismatch bị block file. Source primary line identity là `(partner, external_order_id, shopvn_product_id)`; duplicate cùng payload vào duplicate audit/quarantine, khác payload block. Không ghép external order với PostgreSQL order.

Tiền dùng Decimal: FIN04 = sale_price*quantity_sold, FIN05 = net_revenue nguồn, FIN06 = FIN04-FIN05. Chỉ completed eligible; returned/disputed giữ riêng, không suy refund. Không trừ platform_discount hoặc commission lần hai. P03 chỉ nhận delta -1 VND khi khớp float formula nguồn, ngoài ra block; reported net không bị sửa.

FIN07 là lịch settlement T+15, **không phải tiền thực nhận**. FIN10 so sánh merchandise trước refund/commission (direct FIN01 + marketplace FIN04). FIN02 direct không bị gộp với FIN05. Order counts/AOV tính tại grain partner/day, không average daily ratios. Product lookup theo observed-day/initial fallback; customer marketplace = unknown 0.

## Chuẩn bị và thực thi

Nguồn chỉ đọc. Host key phải được pin trước authentication, không dùng AutoAddPolicy cho connector. Khi container được phục hồi và key thay đổi, kiểm tra public key bằng kênh tin cậy trước khi xoay pin; không tự overwrite pin cũ.

```powershell
.venv/Scripts/python.exe scripts/bootstrap_marketplace.py
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml build airflow-api
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml up -d
```

Bootstrap chỉ bổ sung pin còn thiếu vào `.env` từ đúng container local `sftp`; không sửa source/credential. Môi trường remote cần chủ nguồn cung cấp fingerprint tin cậy. Giữ riêng host key pin và password ngoài Git; `.env.example` chỉ có placeholder.

DAG `shopvn_marketplace_finance` chạy 04:00 VN, CronTriggerTimetable/catchup=False/run_immediately=False, max_active_runs=1, Spark pool một slot/writer advisory lock chung S1. Để trống ngày → D−1 theo logical date VN; June demo phải trigger rõ:

```json
{"start_date":"2026-06-01","end_date":"2026-06-30","partners":["lazada","shopee","tiktok"]}
```

```text
initialize → capture_source → prepare_candidate → validate_candidate
           → publish_finance → verify_and_finish → finalize (ALL_DONE)
```

Mỗi task tối đa 2 retries, delay 15 giây, timeout 15 phút; finalizer không retry và phải fail nếu upstream fail. Capture retry tái dùng completed manifest của cùng run; run mới capture mới. Scheduled capture chờ missing/checksum-missing tối đa 30 giây bằng polling bounded, rồi tiếp tục phần hợp lệ. Manual historical capture không chờ. Không có verified file trong request thì workflow fail, không báo data mới bằng dữ liệu cũ.

CLI kiểm tra source riêng:

```powershell
.venv/Scripts/python.exe -m shopvn.ingestion.sftp --start 2026-06-01 --end 2026-06-30
```

Source frozen June không đáp ứng scheduled November freshness. Partial historical demo thành công không làm 08:00 deadline hiện tại thành đạt. Không sửa lịch hoặc xóa audit để làm dashboard xanh.

## Artifact, version và publication

- Capture ở `runtime/marketplace/landing/<capture_id>`: streaming download <=1MiB/chunk vào `.partial`; MD5 verified trước parse; SHA256 được giữ để replay. Raw files restricted/ignored Git, không load cả file vào RAM.
- Parser stdlib/Decimal tạo baseline độc lập theo file. Row quarantine JSONL chỉ giữ row number/reason; invalid schema/payload block file. Với file parse được: source rows = accepted + rejected, duplicate-same-payload thuộc rejected. Một file có blocking row không được publish các dòng còn lại như complete file.
- Bronze `lakehouse.bronze.marketplace_sales_line` giữ verified content versions, dedup file_version_id khi replay; Silver active rows typed và source-lineage.
- Mỗi run có candidate tables riêng: `staging.marketplace_{registry,silver,facts,mart}_<run_key>` và snapshot IDs. Registry ghi cả last attempt và last accepted file version theo partner/day.
- Replacement hợp lệ thay **toàn bộ đóng góp file**, kể cả dòng biến mất ở bản mới. Missing/corrupt replacement không xóa last accepted version: giữ số cũ với `retained_unverified`, không nói đã kiểm chứng lại. Registry nguồn được lấy từ snapshot mà certified mart tham chiếu, không lấy staging latest tùy ý.
- Trino đọc đúng candidate snapshot để đối soát merchandise/net/orders/units/AOV với baseline file; direct FIN01/FIN02/order counts còn được đối chiếu bằng SQL PostgreSQL READ ONLY độc lập. Nếu direct source đổi, chạy direct Finance fresh để restate trước, không nới tolerance.
- Publisher từ chối candidate thay đổi, reference snapshots thay đổi hoặc parent publication đã advance. Retry cùng publication là no-op. Partial Silver/fact write trước certified commit có thể replay từ candidate; certified mart commit cuối.
- Same-publication no-op phải đúng source_candidate_snapshot_id. `prepared_at` là lúc tạo candidate; `published_at` được gắn ở bước write/publish, không dùng thời gian chuẩn bị thay giờ công bố. Iceberg committed_at là metadata thời điểm commit; post-check completion hiển thị riêng.
- Silver và shared fact có thể đã đổi nếu publish bị dừng giữa các table. Đây **không phải multi-table atomic transaction**. BI chỉ đọc certified mart/views; không đọc Gold tùy ý như cùng một publication. Writer lock phải được giữ khi sửa/publish.

Giữ runtime/candidate registry snapshots tới khi không còn publication nào tham chiếu; chưa tự expire/delete artifacts. Backup phải gồm catalog, object files, registry snapshots và local verified captures/audit tương ứng.

## Bảng phục vụ và query an toàn

Shared fact: `lakehouse.gold.fact_sales_line` chứa source-aware keys. Direct writer chỉ MERGE direct rows; global uniqueness vẫn được kiểm tra, direct row counts không đếm marketplace. `dim_channel` là shared seed/upsert, không bị reset về direct-only.

Certified table `lakehouse.analytics.mart_commerce_finance_daily` có grain `(business_date, channel, report_type)`. Sales dùng report_date=business_date; scheduled_settlement dùng report_date=business_date+15. Không sum cả hai report_type, vì reported net sẽ bị cộng hai lần. Các view ở [marketplace_finance.sql](../../sql/views/marketplace_finance.sql) tách rõ hai report types.

```sql
SELECT channel,sum(comparable_merchandise),sum(partner_reported_net)
FROM lakehouse.analytics.marketplace_finance_daily GROUP BY 1;

SELECT business_date,channel,coverage_status,marketplace_day_complete
FROM lakehouse.analytics.marketplace_finance_daily
WHERE coverage_status <> 'verified';
```

Missing/rejected chưa từng có accepted version: amounts/counts NULL. Verified empty file hoặc không có completed lines: amounts/counts 0, AOV NULL. Không tự fill missing thành 0. `marketplace_day_complete` chỉ true khi đủ registry partners đều verified cho ngày đó; dữ liệu direct và marketplace có source_observed_at riêng. Tổng toàn tháng vẫn partial nếu thiếu bất kỳ partner/day.

## Giám sát và xử lý sự cố

`ops.marketplace_run`/`ops.marketplace_file` nằm trong platform metadata DB, tách Finance direct audit. `/api/status` và Grafana có requested file counts, failed state, coverage, reported net, DQ delta/gate, row exclusions/formula warnings và current D−1 deadline. Source SFTP health là TCP/SSH banner reachability, không thay authentication/checksum/DQ.

Grafana file provisioning dùng updateIntervalSeconds=30 để polling, tránh bỏ lỡ file-change events qua Docker bind mounts trên Windows/WSL. Nếu config có 31 panels nhưng API vẫn thấy bản cũ, kiểm tra provider/reload rồi query lại API; không dùng JSON trên disk thay bằng chứng provision thực tế. Cơ chế polling theo [tài liệu Grafana](https://grafana.com/docs/grafana/latest/administration/provisioning/).

1. **Missing/late:** xem partner/day và last accepted version; bounded wait hết vẫn missing → partial. Khi nguồn tới, trigger đúng phạm vi ngày/partner bằng run mới. Không sửa manifest cũ hoặc copy file ngày khác. Late arrival nguồn bake sẵn chỉ được test bằng fixture/mock clock có nhãn.
2. **Checksum/schema/type/conflict:** file không được accepted; giữ raw partial/verified/quarantine để debug, không tự tính lại MD5 rồi gọi source hợp lệ. Yêu cầu chủ nguồn sửa; khi có file mới, capture version mới và rerun scope.
3. **DQ/reconciliation:** candidate-only bị block, certified Finance không đổi. Xem `runtime/marketplace/runs/<key>/validation.json`; không bật publish thủ công bằng sửa passed=true. DQ flags phục vụ smoke test chỉ ép fail, không bypass checks.
4. **Fail giữa publish:** không xóa catalog/object files. Retry cùng task/run để dùng đúng candidate và registry. Nếu reference/parent đã đổi thì tạo run mới/reprepare/revalidate; không ép snapshot cũ đè publication mới.
5. **Nguồn PostgreSQL hoặc SFTP down:** khôi phục đúng nguồn/volume/pin trước; không reseed DB hoặc rotate HMAC. Health green không có nghĩa run lỗi cũ tự được xóa, coverage đã đủ hoặc SLA đã đạt.

Cảnh báo incomplete của dữ liệu June và daily deadline hiện tại có thể tiếp tục hiện đúng chủ đích; không cần toàn bộ alerts bằng 0 để chứng minh recovery của một run test. Email/Slack chưa triển khai, không tự gửi ra ngoài.

SFTP capture không phải transaction snapshot nguyên tử toàn tháng hoặc đồng thời với PostgreSQL: giữ capture start/completion và observed_at theo file, source version và reference table snapshots. Không diễn giải data_as_of của batch như mọi nguồn cùng thay đổi tại một thời điểm tuyệt đối.

## Kiểm thử và giới hạn bàn giao

```powershell
.venv/Scripts/python.exe -m pytest -q
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml run --rm --no-deps spark --master "local[2]" --driver-memory 1g /workspace/tests/integration/marketplace_spark.py
.venv/Scripts/python.exe tests/integration/verify_marketplace_airflow.py
.venv/Scripts/python.exe tests/integration/verify_stage2_replays.py
```

Chỉ chạy một writer job tại một thời điểm. Spark fixtures dùng namespace riêng `shopvn.stage2_tests`, không sửa nguồn hoặc actual stock history. Failure injection tạo DAG runs test, nên không chạy verifier đồng thời với manual job khác. S2-Txx traceability/kết quả thực nằm trong evidence S2 khi xuất; chưa có Superset, API ingestion, Kafka, HA hoặc full MVP acceptance trong stage này.

Bàn giao ngày 12/11: [evidence aggregate](../verification/stage2.json) và [traceability S2-T01…T12](../verification/README.md). 122 unit/29 Spark cases, 3 fresh-process replays, actual marketplace failure/recovery/final smoke và direct regression đã pass. Review độc lập và quyết định đóng S2 thuộc nhóm; không tự suy phê duyệt S3–S7 từ việc S2 đã có implementation.
