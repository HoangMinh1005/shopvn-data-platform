# Kế hoạch triển khai MVP và checklist kiểm soát thay đổi

Phiên bản kế hoạch: **1.0**, cập nhật **12/11/2026**, ngày/giờ nghiệp vụ theo Asia/Ho_Chi_Minh. Người quản lý kế hoạch: trưởng nhóm; người phê duyệt đổi phạm vi/thứ tự: người dùng đại diện nhóm. Tên thành viên và deadline thực tế chưa được cung cấp.

## 1. Điểm chốt phải đọc trước khi làm

| Mục | Giá trị hiện hành |
|---|---|
| Stage đã hoàn thành | **S1 — PostgreSQL, temporal model và actual inventory** |
| Stage tiếp theo đã xác nhận | **S2 — SFTP và Finance marketplace** |
| Trạng thái S2 | Sẵn sàng lập trình; **chưa triển khai pipeline** |
| Công việc đầu tiên | **S2-01 — Chốt contract file, phạm vi publication và bộ số đối soát** |
| Kafka/simulator | Không thuộc S2; đặt ở S5 trong đề xuất dưới đây |
| Phạm vi được thực hiện trong lượt tạo tài liệu | Kế hoạch, checklist và đồng bộ tài liệu; không tự triển khai S2 |
| Trạng thái phê duyệt lộ trình | S1 có bằng chứng; S2 đã được xác nhận trong hội thoại; **thứ tự/đánh số S3–S7 là đề xuất chờ duyệt** |

Repo chưa lưu đủ bản lộ trình 7 chặng cũ trong hội thoại. Vì vậy S3–S7 bên dưới là **phương án chuẩn hóa để nhóm review**, không được gọi là bản cũ đã được duyệt. Không tự đổi S2 sang Kafka vì một thành phần khác cũng chưa hoàn thành. Muốn triển khai stage còn là đề xuất phải xác nhận với người dùng trước.

Tài liệu này là nguồn duy nhất về **thứ tự stage, phạm vi và checklist tiến độ**. [Yêu cầu gốc](../sources/01_project_requirements.md), [nghiệp vụ v1.1](../business/business-rules-v1.md), [KPI contracts](../business/kpi-contracts-v1.md) và quyết định đã duyệt vẫn quyết định tính đúng nghiệp vụ. Kế hoạch không tự sửa hoặc miễn yêu cầu đề bài.

Từ nay dùng `S1…S7` cho các stage MVP. Các mục profiling/contract/spike trong [readiness](implementation-readiness.md) là bước **tiền triển khai P1…P3**, không phải S1…S3.

## 2. Hiện trạng và các ranh giới không được vượt

### 2.1 Mốc đã kiểm chứng

- [x] S1 có [evidence aggregate](../verification/stage1.json): 84 unit tests, 19 Spark cases, 3 clock checks trong runtime Airflow; 3 Finance process replays và DQ failure/recovery.
- [x] PostgreSQL đã nạp đủ 9 bảng; customer/product SCD2 theo thời điểm quan sát, day-as-of và initial fallback có nhãn.
- [x] Current stock và EOD thật đã đối soát; EOD ngày 11/11 được chụp 00:00:02,153998 ngày 12/11 VN, trong cửa sổ đã duyệt.
- [x] Người dùng phục hồi nguồn ngày 12/11; kiểm tra đọc lại thấy đúng volume PostgreSQL cũ, số dòng 9 bảng và Finance/tồn kho khớp. API health và SFTP authentication đã kiểm tra. Đây **không phải** một lần chạy lại toàn bộ DAG sau restore.
- [x] Core lakehouse, Airflow và local monitoring đã có implementation; Kafka, SFTP/API pipelines và Superset chưa triển khai.

Counts tham chiếu PostgreSQL: orders 211806, order_items 444142, returns 4215, customers 50000, products 2000, vouchers 200, inventory 2000, inventory_transactions 439722, product_reviews 41226. FIN02 direct = **1648602104364.00 VND**; current stock tại mốc kiểm tra = **5053681** đơn vị.

Các số trên là baseline của một snapshot, không phải hằng số bắt mọi dữ liệu tương lai phải bằng. Khi nguồn đổi hợp lệ, tạo baseline nguồn mới có provenance và giải thích delta; không chỉnh source để ép khớp.

### 2.2 Bất biến cho mọi stage

- Nguồn chỉ được đọc. Không sửa file SFTP, DB nguồn hoặc API để tạo ca thử; dùng fixtures/nguồn test cô lập.
- FIN02 chỉ là net customer amount direct sau refunded returns. Marketplace FIN05 giữ reported net riêng; so sánh cùng basis dùng FIN10, không cộng FIN02 + FIN05 thành một headline mơ hồ.
- Không trừ platform discount hoặc commission lần hai khỏi marketplace net_revenue. Không suy refund từ trạng thái returned/disputed.
- Không dựng June stock hoặc loyalty/cost history không có nguồn. SCD2 observed-time không phải thời điểm thay đổi nghiệp vụ chính xác.
- INV-CUTOFF-v1 giữ cửa sổ EOD [00:00:00,00:01:00) ngày sau; late/current observation không bù EOD thiếu. Không đổi go-live để backfill.
- `data_origin=simulated` tách khỏi `source`, kể cả facts, publication, KPI và dashboard.
- DQ/source reconciliation phải pass trước certified publication. Dữ liệu thiếu không được biến thành số 0 hoặc nhãn complete.
- Atomic replace của một Iceberg table không phải transaction nguyên tử của tất cả Gold tables.
- Giữ HMAC key, secrets, source identity và volumes. Không commit raw data/PII/credential; không tự rotate key.

## 3. Bản đồ stage và trách nhiệm

| Stage | Phạm vi chính | Phụ thuộc | Trạng thái | Ước lượng còn lại |
|---|---|---|---|---|
| S1 | PostgreSQL 9 bảng, SCD2/day-as-of, actual inventory | Core/refactor/ops đã có | Hoàn thành trong scope ghi nhận | 0; sửa regression nếu phát sinh |
| **S2** | **SFTP + Finance marketplace** | S1 | **Đã xác nhận; chưa bắt đầu code** | 4–6 ngày công |
| S3 | Logistics API + Operations | S1; làm sau S2 trong đề xuất này | Đề xuất chờ duyệt | 3–5 ngày công |
| S4 | Customer/Marketing + Product/Inventory marts còn thiếu | S1, S2; S3 cho return/logistics liên quan | Đề xuất chờ duyệt | 4–6 ngày công |
| S5 | Simulator + Kafka + Spark Structured Streaming | Batch contracts ổn định sau S2–S4 | Đề xuất chờ duyệt | 4–6 ngày công |
| S6 | Superset + dashboard nghiệp vụ | Certified marts; S5 cho trang simulated | Đề xuất chờ duyệt | 2–4 ngày công |
| S7 | Hardening local, nghiệm thu và bàn giao MVP | Các stage thuộc scope được duyệt đã pass | Đề xuất chờ duyệt | 3–5 ngày công |

Ước lượng là effort tham khảo, không phải cam kết ngày hoàn thành. Chưa có năng lực máy, lịch thành viên và ngày bảo vệ nên chưa gán deadline lịch. S2–S7 khoảng 20–32 ngày công nếu giữ nguyên scope đề xuất, chưa cộng thời gian chờ duyệt/nguồn/sửa lỗi lớn. Review lại sau khi S2 có số đo thực tế.

Phân công theo vai trò, không giả định nhóm đã có người phụ trách:

- Trưởng nhóm: duyệt contract kỹ thuật, giữ kế hoạch, tổng hợp bằng chứng và yêu cầu quyết định nghiệp vụ.
- Data Engineering: ingestion, Bronze/Silver/Gold, marts và reconciliation.
- Platform: Compose, DAG, connections/secrets, service health, checkpoint và deployment.
- DQ/Reviewer: fixture kỳ vọng độc lập, negative tests, replay và kiểm tra evidence.
- BI: semantic dataset, KPI labels, dashboard và đối soát với Trino.

Một người có thể kiêm nhiệm; trước mỗi stage phải điền tên người thực hiện và người review. Không dùng phân vai trong tài liệu để tuyên bố đã có review độc lập.

## 4. S1 — Checklist đóng stage

Tham chiếu [runbook S1](../runbooks/stage1-postgres-inventory.md) và [verification](../verification/README.md).

- [x] PostgreSQL 9 bảng read-only; không export contact PII/review comment.
- [x] Direct Bronze/Silver/Gold, FIN02 và source reconciliation đã pass.
- [x] SCD2 thay đổi/không đổi, replay, stale apply, overlap, same-clock conflict và day-grain fact joins đã kiểm thử.
- [x] Warehouse/current stock và actual EOD có cutoff/observed time/lag; chặn June backfill.
- [x] DAG, retry/finalizer, audit và local monitoring có kiểm chứng.
- [x] Ghi giới hạn: transactions/reviews mới ở Bronze/Silver; chưa có full event-time history, immutable production deployment hay full MVP certification.

Không yêu cầu hoàn thiện Kafka, Superset, mọi Gold mart hoặc HA để mở S2. Regression của S1 phát hiện trong S2 phải sửa và chạy lại đúng bộ test bị ảnh hưởng.

## 5. S2 — SFTP và Finance marketplace: kế hoạch chi tiết

### 5.1 Đầu vào, đầu ra và ngoài phạm vi

Đầu vào: SFTP của Lazada/Shopee/TikTok, CSV + MD5; product dimension/direct Finance đã có; [load contracts](../data-model/load-contracts-v1.md) và profiling marketplace.

Đầu ra: ingestion có manifest phiên bản file, Bronze/Silver marketplace, marketplace sales lines trong model conformed, certified Finance marketplace/settlement/comparable merchandise, DAG + monitoring + evidence.

KPI chính: FIN04/FIN05/FIN06/FIN07, FIN10 và FIN11/FIN12 trên cùng basis hợp lệ. FIN02 direct phải không đổi. Customer spend/voucher analytics đầy đủ nằm ở S4, không tự mở rộng S2 thành toàn bộ UC1.

Ngoài S2: Kafka/simulator, Logistics API ingestion, Superset, actual bank settlement, marketplace customer identity/refund detail không có nguồn, CDC/Debezium.

### 5.2 Điều kiện mở S2

- [x] S1 có evidence và nguồn đã được người dùng khôi phục/kiểm tra đọc.
- [ ] Tạo nhánh S2 từ baseline S1 đã review; kiểm tra tình trạng push/merge với người dùng, không tự merge.
- [ ] Điền người thực hiện/reviewer và xác nhận năng lực máy; giữ một Spark writer local, không tranh cutoff inventory 00:00 VN.
- [ ] Xác minh lại SFTP host key theo kênh tin cậy và quyền đọc, không tự bỏ qua key mismatch. Host key của container phục hồi đã đổi so với profiling cũ.
- [ ] Điền phạm vi partner/ngày demo và ngày báo cáo; không coi data tháng 6 là freshness tháng 11.

### 5.3 Các hạng mục theo thứ tự

#### S2-01 — Contract và baseline đối soát — DE + reviewer

- [ ] Chốt schema, required/optional columns, type tiền Decimal, accepted statuses, quy tắc filename/partner/order_date/settlement_date. Cột mới không ảnh hưởng semantics được ignore có audit; missing/type đổi bị phát hiện, không cast âm thầm.
- [ ] Khóa dòng canonical `(partner, external_order_id, shopvn_product_id)`; cùng key/same payload và khác payload có policy rõ. Không dùng row number làm identity.
- [ ] Lập independent baseline theo partner/day cho counts, completed units, FIN04, FIN05, status exclusions; lưu checksum của input và formula warnings.
- [ ] Xác nhận expected-file calendar và partial publication: thiếu partner/ngày vẫn được công bố phần hợp lệ có nhãn; không báo all-channel complete.

Profiling ngày 11/11 ghi 90 file kỳ vọng, 85 MD5-valid, 3 missing, 2 checksum mismatch; 63103 rows trong file hợp lệ, completed 55481. Sau restore mới kiểm tra được listing 87 CSV, chưa chạy lại toàn bộ MD5. **S2 phải tái xác minh**, không coi số profiling là kết quả test mới.

#### S2-02 — SFTP ingestion và file manifest — DE + Platform

- [ ] Download theo chunks vào vùng tạm; kiểm tra MD5 trên đúng bytes trước parse, lưu thêm SHA256 cho provenance. Không đọc toàn bộ file vào RAM.
- [ ] File thiếu/checksum thiếu/checksum sai/download dở có trạng thái riêng, lỗi rõ và retry phù hợp; file lỗi không vào accepted Bronze/Silver.
- [ ] File identity gồm partner + business date + filename; content version theo checksum. Giữ expected/observed checksum, bytes, observed_at, version, run và row/rejection counts khi đã parse.
- [ ] Chỉ đánh dấu verified sau khi file local hoàn tất; retry tái dùng verified artifact, không nhầm partial download là file hợp lệ.

#### S2-03 — Bronze và quarantine — DE

- [ ] Giữ immutable source-file versions và lineage file/checksum/row; không append trùng khi cùng input chạy lại.
- [ ] Required key null, invalid numeric/date/schema và conflict có quarantine/block policy; có source = accepted + rejected reconciliation.
- [ ] Tách missing-file coverage khỏi rejected-row counts; không đếm file chưa parse như 0-row valid file.

#### S2-04 — Silver marketplace — DE + reviewer

- [ ] Normalize partner/status/date/product mapping; source IDs giữ theo nguồn. Unknown product phải được phát hiện; không tự tạo product nguồn giả.
- [ ] `sale_price*quantity_sold` là merchandise; giữ nguyên `net_revenue`, `platform_discount`, `commission_rate`; completed eligible, returned/disputed tách riêng.
- [ ] Source-target amount reconciliation exact Decimal. P03 có 14 dòng lệch Decimal floor lý tưởng 1 VND nhưng khớp float formula nguồn ở baseline cũ: gắn warning có bằng chứng, không sửa reported net hoặc nới tolerance cho mọi sai lệch.
- [ ] File replacement có cơ chế active version; nếu bản mới bỏ một dòng, current accepted dataset phải bỏ đóng góp của dòng cũ trong phạm vi file, vẫn giữ audit. MERGE-only không được để sót dòng cũ.

#### S2-05 — Chuẩn bị conformed keys và helper đa nguồn — DE + reviewer

- [ ] `merge_fact` hiện hardcode `source_system=postgres/shopvn`: tham số hóa/chuẩn hóa source identity trước khi ghi marketplace, giữ tương thích direct và tách data_origin.
- [ ] `build_dimensions` hiện thay dim_channel bằng seed direct-only: đổi sang stable shared seed/upsert, không xóa marketplace channels khi Finance direct chạy sau.
- [ ] `validate_gold_counts` hiện đối soát toàn bảng fact_sales_line với direct source: chuyển kiểm tra direct về đúng source scope, đồng thời giữ uniqueness toàn bộ business keys.
- [ ] Date/product lookups không advance/backdate SCD bằng file tháng 6; giữ observed/fallback labels. Marketplace không có customer/voucher hợp lệ dùng unknown 0, không nối external_order_id sang orders PostgreSQL.

Ba điểm code trên đã được đọc trong baseline hiện hành; chúng là **việc phải làm trong S2**, chưa được sửa khi lập kế hoạch. Nếu đổi sang một model đích khác thay vì conformed fact đã chọn, phải qua kiểm soát thay đổi, không tự né regression bằng một fact trùng lặp.

#### S2-06 — Gold và Finance marts — DE

- [ ] Build marketplace sales-line contribution với key có source namespace, product/channel/date/settlement dimensions và lineage.
- [ ] FIN04 merchandise, FIN05 reported net, FIN06 implied deduction và FIN07 settlement schedule có grain/eligibility rõ; không gọi T+15 là bank cash đã nhận.
- [ ] FIN10 dùng direct FIN01 + marketplace FIN04 trước refund/commission; không cộng FIN05 thêm lần nữa. FIN11 distinct order có source namespace, FIN12 tính lại tử/mẫu cùng scope, không average daily ratios.
- [ ] Không nhân measures khi join dimensions/facts; tháng thiếu file có coverage/partial label và tổng quan sát, không khẳng định tổng thực đầy đủ.

#### S2-07 — Validation gate và publication — DE + reviewer

- [ ] Candidate đối soát source counts/units/amount theo partner/day; direct contribution cũng có lineage/snapshot được kiểm tra, không đọc trộn các phiên bản tùy ý.
- [ ] Ghi input file versions, reference table snapshot IDs, data_as_of, published_at, completeness và rule version cho publication.
- [ ] Failed gate không thay certified output của scope đó. Một partner fail không xóa publication hợp lệ của partner khác; tổng liên nguồn phải phản ánh coverage.
- [ ] Restart trước/sau commit/publication không nhân dòng hoặc đánh dấu thành công giả; không tuyên bố multi-table exactly-once chỉ nhờ một Iceberg MERGE.

#### S2-08 — Airflow và backfill/rerun — Platform + DE

- [ ] DAG nhận partner/date range, catchup=False có lý do; dependency, retries, timeout và pool/writer lock dùng chung với S1.
- [ ] Late arrival có bounded wait/reschedule rồi partial run; targeted rerun cập nhật completeness và đúng file version, không treo vô hạn trước 08:00.
- [ ] Batch backfill June dùng files/provenance đúng kỳ; không apply lại Finance snapshot cũ hơn dimension watermark hoặc dựng inventory June.
- [ ] Finalizer không che upstream failure; phân biệt workflow hoàn thành xử lý phần hợp lệ với coverage complete và publication được phê chuẩn.

Nguồn file được bake sẵn; LATE trong failure manifest là tình huống thiết kế, không chứng minh delayed live arrival. Dùng fixtures/mock clock có ghi nhãn để kiểm thử; không sửa nguồn SFTP để giả là lỗi tự nhiên.

#### S2-09 — Monitoring và incident runbook — Platform + reviewer

- [ ] Dashboard thể hiện expected/verified/missing/rejected files, coverage partner/day, last observation/publication, reconciliation delta và excluded/formula-warning counts.
- [ ] Có alert local cho checksum mismatch, scope publication block và quá hạn thiếu file; manual June demo không làm daily freshness tháng hiện tại thành đạt.
- [ ] Runbook cho missing/late, checksum mismatch, replacement và fail-mid-run; hướng dẫn nhận diện scope, rerun đúng artifact và xác nhận sau phục hồi.
- [ ] Không gửi email/Slack nếu chưa có cấu hình người nhận và yêu cầu gửi đã được duyệt; local alert không được ghi là outbound notification đã test.

#### S2-10 — Nghiệm thu và bàn giao — reviewer + trưởng nhóm

- [ ] Hoàn thành ma trận test bên dưới, chạy lại regression S1 bị ảnh hưởng.
- [ ] Ghi evidence aggregate từ lệnh đã chạy; không commit file raw/PII/secrets. Evidence phân biệt source thật, fixture và phần chỉ thiết kế.
- [ ] Cập nhật data dictionary, load contract, code map, DAG/runbook, tiến độ tại tài liệu này; review diff và đề nghị người dùng duyệt đóng S2.

### 5.4 Ma trận kiểm thử bắt buộc S2

| ID | Ca thử | Kết quả phải chứng minh |
|---|---|---|
| S2-T01 | CSV đúng + MD5 đúng | Accepted counts/units/FIN04/FIN05 khớp baseline độc lập |
| S2-T02 | File thiếu; MD5 thiếu/sai | Coverage incomplete; file sai không được publish; nguồn hợp lệ khác không bị biến mất |
| S2-T03 | Input giống nhau qua 3 process mới | Active rows, business keys, count/sum không đổi; audit run có thể khác |
| S2-T04 | Cùng key khác payload | Conflict được phát hiện; không arbitrary latest hoặc double count |
| S2-T05 | File replacement bớt/thêm/sửa dòng | Active version phản ánh đúng cả dòng bị bỏ; audit bản cũ còn; unaffected scope không đổi |
| S2-T06 | Kill/fail sau download, transform, trước/sau publish | Retry không nhận file dở; failed validation giữ certified snapshot; không duplicate hoặc false success |
| S2-T07 | File tới muộn bằng fixture | Partial coverage → targeted rerun → đủ coverage, không nhân bản; ghi rõ simulated timing |
| S2-T08 | Extra column/missing column/type drift | Extra harmless theo policy; missing/type lỗi bị báo, không silently corrupt |
| S2-T09 | P03 formula delta và delta lạ | Giữ net nguồn; warning có provenance, delta ngoài contract bị block |
| S2-T10 | Direct chạy trước/sau marketplace | FIN02 direct không đổi; channel keys không mất; direct-only count gate không đếm marketplace |
| S2-T11 | Cùng order ID giữa hai nguồn; multi-line order | Không cross-join nguồn; order count/AOV và header measures không fanout |
| S2-T12 | Airflow retry/DQ fail/recovery + Trino | DAG/audit đúng, certified gate hoạt động, metrics phản ánh coverage thật |

### 5.5 Cổng đóng S2

- [ ] S2-01…S2-10 và S2-T01…S2-T12 có bằng chứng hoặc ngoại lệ cụ thể được duyệt, không chỉ có code.
- [ ] Actual SFTP pipeline chạy hết phạm vi June được chọn, đối soát nguồn/Spark/Trino; missing/corrupt không bị coi là complete.
- [ ] 3 fresh-process replays pass và ít nhất một actual Airflow failure/recovery pass; direct regression pass.
- [ ] Có mart Finance marketplace/cross-channel đúng basis, truy vấn được và có publication metadata.
- [ ] Review/handoff đã hoàn tất. Chưa đi tiếp S3 chỉ vì container SFTP chạy hoặc một file mẫu pass.

## 6. S3–S7 — Đề xuất chi tiết chờ duyệt thứ tự

Các checkbox sau đều chưa hoàn thành trong phạm vi stage đề xuất. Việc S1 đã có một phần nền tảng không đồng nghĩa cả stage sau đã pass.

### S3 — Logistics API và Operations

Đầu vào: direct orders + API. Đầu ra: fetch audit, Bronze/Silver shipments, dim_carrier/geography, fact_shipment và Operations marts. Dùng source_order_id theo one-shipment-per-order contract, không suy lịch sử từng attempt.

- [ ] S3-01: Chốt tập order cần fetch/refresh và timezone contract. Nếu business timezone chưa xác nhận, ghi giả định cần duyệt; chưa publish duration/late KPI như số chính xác khi contract chưa đủ.
- [ ] S3-02: Client batch <=50, rate limit tập trung; 429 theo retry_after, timeout retry 1/2/4s tối đa 3 lần, 500 retry một lần, 404 log/no retry, 400 sửa batch size. Không gọi API từ Spark executors.
- [ ] S3-03: Fetch success/not_found/transport_failed tách riêng; raw PII bị hạn chế, analytics không chứa tên/địa chỉ chi tiết/điện thoại người nhận.
- [ ] S3-04: LEFT JOIN direct orders; giữ carrier/geography unknown, không tạo shipment failed từ API failure hoặc thiếu record.
- [ ] S3-05: OPS01…OPS06 theo cohort/mẫu số trong KPI contracts, duration/ETA hợp lệ; OPS07/category return proxy chỉ publish sau khi chi tiết status còn pending được duyệt.
- [ ] S3-06: DAG, retry audit, refresh open shipments/revisit policy, completeness, source-to-target reconciliation và source error metrics.
- [ ] S3-07: Fixtures bao phủ từng HTTP failure, live sample/incremental coverage được ghi đúng; không coi mẫu 50 order là full API ingestion. Có scope full extract hoặc giới hạn còn lại được duyệt.
- [ ] Cổng đóng: 3 replay cùng accepted input đã lưu có count/sum/status không đổi, fail/recovery không làm mất bản hợp lệ, Trino đọc được mart và runbook API đã review. Không ép ba lần gọi API live có lỗi ngẫu nhiên thành cùng số liệu bằng cách xóa missing/failure audit.

### S4 — Hoàn thiện Customer/Marketing, Inventory/Product và model đích

Đầu vào: S1–S3 và certified sales data. Đầu ra: các marts UC1/UC3/UC4 còn thiếu; không hứa KPI nguồn không hỗ trợ.

- [ ] S4-01: dim_voucher/campaign mapping; customer spend ngày/tháng, top customer và campaign-associated performance theo CUS01/CUS02/CUS05, FIN08/FIN09; tên proxy và denominator rõ, không gọi discount-on-voucher là actual voucher subsidy khi thiếu semantics.
- [ ] S4-02: Loyalty segment và lifecycle CUS03/CUS04/CUS06; initial fallback, đủ 30/90 ngày quan sát và same-day repeat được kiểm tra, không invent inactive history.
- [ ] S4-03: Gold fact_product_review/CUS07 từ valid ratings; comment không export, direct-only/sample count rõ, không tạo marketplace customer/ratings.
- [ ] S4-04: Gold inventory transaction audit nếu nằm trong model nghiệm thu; giữ business_time_reliable=false và warehouse/business time unknown đúng nguồn, không dùng để dựng June stock.
- [ ] S4-05: Best/slow sellers, product cross-channel volume và loss-estimate views theo INV05/INV06; cùng grain/coverage, cost_as_observed có nhãn, không gọi net profit.
- [ ] S4-06: INV03 yêu cầu actual trailing 7-day sales cùng kỳ với current stock, có ngày 0 sales; không đem June velocity dự báo November. INV04 cần opening stock + receipts đủ căn cứ; thiếu nguồn phải là unavailable/ngoại lệ, không tráo days-of-stock thành sell-through.
- [ ] S4-07: Hoàn thiện các field/grains trong data dictionary có nguồn; phần thiếu được ghi exception riêng, không tạo cột giả để khớp ERD.
- [ ] Cổng đóng: mỗi KPI có fixture độc lập, SQL/source reconciliation, null/coverage tests, 3 replay checks và mapping yêu cầu→output/ngoại lệ rõ ràng.

### S5 — Simulator, Kafka và Spark Structured Streaming

Đây là mở rộng đã chọn cho định hướng dự án, **không phải S2** và không thay thế batch official publication. Chưa triển khai khi chưa xác nhận thứ tự S3–S7.

- [ ] S5-01: Chốt event schema version, event_id/type/time/produced_at/source/data_origin, aggregate key và update/order-version contract; giới hạn rate, duration, số event và seed để dừng/replay được.
- [ ] S5-02: Chọn một slice orders/order_items có lifecycle nhất quán; không phát ngẫu nhiên event gây orphan rồi bỏ qua mà không audit. Thay đổi tier/product dùng stream test cô lập nếu cần kiểm chứng SCD.
- [ ] S5-03: Pin Kafka image và Spark Kafka connector tương thích qua runtime test; single-node KRaft cho local, tách service streaming khỏi batch Airflow, đo RAM trước khi chạy đồng thời.
- [ ] S5-04: Bronze simulated lưu payload/offset/partition/event time và rejected events; Silver durable dedup/merge. Cùng event_id khác payload vào conflict/DLQ, không tự latest.
- [ ] S5-05: Checkpoint ổn định, replay/restart/late/out-of-order/backpressure tests; không drop late financial events chỉ để dashboard xanh. Semantics là at-least-once + idempotent, không claim end-to-end exactly-once khi chưa chứng minh.
- [ ] S5-06: Có lag/throughput/error metrics, controlled shutdown và batch reconciliation; official/source Finance và actual inventory không bị stream simulated cập nhật.
- [ ] Cổng đóng: bounded load run đúng contract, duplicates/conflicts/late và restart có evidence; source certified snapshots không đổi; demo simulated được gắn nhãn rõ.

### S6 — Superset và dashboard nghiệp vụ

Đầu vào: certified marts. Đầu ra: dashboard Finance/Operations/Customer/Product và trang simulated riêng. Có thể làm thiết kế giao diện sớm; không mở full stage hoặc công bố KPI trước mart gate.

- [ ] S6-01: Superset metadata tách riêng, image/config được kiểm chứng; read-only Trino account và quyền truy cập dataset tối thiểu, credentials không trong export.
- [ ] S6-02: Dataset theo grain đã duyệt; dùng certified publication, tránh fact-to-fact fanout, sum stock qua ngày hoặc average-of-ratios.
- [ ] S6-03: Finance tách FIN02 direct/FIN05 marketplace/FIN10 comparable; settlement dự kiến, proxy, completeness và exclusions luôn hiện.
- [ ] S6-04: Operations/Customer/Product có denominator, unknown/missing và giới hạn lịch sử; unavailable không thành 0 hoặc forecast giả.
- [ ] S6-05: Mọi dashboard có business date, data_as_of, published_at, source/simulated label và coverage; nguồn chỉ June không hiện freshness hiện tại đã đạt.
- [ ] S6-06: Query/dashboard totals đối soát lại Trino và published baseline; export versioned dashboard assets và hướng dẫn import trên môi trường mới.
- [ ] Cổng đóng: trả lời được các câu hỏi UC1–UC4 hoặc chỉ rõ ngoại lệ đã duyệt, không cần SSH; reviewer kiểm tra filter/cross-filter/tổng tiền và quyền xem.

### S7 — Hardening local, nghiệm thu và bàn giao MVP

Mục tiêu là MVP local tái lập được, không tự nâng scope thành hệ thống HA production.

- [ ] S7-01: Dựng từ checkout + secrets cung cấp riêng trên môi trường/volume test; pin image/JAR/package đã kiểm chứng, script bootstrap/deploy và checklist rollback không đụng nguồn thật.
- [ ] S7-02: CI chạy checks/unit/lint/build/Compose validation; bổ sung và chứng minh controlled test deployment workflow. Không gọi CI/CD hoàn chỉnh nếu chỉ build/validate mà chưa deploy thử.
- [ ] S7-03: End-to-end cả PostgreSQL/SFTP/API, 3 process replays, schema evolution, fail-mid-run và DQ publication gate; so sánh direct/marketplace/stock/shipment theo đúng scope.
- [ ] S7-04: Bounded load test volume x3 có số đo duration/RAM/files/lag; đo recovery từ lúc phát hiện tới dữ liệu đối soát lại. Mục tiêu trước 08:00 và phục hồi trong 2h chỉ ghi đạt khi có bằng chứng, không dùng fixed-clock unit test thay live SLA/load test.
- [ ] S7-05: Backup/restore thử vào volume/environment riêng: catalog + object storage nhất quán, ops/Superset metadata, manifest/checkpoints và HMAC key/secrets có bảo vệ. Restore xong query count/sum và xác minh identity; không kiểm thử bằng xóa volume đang dùng.
- [ ] S7-06: Chốt retention cho landing/candidates/logs/Iceberg snapshots; dry-run và recoverability trước khi bật job tự động. Mọi thao tác expire/delete vật chất phải có scope và phê duyệt phù hợp, không xóa object files thủ công.
- [ ] S7-07: Monitoring trả lời thời điểm hoàn thành/service health/reconciliation/coverage; runbook ít nhất 3 sự cố, alert local test được. Email/Slack là mục bổ sung cần chốt người nhận/quyền gửi; chưa gửi thì không đánh dấu đã hoàn tất outbound delivery.
- [ ] S7-08: ERD/architecture diagram đúng implementation; mapping UC→KPI→mart→test/evidence, danh sách exception, bài demo/thuyết trình và hướng dẫn vận hành/bàn giao.
- [ ] Cổng đóng: nhóm review evidence, người dùng duyệt các ngoại lệ và phạm vi MVP; yêu cầu đề bài còn thiếu phải công khai. Sự đồng ý của nhóm không tự thay thế chấp nhận ngoại lệ của người nghiệm thu đồ án.

HA nhiều node, Kubernetes, autoscaling, multi-region DR và production RBAC/hardening đầy đủ là backlog sau MVP trừ khi có quyết định mở rộng riêng. Không gọi single-node Compose là HA.

## 7. Ánh xạ yêu cầu và các điểm phải quyết định

| Yêu cầu | Stage phụ trách | Điều không được báo đạt giả |
|---|---|---|
| UC1 Finance | S1 direct, S2 marketplace, S4 customer/voucher, S6 hiển thị | Không trộn revenue basis hoặc bỏ partial coverage |
| UC2 Operations | S3, S6 | API not_found/error không đồng nghĩa delivery failed |
| UC3 Customer/Marketing | S1 observed SCD, S4, S6 | Không tạo loyalty history trước go-live, causal ROI hay inactive từ window 30 ngày |
| UC4 Inventory/Product | S1 actual stock, S2 cross-channel, S4, S6 | June stock/sell-through/forecast thiếu nguồn phải là ngoại lệ, không fabricated data |
| Ba nguồn, Airflow, DQ, PII, schema evolution | S1–S4 từng slice; S7 tích hợp | Connector health/sample test không thay full pipeline/reconciliation |
| Deadline, recovery, x3 load và backup/restore | Monitoring từ S1, chứng minh S7 | Test bằng mock clock hoặc manual June run không chứng minh daily freshness hiện tại |
| Streaming/BI theo định hướng nhóm | S5/S6, thứ tự đang đề xuất | Không đổi thứ tự S2; không gọi optional theo đề bài là đã tự được bỏ khỏi scope nhóm |

Các điểm còn mở, không tự coi đã duyệt:

- [ ] Xác nhận thứ tự/đánh số S3–S7 ở mục 3; nếu có bản lộ trình cũ đầy đủ thì đối chiếu trước khi chốt.
- [ ] Điền người thực hiện/reviewer, thời gian khả dụng và lịch bàn giao.
- [ ] OPS07: bộ return statuses actionable còn pending trong KPI contract; chốt trước publication liên quan, không chặn ingestion S2.
- [ ] Xác nhận timezone các source timestamps trước KPI phụ thuộc thời gian; không suy từ DB server timezone.
- [ ] Chốt tiêu chí grading/ngoại lệ cho June stock history, thiếu payout/refund detail, sell-through và freshness của frozen June sources.
- [ ] Chốt một kênh outbound email/Slack nếu nhóm yêu cầu; không tự gửi thông báo ra ngoài.

## 8. Checklist làm việc và thay đổi kế hoạch

### 8.1 Trước mỗi phiên triển khai

- [ ] Đọc mục 1 và stage mục tiêu; nêu rõ `Sx / task ID / input / output / ngoài phạm vi` trong cập nhật bắt đầu.
- [ ] Kiểm tra yêu cầu mới là hỏi/kiểm tra hay triển khai. Yêu cầu xem trạng thái không cho phép tự deploy stage kế tiếp.
- [ ] Đối chiếu phê duyệt: stage/task có được duyệt chưa? Không dùng nội dung còn là đề xuất như quyết định đã chốt.
- [ ] Kiểm tra Git worktree, nhánh và thay đổi của người dùng; giữ nguyên phần không thuộc công việc.
- [ ] Kiểm tra nguồn/platform, secrets và artifact cần dùng bằng cách đọc; source down không được tự reseed/replace.
- [ ] Ghi ngày nghiệp vụ, observed time, file/snapshot version và bộ số kỳ vọng; chọn fixture/namespace riêng cho negative tests.

### 8.2 Trong khi làm và trước khi đánh dấu xong

- [ ] Code bám đúng task ID, không mở thêm Kafka/Superset/nguồn khác cho tiện mà chưa xin đổi scope.
- [ ] Runtime logic nằm trong package, DAG chỉ điều phối; dùng contract hiện hành và không hardcode credentials.
- [ ] Test độc lập về count/amount/identity, replay, failure recovery và schema drift theo rủi ro; failure injection không tác động source thật.
- [ ] Không ghi `[x]` chỉ vì có file code, Compose up, unit pass hoặc lời khẳng định trong chat. Phải có output/evidence đúng loại gate.
- [ ] Ghi evidence path/command/result/scope/thời điểm; tách actual/fixture, partial/complete, historical proof/current health.
- [ ] Handoff ghi stage/task hoàn thành, task tiếp theo, phần chưa làm, giới hạn, commit/PR nếu có và trạng thái push; không tự push/merge.

Trạng thái dùng chung: **chưa bắt đầu → đang làm → chờ kiểm thử → chờ review → hoàn thành**. `Đề xuất/chờ duyệt` là trạng thái phạm vi, không phải hoàn thành. Bị chặn phải nêu dependency cụ thể, không tự nhảy stage hoặc đổi tiêu chí để làm xanh.

### 8.3 Khi muốn đổi thứ tự, scope, stack hoặc KPI

1. Giữ kế hoạch đang hiệu lực; tạo đề xuất `CR-xxx` với trước/sau, lý do/bằng chứng, dependency, chi phí và ảnh hưởng kiểm thử/backfill.
2. Đề xuất phải nói rõ là thay đổi thật hay chỉ sửa tài liệu sai. Không gọi một kế hoạch mới là "bản cũ đã thống nhất".
3. Xin người dùng duyệt trước khi mở stage khác/đổi hợp đồng. Chưa có câu trả lời thì tiếp tục phần không bị ảnh hưởng hoặc dừng đúng điểm thiếu quyết định.
4. Sau duyệt, cập nhật tài liệu này, business decision nếu cần, README/readiness/architecture links và checklist chịu ảnh hưởng trong cùng thay đổi.
5. Tăng plan version khi đổi scope/thứ tự; giữ lịch sử. Sửa typo hoặc bổ sung evidence không tự đổi business rule version/source contract version/inventory policy version.

Mẫu yêu cầu thay đổi:

```text
CR-ID / ngày / người đề xuất:
Plan version / stage-task hiện tại:
Trước → sau:
Lý do và bằng chứng:
Ảnh hưởng KPI, schema, backfill, tests, thời gian và rủi ro:
Phương án giữ nguyên / phương án thay đổi:
Người duyệt / quyết định / ngày hiệu lực:
Tài liệu, checklist và evidence cần cập nhật:
```

### 8.4 Biên bản hoàn thành stage

```text
Stage / plan version / commit hoặc PR:
Người thực hiện / reviewer:
Task IDs và input versions đã nghiệm thu:
Lệnh kiểm tra / kết quả / đường dẫn evidence:
Counts, amounts, reconciliation và completeness:
Replay/failure/schema-drift/regression results:
Ngoại lệ, pending decisions, rollback/rerun:
Người xác nhận đóng stage / thời điểm:
Stage tiếp theo đã được duyệt / task đầu tiên:
```

## 9. Lịch sử quyết định của kế hoạch

| ID | Ngày | Nội dung | Trạng thái |
|---|---|---|---|
| PL-001 | 12/11/2026 | Khôi phục định danh đúng **S2 = SFTP + Finance marketplace** theo người dùng nhắc lại; dòng "bước tiếp theo Kafka" ở readiness là ghi nhận sai, không phải tối ưu đã duyệt | Đã xác nhận |
| PL-002 | 12/11/2026 | Tạo nguồn kế hoạch thống nhất; tách P1–P3 tiền triển khai khỏi S1–S7 MVP; thêm checklist và kiểm soát CR | Theo yêu cầu tạo tài liệu |
| PL-003 | 12/11/2026 | Đề xuất S3 API, S4 marts còn thiếu, S5 streaming, S6 BI, S7 nghiệm thu | **Chờ người dùng duyệt thứ tự** |

Việc cần làm ngay sau tài liệu này, khi người dùng yêu cầu triển khai: **S2-01**, không phải dựng Kafka. Không cần chờ S3–S7 được chốt hết mới thực hiện phần S2 đã xác nhận.
