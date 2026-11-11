# Load, identity và DQ contracts

## Nguồn

| Nguồn/bảng | Extract ban đầu | Incremental sau đó | Identity / thay đổi |
|---|---|---|---|
| customers,products,orders,returns,vouchers,inventory | Full read-only snapshot | Full snapshot cho dataset hiện tại: không có updated_at đảm bảo | PK nguồn; source hash phát hiện update, observed_at không giả business update time |
| order_items | Full snapshot | Full snapshot cho đến khi chứng minh append-only | item_id; quantity=0 có thể là line cancel, phải bắt update |
| inventory_transactions,product_reviews | Full snapshot | Watermark PK kèm overlap chỉ khi log được xác nhận immutable; nếu không full snapshot | txn_id/review_id, conflict hash phải cảnh báo |
| SFTP CSV | Từng partner/ngày, MD5 trước parse | File manifest + checksum; cùng checksum skip/replay idempotent, checksum mới là replacement version | partner+external_order_id+shopvn_product_id unique trong observed data; conflict block, không tự thêm row number làm khóa |
| Logistics API | Batch <=50, tập order có shipment cần theo dõi | Refresh open shipments + revisit recent terminal according to retention policy | Source_order_id với current one-shipment/order contract; 404 khác transport failure |

Xóa nguồn: snapshot absence ban đầu chỉ là candidate_deleted, xác nhận snapshot complete trước khi tombstone. Không hard-delete facts làm mất audit. Nguồn hiện tại không có delete log; không suy deletion từ API fail/missing file.

## Medallion

Bronze giữ source snapshot versions, manifest, batch_id, observed_at, source hash; nguồn có PII chỉ cấp quyền ingestion/engineering hạn chế. Silver tách nguồn, type/check/dedup; quarantine được lưu riêng. Gold chỉ đọc Silver đã được DQ phê chuẩn, PIIs được loại/mã hóa phù hợp.

Rerun cùng input: cùng business keys/measure phải không đổi; loaded_at/run audit có thể khác, không dùng chúng để đánh giá equality nghiệp vụ. Backfill nhận start_date/end_date/as_of/source snapshot; không lấy current status rồi tuyên bố "as known in June".

## Dimensions / facts

| Nhóm | Chiến lược |
|---|---|
| dim_date/channel/carrier | Seed/calendar, stable keys, unknown member 0 |
| dim_customer/product | SCD2 observed-time; unchanged hash không tạo version; một writer, stable key map; cùng snapshot rerun không đổi version |
| dim_voucher/geography/warehouse | Type1/append master mapping cho thuộc tính hiện quan sát; không khẳng định lịch sử thuộc tính chưa có nguồn |
| fact_order/sales_line/return/shipment | MERGE identity nguồn, refresh measures/status từ snapshot được phê chuẩn; no fact-to-fact BI joins |
| fact_inventory_transaction/review | MERGE ID cho initial/replay, detect source mutation; business timestamps unreliable phải mang flag |
| fact_inventory_daily_snapshot | BR32: MERGE product/warehouse/day cho actual EOD snapshot từ inventory_go_live_date, có cutoff hợp lệ; backfill June blocked, không tạo history simulated cho MVP |
| marts | Rebuild affected business partitions từ cùng publication snapshot set, sau reconciliation |

Bronze partition theo ingestion day; fact lớn có thể dùng month(order_date) khi dữ liệu/query đủ căn cứ. Dimension nhỏ và bảng nhỏ ban đầu unpartitioned. Không partition customer_id/product_id. Compaction và snapshot retention là maintenance, không xóa file MinIO thủ công.

## Cổng chất lượng

| Check | Severity / hành động |
|---|---|
| Checksum missing/mismatch | Reject file, alert, incomplete partner/day |
| Duplicate identity khác payload | Block scope, giữ quarantine; không chọn arbitrary latest |
| Required key null/orphan | Quarantine; block publication nếu vi phạm contract accepted-count/reconciliation |
| Source count = accepted + rejected | Exact; fail block scope |
| Source amount vs target theo cùng eligibility | Exact Decimal; fail block scope |
| discount>subtotal | Exclude revenue theo BR27, count visible; không block toàn dataset vì lỗi đã được mô tả |
| quantity=0 | Audit, loại units/sales lines hợp lệ |
| Source discount có phần lẻ VND | Giữ DECIMAL(20,2); integer allocation theo rank ổn định, fractional tail <1 VND vào rank đầu. Tổng allocation phải khớp source header exact, không truncate FIN02 |
| phone null/shipping=0/cost>base | Nullable hoặc warning theo đề bài |
| Marketplace Decimal-vs-source delta -1 VND và khớp float source | Formula warning theo evidence P03; preserve reported net, source-target reconciliation vẫn exact. Mọi mức lệch mới phải review/block |
| SCD overlaps/multiple current | Block dimension và dependent facts |
| Inventory business-time unreliable | Chặn historical EOD/stockout historical claims, không chặn current stock |

Theo BR31, certified Direct Finance view chọn net_customer_amount (FIN02); merchandise_less_order_refund_proxy luôn có nhãn proxy và không dùng thay headline. Theo BR32, inventory_go_live_date lấy từ cấu hình/metadata lúc pipeline bắt đầu quan sát; không tự đặt bằng source.last_updated hoặc ngày chạy backfill. DQ reject mọi actual daily snapshot trước mốc đó.

## Late events và Kafka

MVP simulator tách data_origin. Event_id ổn định; cùng ID/cùng payload là replay, khác payload là conflict vào DLQ. Không bật watermark drop để mất dữ liệu tài chính khi chưa chốt lateness contract; Bronze giữ mọi event, Silver durable dedup/merge và batch reconciliation. Đây là at-least-once + idempotent processing, chưa chứng nhận end-to-end exactly-once.
