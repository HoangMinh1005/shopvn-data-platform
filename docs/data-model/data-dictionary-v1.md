# Gold data dictionary v1

Thiết kế cột dựa trên [schema đã profiling](../profiling/observed-profile.json), [findings](../profiling/findings.md) và [KPI contracts](../business/kpi-contracts-v1.md). Đây là implementation contract dự kiến, chưa phải DDL đã nạp toàn bộ dữ liệu thật. Những bảng/cột bị hạn chế được ghi rõ.

Áp dụng rule version 1.1 (BR31/BR32): net_customer_amount là Direct Finance primary measure; daily inventory chỉ có actual snapshots từ go-live, không backfill June 2026.

Runtime đầu tiên đã triển khai subset phục vụ direct Finance, xem [scope và evidence](../verification/README.md). Dictionary này vẫn là model đích đầy đủ; các field/dimension ngoài scope không được xem là đã deploy.

## Quy ước type và audit

`!` = NOT NULL, `?` = nullable. `SK` = BIGINT surrogate key, unknown = 0. `DK` = INT YYYYMMDD, unknown = 0. `M` = DECIMAL(20,2), currency_code = VND. `TS` = TIMESTAMP UTC (Spark) với timezone contract; Trino đọc theo UTC và đổi local khi lấy business day. Không dùng FLOAT cho tiền. Với source created_at/last_updated không có timezone: Silver giữ TIMESTAMP_NTZ + source_timezone chưa xác nhận; cột Gold UTC tương ứng chưa được publish cho đến khi contract timezone đủ căn cứ, không tự áp server UTC.

Mọi Gold fact có cột audit dưới đây, ngoài các cột trong từng bảng:

| Cột | Type | Mapping / quy tắc |
|---|---|---|
| source_system | STRING! | postgres/shopvn, marketplace/{partner}, logistics/api |
| data_origin | STRING! | source hoặc simulated; tách publication |
| source_record_hash | STRING! | SHA256 canonical payload, không chứa PII trực tiếp |
| observed_at | TS! | Thời điểm snapshot nguồn được thu thập |
| loaded_at | TS! | Thời điểm ghi analytics; không dùng thay business time |
| run_id | STRING! | ID lần nạp; không tham gia business identity |
| dq_status | STRING! | valid/warning/quarantined; quarantined không vào certified marts |

Dimension SK được cấp qua key map bền vững cho natural key + version, một writer dimension, không tạo lại SK khi rerun. Fact keys dùng SHA256 identity theo nguồn; source primary keys vẫn được giữ để trace. Key-map implementation cần được kiểm thử trong vertical slice, không dùng monotonically_increasing_id làm business surrogate ổn định.

## dim_date — một ngày

| Cột | Type | Mapping |
|---|---|---|
| date_key | DK! PK | YYYYMMDD từ calendar spine |
| full_date | DATE? | NULL ở unknown member |
| year, month, day, quarter | INT? | Thành phần lịch |
| week_of_year, day_of_week | INT? | ISO week, Monday=1 |
| is_weekend | BOOLEAN? | day_of_week 6/7 |
| is_flash_sale | BOOLEAN? | Calendar cấu hình: 2026-06-07, 2026-06-15 |

## dim_customer — một version customer

| Cột | Type | Mapping |
|---|---|---|
| customer_key | SK! PK | Key-map (customer_ref, version) |
| customer_ref | STRING! NK | HMAC-SHA256 customer_id + source, secret ngoài Git |
| loyalty_tier | STRING? | customers.loyalty_tier |
| gender | STRING? | customers.gender, accepted values |
| age_band | STRING? | Dẫn xuất DOB tại observed date; không lưu DOB gốc trong Gold |
| customer_city | STRING? | customers.city; không thay recipient province |
| registered_at | TS? | customers.created_at theo source-timezone contract, có cờ timestamp limitations |
| valid_from | TS! | Thời điểm quan sát version, không backdate về tháng 6 |
| valid_to | TS? | NULL cho current, nửa mở [from,to) |
| is_current | BOOLEAN! | Một record current / customer_ref |
| attribute_hash | STRING! | Các thuộc tính được theo dõi, loại audit timestamp |
| history_reliable | BOOLEAN! | Chỉ đáng tin trong phạm vi đã quan sát; initial snapshot không chứng minh tier lịch sử |

Không lưu full_name, phone, email hoặc địa chỉ chi tiết ở Gold. Historical fact trước initial observation có thể map fallback version ban đầu để giữ customer identity, nhưng phải ghi dimension_match_basis=initial_snapshot_fallback và chỉ cho phép current/initial-tier analysis. Không diễn giải fallback thành tier lúc mua.

## dim_product — một version product

| Cột | Type | Mapping |
|---|---|---|
| product_key | SK! PK | Key-map (source product_id, version) |
| product_id | BIGINT! NK | products.product_id |
| sku, product_name | STRING? | products.sku, name |
| category, subcategory, brand | STRING? | Từng cột nguồn; subcategory không tự sửa nghĩa thành taxonomy chuẩn |
| base_price, cost_price | M? | Giá catalog/cost quan sát, không phải lịch sử trước go-live |
| weight_gram | INT? | products.weight_gram |
| is_active | BOOLEAN? | products.is_active hiện quan sát |
| is_catalog_loss_making | BOOLEAN? | cost_price > base_price |
| valid_from, valid_to | TS! / TS? | Observed SCD2 window |
| is_current, history_reliable | BOOLEAN! | Current version và độ tin cậy lịch sử |
| attribute_hash | STRING! | Category/brand/cost/base/is_active và thuộc tính đã chọn |

## dim_channel — một kênh

| Cột | Type | Mapping |
|---|---|---|
| channel_key | SK! PK | Seed mapping ổn định |
| channel_code | STRING! NK | direct_web, direct_app, lazada, shopee, tiktok |
| channel_name, channel_group | STRING! | Tên và direct/marketplace |
| is_marketplace | BOOLEAN! | Theo channel_group |

## dim_voucher — một voucher được quan sát

| Cột | Type | Mapping |
|---|---|---|
| voucher_key | SK! PK | Persistent key-map voucher_code |
| voucher_code | STRING! NK | vouchers.voucher_code |
| voucher_type | STRING? | vouchers.type |
| voucher_value | DECIMAL(20,4)? | vouchers.value, percent/value theo type |
| min_order_value, max_discount | M? | Các cột nguồn |
| campaign_id | STRING? | vouchers.campaign_id |
| valid_from_date, valid_to_date | DATE? | Validity voucher, không phải SCD2 observed window |
| max_usage | BIGINT? | vouchers.max_usage |

usage_count nguồn chỉ có current cumulative value; audit Silver, không thay usage trong kỳ. sponsor_type/campaign_name không có nguồn nên chưa đưa vào contract.

## dim_carrier / dim_geography / dim_warehouse

| Bảng.cột | Type | Mapping |
|---|---|---|
| dim_carrier.carrier_key | SK! PK | Stable map API carrier |
| dim_carrier.carrier_code | STRING! NK | ghn/ghtk/unknown |
| dim_carrier.carrier_name | STRING! | Seed tên; không tự suy service_type |
| dim_geography.geography_key | SK! PK | Map normalized(province,district) |
| dim_geography.province_name | STRING? | API recipient_province |
| dim_geography.district_name | STRING? | API recipient_district |
| dim_geography.geography_ref | STRING! NK | Hash cặp tên đã chuẩn hóa; chưa có mã địa giới authoritative |
| dim_warehouse.warehouse_key | SK! PK | Map inventory.warehouse_id |
| dim_warehouse.warehouse_id | STRING! NK | inventory.warehouse_id |

Không tự tạo warehouse name/type/province hoặc government geography codes khi chưa có master data.

## fact_order — một direct order hiện quan sát

| Cột | Type | Mapping |
|---|---|---|
| order_key | STRING! PK | SHA256(postgres, order_id) |
| source_order_id | STRING! NK | orders.order_id |
| customer_key, channel_key, voucher_key | SK! FK | Customer fallback có nhãn, source channel, voucher hoặc unknown 0 |
| order_date_key | DK! FK | orders.order_date |
| order_status, payment_status, payment_method | STRING! | Từng field nguồn |
| dimension_match_basis | STRING! | observed_day_asof / initial_snapshot_fallback / unknown; lookup customer theo business key + order_date, không exact purchase-time |
| currency_code | STRING! | VND |
| subtotal_amount | M! | orders.subtotal, đã sau item discounts |
| header_discount_amount | M! | orders.discount_amount |
| customer_shipping_fee | M! | orders.shipping_fee |
| source_total_amount | M! | orders.total_amount, giữ kể cả bug clamp |
| merchandise_amount | M! | subtotal-header discount, không tự clamp |
| refunded_order_amount | M! | SUM refunded return theo order; default 0 |
| net_customer_amount | M! | source_total_amount-refunded_order_amount; Direct Finance KPI chính FIN02 theo BR31 |
| merchandise_less_order_refund_proxy | M! | merchandise-refunded_order_amount, giới hạn P01 |
| is_revenue_eligible | BOOLEAN! | BR02 + BR27; giữ excluded order trong fact để audit |
| has_excess_discount | BOOLEAN! | discount > subtotal |
| zero_shipping_anomaly | BOOLEAN! | shipping=0; source voucher không mô tả freeship nên không tự nhận biết |

Không có payment_date trong nguồn; chưa tạo payment_date_key. Không có order-level marketplace header đáng tin thì không đưa vào bảng này.

## fact_sales_line — một source sales line

| Cột | Type | Mapping |
|---|---|---|
| sales_line_key | STRING! PK | SHA256(source_system, source_line_id) |
| source_order_id | STRING! | orders.order_id hoặc external_order_id, không cross-join hai hệ thống |
| source_line_id | STRING! NK theo nguồn | Direct item_id; marketplace canonical(partner, external_order_id, shopvn_product_id), reject conflict |
| product_key, customer_key, channel_key, voucher_key | SK! FK | Conformed lookups; marketplace customer/voucher = 0 khi không có nguồn |
| dimension_match_basis | STRING! | Customer basis như fact_order; runtime có customer_dimension_match_basis và product_dimension_match_basis riêng cho hai lookup |
| order_date_key | DK! FK | Source order_date |
| scheduled_settlement_date_key | DK! FK | Marketplace settlement_date, direct = 0 |
| currency_code | STRING! | VND |
| line_status | STRING! | Header status direct hoặc marketplace status |
| quantity | BIGINT! | order_items.quantity / quantity_sold |
| unit_sale_price | M! | Direct unit_price-discount_per_item; marketplace sale_price |
| item_discount_amount | M? | Direct discount_per_item*quantity; marketplace NULL vì semantics platform_discount chưa đủ |
| line_merchandise_before_header_discount | M! | Direct total_price; marketplace sale_price*quantity |
| allocated_header_discount_amount | M? | Direct BR09; NULL khi header lỗi không phân bổ được và bị loại revenue. Integer VND + fractional residual nguồn vào rank đầu để tổng khớp chính xác; marketplace 0 khi không có seller header discount |
| net_merchandise_before_refund | M! | Line merchandise-allocated discount |
| commission_rate | DECIMAL(10,6)? | Marketplace commission_rate, direct NULL |
| partner_reported_net_amount | M? | Marketplace net_revenue nguyên nguồn, direct NULL |
| implied_partner_deduction | M? | Merchandise-partner net, gồm rounding residual |
| source_platform_discount | M? | Giữ nguyên platform_discount, chưa giả định per-unit hay line total |
| net_amount_basis | STRING! | merchandise_net_before_refund / partner_reported_after_commission; certified view chọn cột rõ |
| formula_delta_vnd | M? | net_revenue nguồn - Decimal floor kiểm tra, không sửa nguồn |
| is_sales_eligible | BOOLEAN! | Order eligibility direct + quantity>0; marketplace completed + quantity>0 + valid source |

Không có product refund/shipping allocation chính thức. Refund chỉ có cấp order. marketplace platform_discount không đủ cơ sở dựng giá gốc trước discount.

## fact_shipment — một order-level shipment snapshot theo API hiện tại

| Cột | Type | Mapping |
|---|---|---|
| shipment_key | STRING! PK | SHA256(logistics, source_order_id); API hiện một shipment response/order |
| source_order_id | STRING! NK | API.order_id |
| tracking_code | STRING? | API.tracking_code, chỉ thuộc tính; chưa dùng làm primary identity |
| carrier_key, geography_key | SK! FK | Carrier và recipient province/district |
| order_date_key | DK! FK | Lookup Silver direct order |
| shipped_date_key, delivered_date_key, estimated_delivery_date_key | DK! FK | Ngày local từ timestamps/ETA, unknown 0 |
| shipped_at, delivered_at | TS? | API shipped_at, actual_delivery_date |
| estimated_delivery_date | DATE? | API date-only |
| shipment_status, failure_reason | STRING! / STRING? | API.status / failure_reason |
| delivery_attempts | INT? | Snapshot count |
| actual_shipping_fee | M? | API fee vận chuyển; không đồng nhất shipping thu khách |
| delivery_duration_hours | DECIMAL(16,4)? | Delivered hợp lệ, timestamp difference |
| is_late | BOOLEAN? | Delivered đủ dates; unknown NULL |

API 404/error ghi ops.source_fetch_result, không tạo fake failed shipment. warehouse_key không có căn cứ từ API, chưa gán từ kho product hiện tại.

## fact_return — một return request

| Cột | Type | Mapping |
|---|---|---|
| return_key | STRING! PK | SHA256(postgres, return_id) |
| source_return_id | BIGINT! NK | returns.return_id |
| source_order_id | STRING! | returns.order_id |
| customer_key, channel_key | SK! FK | Lookup direct order |
| order_date_key | DK! FK | orders.order_date |
| request_date_key | DK! FK | returns.created_at theo timezone contract; có timestamp-generation warning |
| request_created_at | TS? | Source created_at, không dùng làm actual refund time |
| return_status, return_reason | STRING! / STRING? | returns.status / reason |
| source_refund_amount | M! | returns.refund_amount, có thể tồn tại ngay khi pending |
| refunded_order_amount | M! | Amount chỉ khi status refunded, khác thì 0 |

Không tạo product_key, returned_quantity, approved_refund_amount hoặc refunded_at khi nguồn không có.

## fact_inventory_transaction — một movement được quan sát

| Cột | Type | Mapping |
|---|---|---|
| inventory_transaction_key | STRING! PK | SHA256(postgres, txn_id) |
| source_transaction_id | BIGINT! NK | inventory_transactions.txn_id |
| product_key | SK! FK | product_id |
| source_order_id | STRING? | order_id nullable |
| transaction_type | STRING! | reserve/release/sold/restock |
| quantity_change | BIGINT! | qty_change giữ dấu nguồn |
| source_created_at | TS! | created_at nguồn, có dấu hiệu ingestion timestamp |
| business_effective_at | TS? | NULL đến khi có business time có căn cứ |
| business_date_key | DK! FK | Unknown 0 nếu business_effective_at chưa xác định |
| business_time_reliable | BOOLEAN! | False với dataset đang profiling |

Không tự gán warehouse/cost lịch sử. Transaction vẫn audit được nhưng chưa dùng backfill stock theo business day.

## fact_inventory_daily_snapshot — product/kho quan sát/ngày cutoff

| Cột | Type | Mapping |
|---|---|---|
| snapshot_date_key | DK! FK / PK thành phần | Business EOD thực đã thu thập, không trước inventory_go_live_date; June backfill bị chặn theo BR32 |
| product_key, warehouse_key | SK! FK / PK thành phần | Tại snapshot quan sát; inventory hiện 1:1 product |
| stock_qty | BIGINT! | inventory.stock_qty |
| source_last_updated | TS? | inventory.last_updated |
| snapshot_date | DATE! | Business EOD ngày D; cutoff là 00:00 ngày D+1 theo Việt Nam |
| snapshot_cutoff_at | TS! | Cutoff 00:00 VN, lưu UTC; không phải lúc Spark kết thúc |
| observed_at | TS! | Thời điểm transaction snapshot nguồn thật được quan sát |
| capture_lag_seconds | DOUBLE! | observed_at-cutoff, chỉ chấp nhận 0 <= lag < 60 |
| snapshot_method | STRING! | observed_eod_cutoff_window; là quan sát gần cutoff, không exact stock tại 00:00 |
| policy_version | STRING! | INV-CUTOFF-v1; go-live/window được persist immutable tại ops.inventory_policy |

Nếu chỉ capture giữa ngày, giữ Silver và công bố riêng analytics.current_stock (grain product/warehouse), không gọi là EOD. Current stock có observed_at, source_last_updated TIMESTAMP_NTZ, snapshot lineage và source data_origin. First accepted EOD không bị ghi đè khi replay; payload khác phải block. Không cộng stock qua nhiều ngày. reserved/available/in_transit/value chưa có nguồn đủ. Ngoại lệ June stock history vẫn giữ nguyên.

## fact_product_review — một direct review

| Cột | Type | Mapping |
|---|---|---|
| review_key | STRING! PK | SHA256(postgres, review_id) |
| source_review_id | BIGINT! NK | review_id |
| source_order_id | STRING! | order_id |
| product_key, customer_key, channel_key | SK! FK | Product/customer/order channel |
| review_date_key | DK! FK | created_at theo source contract; không khẳng định business time nếu timestamp sinh dữ liệu |
| rating | INT! | Nằm trong [1,5], invalid quarantine |

Comment có thể chứa PII nên không vào Gold. verified_purchase/sentiment_score chưa có nguồn hoặc model, chưa thêm như dữ liệu thật.

## Ops / publication contracts

| Bảng | Identity / trường chính |
|---|---|
| ops.pipeline_run | run_id PK; pipeline, start/end, business range, status, counts, code_version |
| ops.data_quality_result | run_id+check_id+scope; severity, observed, expected, passed, affected_count |
| ops.reconciliation_result | run_id+measure+source+period; source_total,target_total,delta,passed |
| ops.file_ingestion_manifest | partner+business_date+filename+checksum; state, source_rows, rejected_rows, ingest time |
| ops.source_fetch_result | source+request identity+observed_at; success/not_found/transport_failed, HTTP code; không raw payload/PII trong dashboard |
| ops.publication | publication_id PK; data_as_of,published_at,completeness,code_version,rule_version; table snapshot IDs để cross-table consistency |

Iceberg atomicity chỉ ở từng table: dùng publication manifest trỏ snapshot IDs đã kiểm tra để BI không đọc trộn một nửa run cũ và một nửa run mới.
