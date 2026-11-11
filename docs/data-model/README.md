# Data model hiện hành

Baseline logic sau thảo luận với nhóm: **Medallion + star schema**, Iceberg trên MinIO. [Business rules v1](../business/business-rules-v1.md) và [KPI contracts](../business/kpi-contracts-v1.md) điều khiển KPI. Đã bổ sung [data dictionary](data-dictionary-v1.md), [load contracts](load-contracts-v1.md) và [ERD Mermaid](gold-erd.mmd) sau profiling nguồn. Xem [findings](../profiling/findings.md): inventory history và refund composition còn giới hạn cần xử lý trước publication.

## Phân lớp

- Bronze: dữ liệu từng nguồn gần nguyên bản, có ingest metadata và khả năng replay; quyền truy cập hạn chế vì có thể chứa PII.
- Silver: chuẩn hóa riêng direct/marketplace/logistics/inventory; chỉ tạo sales_lines_conformed sau khi xử lý riêng từng nguồn.
- Gold: dimensions dùng chung, core facts và certified marts. Không công bố PII trực tiếp.

## Dimensions dự kiến

| Bảng | Grain / ghi chú |
|---|---|
| dim_date | Một ngày; role-playing order/shipment/return/settlement/snapshot |
| dim_customer | Một version customer, SCD2 từ go-live, customer_ref dùng HMAC ổn định |
| dim_product | Một version product; giữ giá bán trong fact, không thay bằng giá catalog hiện tại |
| dim_channel | Một kênh nguồn; mapping web/app/direct và marketplace rõ ràng |
| dim_voucher | Một voucher; chỉ công bố thuộc tính thực có hoặc có quy tắc dẫn xuất |
| dim_carrier | Một carrier |
| dim_geography | Một cặp tỉnh/huyện chuẩn hóa phục vụ shipment |
| dim_warehouse | Một warehouse được nguồn xác nhận; chưa suy ra lịch sử thiếu căn cứ |

Unknown member key 0, có phân biệt lý do thiếu trong audit nếu cần. SCD2 dùng khoảng nửa mở [valid_from, valid_to), chỉ một record current; không tạo lịch sử giả trước go-live.

## Facts dự kiến

| Bảng | Grain / giới hạn |
|---|---|
| fact_order | Một direct order, header amounts; không nhân bản measure bằng line join |
| fact_sales_line | Một sales line theo nguồn, hợp nhất direct và marketplace sau conformance; marketplace line identity cần profiling |
| fact_shipment | Một shipment theo nguồn; source hiện cung cấp snapshot theo order, chưa có lịch sử từng attempt |
| fact_return | Một return request, giữ trạng thái hiện quan sát; refund không có product detail/actual payout timestamp |
| fact_inventory_transaction | Một stock movement; warehouse lịch sử chưa có nguồn trực tiếp |
| fact_inventory_daily_snapshot | Một product tại cutoff cuối ngày, thêm warehouse chỉ trong phạm vi có căn cứ; semi-additive theo thời gian |
| fact_product_review | Một review direct; không suy rộng sang marketplace |

Chưa tạo settlement fact độc lập khi nguồn chỉ có T+15 trên sales line. Dùng scheduled settlement date và mart lịch settlement. Không nối fact-to-fact tùy ý trong BI; drill-across qua conformed dimensions và aggregate về cùng grain.

Merchandise revenue trước refund theo sản phẩm và revenue sau refund cấp order là hai phạm vi khác nhau: không tạo product-level refund ước tính như số chính thức. Marts phải ghi scope/method rõ ràng.

## Phần chưa được nguồn hỗ trợ

- Actual payout, bank confirmation, actual refund timestamp.
- Returned product/quantity.
- Reserved/available stock nếu chưa xác minh transaction semantics.
- Historical warehouse của transaction, historical cost trước go-live.
- Tier tại thời điểm mua trong lịch sử trước go-live.

## Bản thiết kế cũ

[Danh mục legacy](legacy/README.md) giữ nguyên hai bản model cũ và ERD. Chúng có Delta, source-specific facts, allocation và quan hệ khác baseline này; chỉ dùng để tham khảo lịch sử.
