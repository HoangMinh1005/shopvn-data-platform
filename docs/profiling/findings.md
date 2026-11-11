# Kết quả profiling nguồn thực tế

Ngày kiểm tra: 2026-11-11. Kết nối read-only bằng shopvn_reader, không sửa nguồn. Sử dụng ba container có sẵn của người dùng (PostgreSQL/API từ image local shopvn-*:v1.1.0; SFTP từ image nguồn đang chạy). Kết quả phản ánh dataset được nạp trên máy này, không tự suy rộng sang mọi bản image.

Chi tiết aggregate/schema được xuất bằng `scripts/profile_sources.py` tại [observed-profile.json](observed-profile.json). Report không chứa bản ghi customer, địa chỉ người nhận, review text hay credential. API chỉ kiểm tra mẫu phân tầng 50 order, không phải đối soát toàn bộ 211806 order.

## PostgreSQL

| Bảng | Số dòng |
|---|---:|
| customers | 50000 |
| products | 2000 |
| orders | 211806 |
| order_items | 444142 |
| vouchers | 200 |
| inventory | 2000 |
| inventory_transactions | 439722 |
| returns | 4215 |
| product_reviews | 41226 |

- Tài khoản ingestion chỉ được SELECT trên 9 bảng; phiên profiling bật default_transaction_read_only.
- SUM(line.total_price) khớp subtotal trên tất cả order; không có order thiếu line.
- Công thức total_amount khớp GREATEST(0, subtotal + shipping_fee - discount_amount).
- 622 order discount > subtotal, 4420 line quantity = 0, 4160 order shipping_fee = 0.
- Không phát hiện orphan ở các quan hệ order-customer, line-order, line-product, return-order đã kiểm tra.
- Các trạng thái thực tế không đồng nghĩa với nhau: delivered/pending = 27473, delivered/failed = 2440. Giữ tiêu chí eligibility đã duyệt và hiển thị exclusion count.
- Chưa có updated_at trên orders/customers/products/returns. PK/created_at không đủ bắt update trạng thái: MVP dùng full snapshot cho các bảng mutable trong phạm vi dataset, append/merge theo PK cho log bất biến sau khi xác minh.
- Database timezone là UTC nhưng nhiều timestamp là WITHOUT TIME ZONE. Server setting không chứng minh múi giờ nghiệp vụ của giá trị; cần source contract ghi rõ giả định.

## Ba vấn đề cần ghi vào contract

### P01 — Refund bao gồm hoặc có thể bao gồm shipping

1705 refunded order có refund bằng total_amount và shipping > 0; 1698 refunded order có refund vượt merchandise amount hợp lệ. Không có refund component breakdown. Trừ toàn bộ refund khỏi merchandise tạo ra một proxy và có thể âm cho đơn hoàn toàn hoàn tiền.

Giữ phép tính đã duyệt để đối chiếu, nhưng đặt nhãn **merchandise less order refund — proxy**; không công bố là net merchandise revenue chính xác. Chỉ tiêu net customer amount = total_amount - refunded_amount ở cấp order có căn cứ đối soát tốt hơn. Chính sách tách shipping refund còn mở; không tự chia hoặc clamp về 0.

Kết quả xử lý đã được người dùng duyệt: BR31 chọn net customer amount làm Direct Finance KPI chính, giữ merchandise-less-refund là proxy. Không yêu cầu tách shipping refund để publish FIN02; source limitation vẫn giữ nguyên.

### P02 — Không đủ căn cứ dựng EOD tháng 6

Snapshot inventory.last_updated nằm trong 2026-08-06 07:30:51; 439722 transaction đều sau anchor, từ 07:31:14 đến 07:31:15 cùng ngày. Đây có dấu hiệu là timestamp lúc sinh/nạp dữ liệu, không phải ngày business movement tháng 6.

Không dùng trực tiếp phép dựng ngược trong guide để tuyên bố tồn cuối từng ngày tháng 6. BR32 đã được người dùng duyệt: MVP lưu current stock và actual snapshots từ go-live, chặn June backfill, không thêm history simulated cho MVP. Không suy ngày movement từ order_date như lịch sử thật.

### P03 — Lệch công thức marketplace cần giữ provenance

14 dòng thấp hơn công thức Decimal floor lý tưởng đúng 1 VND; toàn bộ 63103 dòng khớp công thức tính bằng float của nguồn. Không thay net_revenue nguồn bằng số tính lại để làm đối soát khớp giả. Publish phải kiểm tra reconciliation source-to-target chính xác, đồng thời công khai formula warning theo contract DQ; chênh lệch vượt phạm vi đã giải thích phải chặn.

## Marketplace

- 85 file hợp lệ MD5, 3 missing, 2 checksum mismatch bị từ chối trước parse.
- 63103 dòng trong các file hợp lệ; completed 55481, returned 5707, disputed 1915.
- `(partner, external_order_id, shopvn_product_id)` và bản có thêm seller_sku không trùng trong sample toàn bộ file hợp lệ hiện có. Khóa vẫn cần duplicate/conflict check ở mỗi lần nạp.
- Không có unknown product, quantity <= 0, partner mismatch hay settlement khác T+15 trong các dòng đã parse.
- Thiếu/corrupt files đồng nghĩa số marketplace cả tháng chưa complete; không điền doanh thu thiếu thành 0.

## API

Mẫu 50 order (10 mỗi order status): một batch request HTTP 200, 38 shipment, 12 not_found; không suy tỷ lệ 24% này thành tỷ lệ thiếu toàn dataset. Đã xác nhận các field carrier, tracking, dates, attempts, failure reason, province/district. Hai lần gọi lặp một order cùng payload và tracking_code trong lần kiểm tra này; đây chỉ là sample, chưa chứng minh tracking_code unique/toàn lịch sử. Fact giữ source_order_id làm identity theo contract một shipment response/order hiện tại.

## Số đối soát theo chính sách v1

Eligible direct orders: app 44204, web 66707, loại excessive discount. Merchandise less refunded-order amount (proxy): app 655826440223 VND, web 990058684141 VND. Net customer amount sau refund: app 656908135223 VND, web 991693969141 VND.

Đây là snapshot theo trạng thái nguồn hiện tại, không phải phiên bản dữ liệu "đã biết tại tháng 6". Xem [KPI contracts](../business/kpi-contracts-v1.md) trước khi sử dụng.
