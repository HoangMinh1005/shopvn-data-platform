# Quy tắc nghiệp vụ v1

Trạng thái: **được người dùng duyệt ngày 2026-11-11 trong hội thoại dự án**. Đây là baseline báo cáo quản trị cho đồ án; không đại diện cho phê duyệt của phòng Finance ngoài dự án.

Nguồn tham chiếu: [project requirements](../sources/01_project_requirements.md), [dataset guide](../sources/02_dataset_guide.md). Các giả định phụ thuộc dữ liệu phải được kiểm tra bằng profiling trước khi lập trình Gold.

## Các quyết định đã duyệt

| ID | Chủ đề | Quyết định |
|---|---|---|
| BR01 | Doanh thu chính | Doanh thu hàng hóa sau giảm giá và refund; shipping và commission là measure riêng. Có thêm marketplace net sau commission. |
| BR02 | Direct eligible orders | `status IN ('delivered', 'returned')` và `payment_status IN ('paid', 'refunded')`; giữ doanh số gốc rồi trừ refund thực tế. Tổ hợp trạng thái bất thường phải được gắn cờ. |
| BR03 | Marketplace eligible lines | Chỉ `completed` vào doanh thu hoàn tất; `returned` và `disputed` theo dõi riêng. Không suy ra tiền hoàn từ trạng thái. |
| BR04 | Ngày doanh thu | Báo cáo cohort theo `order_date`, được điều chỉnh khi trạng thái/refund thay đổi; không gọi là ngày ghi nhận kế toán/ngày giao thành công. |
| BR05 | Shipping | Direct merchandise amount = `subtotal - discount_amount`; amount khách trả thêm `shipping_fee`. Đối soát với `total_amount`. |
| BR06 | Platform discount | `sale_price` đã sau platform discount. Không trừ thêm `platform_discount`; `net_revenue` đã sau commission. Chưa mặc định khoản platform discount là chi phí ShopVN. |
| BR07 | Settlement | Báo cáo lịch T+15; có thể dùng góc nhìn nhận tiền giả định của dataset nhưng phải ghi rõ không có bank/payout confirmation. |
| BR08 | Hai lớp discount | `order_items.total_price` đã trừ `discount_per_item`; header `discount_amount` trừ tiếp theo công thức nguồn. Kiểm tra SUM line = subtotal. |
| BR09 | Discount allocation | Phân bổ header discount theo tỷ trọng `total_price` các line có quantity dương. Làm tròn VND và xử lý phần dư theo thứ tự ổn định để khớp header. Nếu không có cơ sở phân bổ hợp lệ, gắn lỗi thay vì chia tùy ý. |
| BR10 | Voucher cost | Chưa đồng nhất toàn bộ header discount với voucher cost. Báo cáo "giảm giá trên đơn sử dụng voucher" đến khi xác nhận semantics nguồn. |
| BR11 | Refund eligibility | Chỉ `returns.status = 'refunded'` mới được trừ. Không trừ lần nữa từ `payment_status = 'refunded'`. Tổng hợp refund về order trước khi ghép với order. |
| BR12 | Refund date | Refund được gán về order date để tính doanh thu cohort đã điều chỉnh. Chưa có actual refund timestamp; không dùng request created_at làm ngày chi tiền. Số lịch sử được phép restate. |
| BR13 | Product refund | Không phân bổ refund xuống sản phẩm. Chưa có product-level net revenue sau refund hoặc product return rate chính xác. |
| BR14 | Campaign | Usage, khách hàng, merchandise amount, discount và revenue sau refund của nhóm đơn gắn campaign. Không tuyên bố causal uplift/ROI. |
| BR15 | Customer spend | Direct: eligible merchandise amount sau discount/refund chia số khách có đơn đủ điều kiện trong kỳ. Tính ngày/tháng độc lập; không lấy trung bình tỷ lệ ngày làm tỷ lệ tháng. |
| BR16 | Marketplace customers | Không đưa marketplace vào customer ranking, loyalty, repeat purchase khi chưa có định danh khách đáng tin cậy. |
| BR17 | Loyalty history | SCD2 từ go-live; history trước go-live chỉ phân tích theo tier hiện tại và phải ghi nhãn. Snapshot theo ngày chỉ cho biết trạng thái đã quan sát, không chứng minh thời điểm thay đổi chính xác trong ngày. |
| BR18 | Lifecycle | Repeat: eligible order thứ hai trong 30 ngày sau đơn đầu, chỉ đánh giá cohort đủ thời gian quan sát. Inactive: 90 ngày không mua; dataset 30 ngày chưa đủ bao phủ. |
| BR19 | Delivery success | `delivered / (delivered + failed + returned)` trên shipment có kết quả cuối theo snapshot đang biết. Tách in-flight; không suy ra lịch sử từng attempt. |
| BR20 | Missing shipment | Tách no shipment record và API fetch failure. Không coi HTTP 404/timeout/500 là giao thất bại. |
| BR21 | Delivery time / late | Chỉ delivered có timestamp hợp lệ: actual_delivery_date - shipped_at. ETA là ngày: trễ khi ngày giao thực tế sau ngày ETA theo giờ Việt Nam. |
| BR22 | Category return proxy | Có thể báo cáo tỷ lệ đơn có yêu cầu return trong các đơn chứa category; mẫu số là các đơn đủ điều kiện mua thuộc category. Một đơn có nhiều category tham gia nhiều nhóm; chỉ số không xác định category thực sự bị trả. Tên proxy phải thể hiện giới hạn. |
| BR23 | Inventory boundary | EOD theo Asia/Ho_Chi_Minh, dùng ranh giới đầu ngày kế tiếp. Snapshot và transaction dựng ngược phải cùng cutoff. |
| BR24 | Inventory semantics | Chỉ công bố stock_qty có căn cứ. Xác minh reserve/release/sold/restock trước khi suy ra available stock. Chưa dựng lịch sử theo warehouse nếu nguồn không chứng minh được warehouse của transaction. |
| BR25 | Loss estimate | So sánh giá bán thực tế sau discount với cost đang biết; marketplace thêm góc nhìn sau commission. Đây là ước tính biên hàng hóa, không phải lợi nhuận ròng/lịch sử giá vốn đầy đủ. |
| BR26 | Stockout | Days of stock = stock hiện tại / avg units sold 7 ngày có cả ngày không bán. Giữ flash sale và gắn cờ. Avg = 0 thì không có dự báo hữu hạn. Sell-through là KPI khác, cần tồn đầu kỳ và nhập hàng đủ căn cứ. |
| BR27 | Source anomalies | Discount > subtotal: loại revenue, đếm record loại. Quantity = 0: giữ audit, loại line bán hợp lệ. Shipping = 0: cảnh báo, vẫn nạp. Cost > base: vẫn nạp để phân tích. |
| BR28 | Incomplete publication | Công bố phần nguồn hợp lệ với nhãn thiếu partner/ngày; không điền 0. Reject checksum sai. DQ nghiêm trọng chặn publication phạm vi bị ảnh hưởng. |
| BR29 | Simulated data | Kafka simulator có `data_origin = simulated`, dùng dashboard demo riêng. Hợp nhất batch/stream chỉ khi có identity/reconciliation chống đếm trùng. |
| BR30 | Publication cutoff | Trước 08:00, dữ liệu đến hết D-1 theo giờ Việt Nam. Có data_as_of, published_at và completeness từng nguồn. Batch là bản công bố chính thức, streaming là tạm thời. |

## Kiểm soát thay đổi

Không thay đổi ID quyết định cũ để che lịch sử. Ghi decision mới với lý do, người duyệt, ngày hiệu lực, KPI bị ảnh hưởng và nhu cầu backfill. Phân biệt làm rõ kỹ thuật với thay đổi định nghĩa nghiệp vụ.

## Ví dụ đối soát

Marketplace: sale_price = 100000, quantity_sold = 2, commission_rate = 0.08. Merchandise amount sau platform discount = 200000; net theo công thức floor của nguồn = 184000. Không trừ platform_discount hoặc commission lần nữa khỏi 184000.

Direct: tổng line đã sau item discount = 200000, header discount = 20000, shipping = 15000. Merchandise sau discount = 180000; total_amount = 195000. Nếu có refunded amount = 30000, doanh thu theo quy ước quản trị = 150000. Cần xác minh thành phần refund có bao gồm shipping hay không; nếu nguồn không tách được phải ghi giới hạn, không tự chia.

## Các chi tiết phải profiling/làm rõ trước Gold

- Semantics refund có bao gồm shipping; tổng refund vượt merchandise/total phải được phát hiện, không tự clamp.
- Khóa dòng marketplace và khả năng nhiều return trên một order.
- Trạng thái payment/order thực có, timestamp timezone, inventory transaction semantics và warehouse history.
- Chưa có lịch sử status/refund trước go-live: chỉ biết trạng thái từ snapshot đã quan sát, không dựng lịch sử giả.
