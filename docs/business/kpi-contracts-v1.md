# KPI contracts v1

Nghiệp vụ tham chiếu [BR01–BR30](business-rules-v1.md). Tiền dùng VND, Decimal/integer arithmetic; ngày báo cáo theo Asia/Ho_Chi_Minh. Contract ghi đủ công thức, grain, thời gian, eligibility và giới hạn để người viết SQL không tự suy nghĩa. Data owner dưới đây là vai trò trong đồ án; nhóm điền tên người review khi phân công.

## Quy tắc dùng chung

- Eligible direct: status delivered/returned, payment paid/refunded, discount trong [0, subtotal], khóa nguồn hợp lệ. Không ép đơn discount lỗi thành doanh thu 0. Dòng quantity 0 không tham gia units/sales-line denominator.
- Eligible marketplace: completed trong file đã xác minh MD5. Không trộn returned/disputed vào completed revenue.
- Refund: tổng theo order_id trước khi ghép order, chỉ status refunded. payment_status refunded không tạo phép trừ lần hai.
- order_date dùng để restate cohort; report có data_as_of/published_at/publication_id/source_completeness. Không có actual refund timestamp hoặc payout confirmation.
- So sánh direct/marketplace dùng merchandise sau discount **trước refund và commission** trên cùng date/channel grain. Marketplace thiếu refund detail nên không gọi so sánh này là net after return toàn kênh.
- Báo cáo từng nguồn có thể partial khi nguồn khác thiếu; tổng toàn kênh gắn completeness, không thay missing thành 0. DQ block là trạng thái publication, không phải một dòng doanh thu bằng 0.

## Finance — owner: Finance/Data lead

| ID | KPI / grain | Công thức và nguồn | Ngày / giới hạn |
|---|---|---|---|
| FIN01 | Direct merchandise after discounts / order rồi day-channel | orders.subtotal - orders.discount_amount | order_date, eligible direct, trước refund/shipping |
| FIN02 | Net customer amount after refund / order rồi day-channel | orders.total_amount - SUM(returns.refund_amount WHERE refunded) | order_date restated; khớp phép tính đề bài; bao gồm shipping thu khách |
| FIN03 | Merchandise less order refunds — **proxy** / order rồi day-channel | FIN01 - SUM(refunded amount) | Refund source có shipping; có thể âm, chưa phải net merchandise chính xác. Giới hạn P01 trong profiling |
| FIN04 | Marketplace merchandise / line rồi day-partner | SUM(sale_price * quantity_sold) | order_date, completed; sale_price đã sau platform discount |
| FIN05 | Marketplace partner-reported net / line rồi day-partner | SUM(net_revenue nguồn) | order_date, completed; không trừ commission/discount lần hai |
| FIN06 | Marketplace implied deduction / line rồi day-partner | FIN04 - FIN05 | Bao gồm sai số floor/float nguồn, không chứng minh commission đã thực trả |
| FIN07 | Settlement schedule / settlement-day-partner | SUM(net_revenue nguồn) | settlement_date=T+15; scheduled/assumed receipts trong dataset, không phải bank cash |
| FIN08 | Direct header discount / day-channel | SUM(discount_amount) eligible orders, không nhân theo line | order_date; không mặc định toàn bộ là voucher cost |
| FIN09 | Direct item discount / day-channel-product | SUM(discount_per_item * quantity) trên eligible line | order_date, trước header discount; không cộng platform discount vào chi phí ShopVN |
| FIN10 | Cross-channel comparable merchandise / day-channel | Direct FIN01 + marketplace FIN04 theo conformed schema | Trước refund và commission; không cộng FIN05 lần nữa |
| FIN11 | Eligible order count / day-channel | COUNT DISTINCT (source_system, source_order_id) trên tập eligible | Direct header; marketplace chỉ các completed order quan sát từ file hợp lệ, đơn mixed-status được tính khi có completed line |
| FIN12 | AOV merchandise / day-channel | Comparable merchandise / eligible order count | Không có denominator thì NULL; không lấy average của các tỷ lệ ngày để tính tháng |

## Customer & Marketing — owner: Marketing/Data lead

| ID | KPI / grain | Định nghĩa | Giới hạn |
|---|---|---|---|
| CUS01 | Average customer spend / day hoặc month | SUM(FIN03 direct proxy) / distinct direct customers có eligible order trong kỳ | Nhãn spend proxy kế thừa P01; có thể bổ sung FIN02 customer-payment view, không tráo measure |
| CUS02 | Top customers / month | Rank direct customers theo cùng measure CUS01 | Customer ref HMAC; không có marketplace customer |
| CUS03 | Repeat within 30 days / first-order cohort | Khách có eligible order thứ hai trong [first_order_date, first_order_date+30 ngày] / khách cohort đủ 30 ngày quan sát | Hai order distinct cùng ngày là repeat cùng ngày; chỉ chính xác ở độ phân giải ngày. Không dùng ingestion created_at thay business time |
| CUS04 | Inactive / as-of day | Có eligible order trước đây nhưng không có đơn trong 90 ngày gần nhất | Chặn kết luận đầy đủ khi chỉ có 30 ngày dữ liệu; không gọi khách mới là inactive |
| CUS05 | Campaign-associated performance / campaign-period | Distinct eligible order/customer, merchandise, header discount, FIN03 theo voucher_code->campaign_id | Không chứng minh causal uplift hoặc ROI; không dùng vouchers.usage_count hiện tại để thay số đơn trong kỳ |
| CUS06 | Loyalty segment spend / tier-period | CUS01 theo tier tại thời điểm đã quan sát | Trước go-live dùng initial/current tier view có nhãn, không khẳng định historical tier |
| CUS07 | Product rating / product-category-channel-period | AVG(rating), review_count trên valid ratings 1..5 | Review.created_at, direct only; hiển thị sample count; không tạo marketplace ratings |

CUS03 ghi rõ độ phân giải theo ngày vì nguồn không có business timestamp đặt hàng đáng tin cậy. Đây là làm rõ độ chính xác của cửa sổ 30 ngày, không suy lịch sử từ thời gian nạp dữ liệu.

## Logistics — owner: Operations

| ID | KPI / grain | Công thức | Giới hạn |
|---|---|---|---|
| OPS01 | Success / carrier-order-date-cohort | delivered / (delivered+failed+returned) | Trạng thái mới nhất quan sát; in-flight riêng; không group theo ngày delivery cho denominator failed thiếu date |
| OPS02 | Failure / cùng grain | (failed+returned) / terminal shipments; hiển thị failed và returned riêng | Returned ở API là kết quả logistics, không tương đương refunded return request |
| OPS03 | Average delivery hours / carrier/province | AVG(actual_delivery_date-shipped_at) trên delivered, đủ timestamp hợp lệ | Không ép duration thiếu thành 0; lưu UTC rồi tính ngày local |
| OPS04 | Re-delivery attempt rate / carrier-cohort | shipments có delivery_attempts>1 / shipments có attempt count hợp lệ | Snapshot count, không có fact từng attempt; có thể lọc terminal khi so sánh cùng cohort |
| OPS05 | Late delivery rate / carrier-cohort | delivered có local_delivery_date>estimated_delivery_date / delivered có đủ dates | ETA là ngày; ETA thiếu thì loại denominator và đếm missing |
| OPS06 | Failure reasons / carrier-province | COUNT shipment failed theo failure_reason | NULL -> unknown; không suy reason cho API error |
| OPS07 | Return-request order rate / direct channel-cohort | Distinct eligible order có return status pending/approved/refunded / distinct eligible order | Loại rejected khỏi return-request actionable rate; vẫn báo rejected riêng. Category proxy theo BR22, multi-category non-additive |

OPS07 là default đề xuất làm rõ vì BR22 chưa xác định bộ return statuses. Cần nghiệp vụ duyệt trước khi publish KPI này; không gắn nhãn đã duyệt cho chi tiết bổ sung.

## Inventory & Product — owner: Product/Operations

| ID | KPI / grain | Công thức | Giới hạn |
|---|---|---|---|
| INV01 | Current stock / product-observed_at | inventory.stock_qty | Có nguồn hiện tại, warehouse có trong inventory |
| INV02 | EOD stock / product-business-day | Anchor - signed changes sau EOD đến cùng anchor cutoff | **Blocked cho lịch sử tháng 6**: timestamp nguồn không hợp lệ làm business time; chỉ snapshot go-live hoặc lịch sử mô phỏng tách riêng |
| INV03 | Days of stock / product-as_of | current_stock / (eligible units sold 7 ngày gồm ngày 0 sales /7) | Avg=0 -> NULL; không đủ window -> incomplete, không ngoại suy từ June sang ngày hiện tại |
| INV04 | Sell-through / product-period | units sold / (opening stock + eligible receipts) | Chưa publish khi inventory history chưa đủ căn cứ; không dùng INV03 thay sell-through |
| INV05 | Loss estimate / product/channel-period | Direct net line sau phân bổ header discount - quantity*cost_as_observed; marketplace thêm view reported_net-quantity*cost | Trước product refund, thiếu historical cost/cost vận hành; không gọi net profit |
| INV06 | Best/slow sellers / category-period-channel | Rank eligible units sold; include active products 0 sales bằng product spine | So sánh cùng khoảng dữ liệu complete; active hiện tại không chứng minh active lịch sử |

## Publication / owner: Data lead

PUB01: publish D-1 trước 08:00 Asia/Ho_Chi_Minh với completeness từng partner/ngày. PUB02: khi incident, phục hồi trong 2 giờ từ phát hiện và đối soát lại. PUB03: dữ liệu simulated hiển thị riêng. Dashboard phải có rejected/excluded counts, formula warnings và freshness để người xem nhận ra giới hạn.
