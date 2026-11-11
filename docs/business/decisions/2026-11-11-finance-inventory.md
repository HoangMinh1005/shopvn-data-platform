# Quyết định bổ sung sau profiling — BR31, BR32

Ngày duyệt: 2026-11-11. Người duyệt: người dùng đại diện nhóm dự án, qua câu trả lời trực tiếp cho hai câu hỏi trong hội thoại. Rule version sau thay đổi: **1.1**. Tài liệu nguồn gốc và 30 quyết định trước được giữ để truy vết.

## BR31 — Finance primary metric

Lựa chọn được duyệt: **Dùng net customer amount sau refund; merchandise sau refund giữ nhãn proxy.**

- Direct KPI chính: `SUM(orders.total_amount - refunded_order_amount)` trên các order đủ điều kiện BR02/BR27. Tổng hợp returns về order trước khi join; chỉ status refunded.
- Merchandise less order refunds giữ measure riêng có tên `_proxy`, có thể âm khi hoàn cả shipping, không tự clamp hoặc phân bổ refund.
- Marketplace tiếp tục dùng partner-reported net sau commission ở FIN05. Không cộng FIN02 và FIN05 thành một KPI có ngữ nghĩa đồng nhất. FIN10 là merchandise so sánh đa kênh trước refund/commission, tách khỏi headline Finance theo từng nguồn.
- Customer spend proxy/CUS01 vẫn giữ định nghĩa trước; quyết định chọn headline Finance không tự đổi các KPI khác chưa được duyệt thay đổi.
- Giữ cách gán refund về order_date và khả năng restate theo BR12. Đây là báo cáo quản trị, không tạo dữ liệu bank cash hoặc actual refund timestamp.

Ảnh hưởng model: `fact_order.net_customer_amount` là Finance primary measure; `merchandise_less_order_refund_proxy` là measure phụ. Không đổi source data hoặc source identities. Chưa có production publication nên chưa có backfill production cần chạy; fixture và reference outputs vẫn giữ hai số để kiểm tra sự khác biệt.

## BR32 — Inventory scope

Lựa chọn được duyệt: **Dùng current stock và snapshot từ go-live; chặn backfill tháng 6.**

- Publish current stock kèm observed_at/source_last_updated; không gọi một snapshot lấy giữa ngày là EOD.
- Lưu daily snapshot thực từ khi pipeline bắt đầu quan sát và có cutoff EOD hợp lệ.
- `inventory_go_live_date` phải được cấu hình và lưu trong metadata khi triển khai. Snapshot trước mốc đó bị chặn ở DQ/publication.
- Không dùng source.created_at đang sai business time để dựng June EOD. Không tạo dữ liệu lịch sử giả lập cho MVP dưới cùng nhãn actual/source.
- INV02 chỉ bắt đầu từ snapshot EOD hợp lệ; INV04 sell-through chưa có historical stock đủ thì chưa publish. Không ghép current stock tháng khác với June sales để gọi là dự báo stockout hiện tại.

Ảnh hưởng phạm vi: phần yêu cầu đề bài về dựng tồn kho lịch sử tháng 6 được ghi thành **ngoại lệ do nguồn**, chưa được xem là đã đáp ứng. Sự đồng ý của nhóm dự án không tự thay thế xác nhận của người đánh giá đề bài; khi demo phải trình bày giới hạn cùng evidence profiling.

## Kiểm chứng

Fixture Finance tổng: FIN02 = 300002 VND, FIN03 proxy = 265002 VND. Full refund gồm merchandise 100 + shipping 20: FIN02 = 0, proxy = -20. Reference gate kiểm tra snapshot trước go-live bị từ chối; gate này chưa thay thế kiểm tra cutoff/source integrity trong production pipeline.
