# INV-CUTOFF-v1 — cửa sổ snapshot tồn kho đã duyệt

Ngày duyệt: 2026-11-11. Người dùng đại diện nhóm xác nhận trong hội thoại: **đồng ý cửa sổ 60 giây, ghi rõ độ trễ**.

Chi tiết triển khai BR32, không đổi công thức Finance/rule version 1.1:

- EOD của ngày D là một **quan sát nguồn thật** trong khoảng [00:00:00, 00:01:00) ngày D+1, Asia/Ho_Chi_Minh.
- Đây là tồn quan sát gần cutoff, **không khẳng định tồn chính xác tuyệt đối tại 00:00**. Giữ actual observed_at, cutoff_at, capture_lag_seconds và snapshot_method=observed_eod_cutoff_window.
- Snapshot trước cutoff, muộn từ giây 60 trở đi, hoặc giữa ngày không được công bố dưới nhãn EOD. Current stock hợp lệ vẫn có thể được cập nhật, nhưng yêu cầu EOD phải báo thất bại/missing.
- Go-live phải cấu hình khi bắt đầu quan sát, được persist và không tự dịch chuyển. Không dùng source.last_updated để đặt ngày này.
- Không backfill tháng 6 bằng stock hiện tại, không tạo history simulated trong actual inventory.
- Capture đã lưu nguyên vẹn trong cửa sổ có thể được xử lý/rerun sau đó. Không sửa timestamp của manifest để vượt gate.
- Giữ capture EOD đã chấp nhận đầu tiên. Replay cùng vector product/warehouse/qty không thêm dòng; payload khác cho cùng ngày phải bị chặn/review, không tự chọn latest.

Go-live thực tế được xem trong lakehouse.ops.inventory_policy và audit ops.inventory_run. Việc nhóm đồng ý cửa sổ gần cutoff không tự thay thế chấp nhận ngoại lệ của người chấm đồ án.
