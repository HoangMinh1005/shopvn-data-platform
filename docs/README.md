# Bản đồ tài liệu

Thứ tự áp dụng khi có khác biệt:

1. [Yêu cầu dự án gốc](sources/01_project_requirements.md): yêu cầu bắt buộc của đề bài.
2. [Business rules v1](business/business-rules-v1.md): quyết định nghiệp vụ được người dùng duyệt ngày 2026-11-11.
3. [Architecture baseline](architecture/baseline-v1.md) và [model baseline](data-model/README.md): định hướng triển khai hiện hành, có ghi rõ phần chưa kiểm chứng.
4. Các tài liệu `legacy/`: lịch sử phương án, không phải hợp đồng triển khai hiện hành.

Nếu quyết định nghiệp vụ đụng giới hạn dữ liệu hoặc yêu cầu đề bài, ghi ngoại lệ và báo cáo giới hạn; không sửa dữ liệu để tạo cảm giác đã đáp ứng.

## Vị trí tài liệu

| Thư mục | Nội dung |
|---|---|
| `sources/` | Yêu cầu, dataset guide, template và hình gốc giữ nguyên |
| `business/` | Quy tắc nghiệp vụ đã duyệt, KPI contracts |
| `architecture/` | Kiến trúc, stack, readiness, lịch sử migration |
| `data-model/` | Model hiện hành, mappings, data dictionary và bản nháp lịch sử |
| `profiling/` | Kết quả kiểm tra nguồn, không lưu raw PII |
| `runbooks/` | Xử lý sự cố và chạy lại an toàn |

Google Doc của nhóm đã được đối chiếu trong quá trình thảo luận: [GR2](https://docs.google.com/document/d/1W7usAMUEnK2EgTvUGEzO_teVFrxofRbNJk3tulM7zRs/edit). Nội dung trên Drive có thể thay đổi; repo chưa có bản export/version cố định của tài liệu này.

Template gốc còn ảnh minh họa chưa tồn tại và lệnh theo layout cũ. Dùng [README repository](../README.md) để chạy lệnh trong layout mới.
