# Bản đồ tài liệu

Thứ tự áp dụng khi có khác biệt:

1. [Yêu cầu dự án gốc](sources/01_project_requirements.md): yêu cầu bắt buộc của đề bài.
2. [Business rules, phiên bản hiện hành 1.1](business/business-rules-v1.md): gồm BR01–BR30 và [BR31/BR32 bổ sung đã duyệt](business/decisions/2026-11-11-finance-inventory.md) ngày 2026-11-11.
3. [Kế hoạch triển khai MVP](architecture/mvp-implementation-plan.md): nguồn duy nhất về thứ tự stage, phạm vi, checklist và trạng thái phê duyệt. S2 là SFTP + Finance marketplace; thứ tự S3–S7 còn là đề xuất chờ duyệt.
4. [Architecture baseline](architecture/baseline-v1.md) và [model baseline](data-model/README.md): định hướng kỹ thuật/model, có ghi rõ phần chưa kiểm chứng; không tự đổi thứ tự trong kế hoạch.
5. Các tài liệu `legacy/`: lịch sử phương án, không phải hợp đồng triển khai hiện hành.

Nếu quyết định nghiệp vụ đụng giới hạn dữ liệu hoặc yêu cầu đề bài, ghi ngoại lệ và báo cáo giới hạn; không sửa dữ liệu để tạo cảm giác đã đáp ứng.

Readiness chứa các bước chuẩn bị **P1–P3**, không phải các stage MVP **S1–S7**. Muốn đổi thứ tự/phạm vi phải theo change-control trong kế hoạch và được người dùng duyệt trước; không lấy bản ghi trong chat hoặc dòng "bước tiếp theo" ở tài liệu khác làm phê duyệt tự động.

Bản đồ implementation sau refactor: [cấu trúc application](architecture/code-structure.md). Model thiết kế đích không đồng nghĩa toàn bộ bảng/feature đã được triển khai.

Chặng 1 đã có [runbook PostgreSQL/SCD2/inventory](runbooks/stage1-postgres-inventory.md), [quyết định cutoff EOD 60 giây đã duyệt](business/decisions/2026-11-11-inventory-cutoff.md) và [bằng chứng thực thi](verification/stage1.json). Finance rule version vẫn 1.1, source contract version 2 và inventory policy INV-CUTOFF-v1 là ba version riêng.

S2 có [runbook SFTP/marketplace Finance](runbooks/stage2-marketplace-finance.md) và [evidence aggregate](verification/stage2.json): implementation/self-verification PASS, chờ nhóm review/đóng stage. Source contract marketplace version 1 không thay business rule 1.1 hoặc PostgreSQL contract version 2.

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
