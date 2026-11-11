# Runbooks — chưa triển khai

Cần ít nhất ba kịch bản: API lỗi/rate limit, SFTP thiếu/checksum sai, và DQ/reconciliation fail. Mỗi runbook phải có detect, phạm vi ảnh hưởng, cách xử lý, rerun an toàn và verify sau rerun.

Không coi việc DAG thành công là bằng chứng dữ liệu đúng. Đối soát và completeness phải được kiểm tra trước publication.
