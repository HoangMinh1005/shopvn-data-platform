# Runbooks

Đã có [direct Finance trên Airflow](direct-finance-airflow.md): service/task fail, source coverage, DQ/reconciliation, deadline, rerun và restart. API lỗi/rate limit và SFTP thiếu/checksum sai sẽ bổ sung khi các connector đó được triển khai; chưa gắn nhãn đã kiểm thử.

[Chặng 1 — PostgreSQL, SCD2 và actual inventory](stage1-postgres-inventory.md): source contract 9 bảng, observed-time/day-as-of, current stock, lịch EOD 00:00 VN/cửa sổ 60 giây, replay và chống backfill sai.

[Khả năng khôi phục PostgreSQL nguồn](source-postgres-recovery.md): kiểm tra read-only volume còn lại ngày 12/11/2026; chưa thực hiện restore/start nguồn.

[S2 — SFTP và Finance marketplace](stage2-marketplace-finance.md): host pin, MD5-before-parse, versioned file manifest, replacement/rerun, partial coverage và DQ-gated publication. Runbook PostgreSQL recovery ở trên có cập nhật sau khi người dùng tự phục hồi nguồn; không dùng mốc outage cũ như current health.

Không coi việc DAG thành công là bằng chứng dữ liệu đúng. Đối soát và completeness phải được kiểm tra trước publication.
