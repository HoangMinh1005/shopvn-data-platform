# Runbooks

Đã có [direct Finance trên Airflow](direct-finance-airflow.md): service/task fail, source coverage, DQ/reconciliation, deadline, rerun và restart. API lỗi/rate limit và SFTP thiếu/checksum sai sẽ bổ sung khi các connector đó được triển khai; chưa gắn nhãn đã kiểm thử.

Không coi việc DAG thành công là bằng chứng dữ liệu đúng. Đối soát và completeness phải được kiểm tra trước publication.
