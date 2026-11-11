# Scripts

`check_repository.py` kiểm tra cấu trúc, link tài liệu hiện hành và Git ignore. Chạy từ bất kỳ working directory bằng đường dẫn tới script; script tự tìm repository root.

Scripts profiling/bootstrap/backfill sẽ được thêm khi triển khai. Công cụ ERD cũ nằm trong [legacy/](legacy/README.md).

Đã có `profile_sources.py` (read-only, output aggregate + source schema), `fetch_spike_artifacts.py` (download/cache official artifacts), `run_lakehouse_spike.py` và `spike_spark.py` (synthetic storage probe). Xem [trạng thái kiểm chứng](../docs/architecture/preimplementation-status.md) và [spike runbook](../infra/platform/spike/README.md); không coi script tồn tại là kết quả spike pass.
