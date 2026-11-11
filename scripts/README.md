# Scripts

`check_repository.py` kiểm tra cấu trúc, link tài liệu hiện hành và Git ignore. Chạy từ bất kỳ working directory bằng đường dẫn tới script; script tự tìm repository root.

Scripts profiling/bootstrap/backfill sẽ được thêm khi triển khai. Công cụ ERD cũ nằm trong [legacy/](legacy/README.md).

Đã có profiling, artifact downloader, storage spike, consistent read-only direct snapshot extraction, direct Spark runner, chart renderer và evidence exporter. Xem [verification](../docs/verification/README.md) và [spike runbook](../infra/platform/spike/README.md) để phân biệt phần đã chạy với phần còn kế hoạch.
