# Công cụ repository và CLI wrappers

Application logic nằm trong [src/shopvn](../src/shopvn/README.md). Không import scripts từ application.

| Công cụ | Vai trò |
|---|---|
| direct_sales_spark.py | Wrapper Spark-submit → shopvn.jobs.direct_finance |
| publish_finance_spark.py | Wrapper → shopvn.jobs.publish_finance |
| extract_direct_snapshot.py | Wrapper → shopvn.ingestion.postgres |
| pipeline_monitor.py | Wrapper → shopvn.monitoring.exporter |
| bootstrap_airflow.py | Local metadata DB/credentials bootstrap |
| check_repository.py | Kiểm tra cấu trúc, links, Git ignore |
| profile_sources.py | Profiling aggregate, read-only |
| run_lakehouse_spike.py / run_direct_slice.py | Kiểm chứng storage/replay, không phải dependency runtime |
| verify_airflow.py | Smoke test scheduler, DQ gate, retry và alert recovery |
| export_verification.py / export_airflow_verification.py | Xuất bằng chứng aggregate và image lock |
| fetch_spike_artifacts.py / spike_spark.py | Build artifacts và storage compatibility probe |
| render_finance_chart.py | Vẽ từ kết quả Trino đã đối soát |

Cài package trước khi chạy tool: python -m pip install --no-deps -e . (hoặc uv pip install theo [hướng dẫn](../docs/architecture/code-structure.md)). Scripts verification có thể tạo test runs/candidate tables; không chạy tùy tiện trong production. Generator ERD cũ đã bỏ, xem [lịch sử](legacy/README.md).

[Runtime evidence](../docs/verification/README.md), [Airflow runbook](../infra/platform/airflow/README.md), [spike runbook](../infra/platform/spike/README.md).
