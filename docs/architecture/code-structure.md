# Cấu trúc application sau refactor

Mục tiêu: package có thể cài đặt, phân tách trách nhiệm rõ ràng, giữ nguyên kết quả Finance và cách vận hành local đã kiểm chứng. Đây là cấu trúc hướng sản phẩm, không phải xác nhận hệ thống đã production-ready.

```text
src/shopvn/
  config.py                    Đường dẫn môi trường và runtime
  contracts/direct.py          Schema/parser/allowlist nguồn, PK, rule version
  ingestion/postgres.py        Snapshot PostgreSQL read-only, manifest/checksum
  ingestion/sql/               SQL đối soát nguồn được đóng gói trong wheel
  transformations/
    bronze/direct.py           Xác minh snapshot, ghi Bronze chống nạp trùng
    silver/direct.py           Chuẩn hóa wire values, snapshot Silver hiện hành
    gold/dimensions.py         Observed versions và lookup trước go-live
    gold/orders.py            Eligibility và số tiền cấp đơn
    gold/sales_lines.py       Phân bổ discount cấp dòng
    gold/returns.py           Return cấp đơn
  marts/finance.py             Tổng hợp FIN02 ngày × kênh
  marts/publication.py         Gate snapshot đã duyệt và atomic publish
  pipelines/direct_finance.py  Kết hợp các tầng, kiểm tra và persistence
  jobs/                       CLI/Spark-submit entry points
  quality/                    Spark checks, Decimal reconciliation, reference tests
  common/                     Spark runtime, Iceberg writes, local env loader
  clients/trino.py             Trino HTTP client
  orchestration/              Airflow task adapters, audit, callback, deadline
  monitoring/exporter.py       Exporter Prometheus và health checks
dags/                         Khai báo dependency, lịch, pool, retry
scripts/                      Wrapper CLI, bootstrap, profiling, verification/build tools
tests/unit/                   Contracts, nghiệp vụ, cấu hình và ranh giới package
tests/integration/            Kiểm thử Spark thật và saved-snapshot replay
```

## Ranh giới dependency

- Application không import bất kỳ module nào từ `scripts/`; có AST regression test bảo vệ quy tắc này.
- Ingestion không cần Spark/Airflow. Source allowlist và parser schema dùng chung contract để tránh lệch cột.
- Fact builders và mart aggregation nhận/trả DataFrame; pipeline điều phối ghi Iceberg, writer lock và DQ.
- Dimension observed-version management có đọc/ghi Iceberg; giới hạn pre-go-live vẫn được giữ nguyên. Chưa có complete event-time SCD2/as-of.
- DAG chỉ điều phối. Entry scripts cũ được giữ làm wrapper mỏng để lệnh chạy và đường dẫn Spark-submit không bị đứt.
- `quality/acceptance.py` là công thức tham chiếu độc lập cho fixture; không dùng nó làm engine xử lý Spark, tránh kiểm chứng bằng chính implementation.

## Cài và cấu hình

Python 3.11. Từ root repository:

```powershell
python -m pip install -r requirements-preparation.txt
python -m pip install --no-deps -e .
python scripts/check_repository.py
python -m pytest -q
```

Với môi trường `.venv` tạo bằng uv không có pip, dùng `uv pip install --python .venv/Scripts/python.exe --no-deps -e .`.

Package cũng build/cài được từ wheel; SQL baseline đi kèm package qua importlib.resources, không phụ thuộc một checkout bên ngoài. Khi dùng wheel ở môi trường triển khai, cấu hình `SHOPVN_PROJECT_ROOT` và `SHOPVN_RUNTIME_DIR` bằng đường dẫn rõ ràng. Runtime tương đối được resolve theo project root; không tự ghi vào site-packages. Credentials vẫn truyền bằng environment/secret mount, không commit.

Console commands: `shopvn-extract`, `shopvn-finance`, `shopvn-publish-finance`, `shopvn-monitor`. Spark jobs chạy trong image Spark đã khóa; không cài PySpark mới nhất vào Airflow virtualenv để thay engine. CLI `--help` và import job không khởi động Spark.

Local Compose vẫn mount source để phát triển và dùng image runtime đã kiểm chứng. Việc kiểm tra wheel không đồng nghĩa đã có immutable application image/CI deployment production. Không đổi tên catalog, bucket, bảng, volume hoặc rotate HMAC key trong đợt refactor này.

## Kiểm thử hồi quy

1. Unit/reference tests và kiểm tra import boundary.
2. Spark integration trên fixture: boolean encoding, formula mismatch, fractional discount, zero quantity, refund và FIN02.
3. Replay snapshot nguồn đã lưu qua toàn bộ các tầng; query candidate bằng Trino và đối soát 60 ngày × kênh. Không publish candidate trong bài replay này.
4. Smoke test Airflow khi PostgreSQL nguồn sẵn sàng: DQ fail phải chặn publication, sau đó retry và phục hồi.

Xem [hướng dẫn tests](../../tests/README.md) và [bằng chứng thực thi](../verification/README.md).

## Tinh gọn và migration

Đã bỏ monolith `transformations/direct_sales.py`, khai báo schema/allowlist trùng, dependency runtime vào spike scripts, các `.gitkeep` không cần thiết, thư mục config rỗng và generator ERD JavaScript chỉ dành cho model cũ. File SQL baseline được chuyển vào ingestion/sql; entry scripts hiện hành không bị xóa.

Giữ nguyên tài liệu nguồn, model/ERD lịch sử, source profiling, storage spike tools, acceptance references và evidence. Chúng là tài sản kiểm chứng, không phải code thừa. Không xóa `.env`, runtime snapshots, data, volumes, HMAC key hoặc logs. Các file code đã bỏ có thể khôi phục từ Git tại commit `2430174`.

Kafka/simulator, API/SFTP ingestion, Superset, incremental/as-of đầy đủ và retention vẫn là hạng mục tiếp theo; không tạo implementation rỗng để thể hiện chúng đã có.
