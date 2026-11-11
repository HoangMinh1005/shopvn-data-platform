# Readiness spike — đã kiểm chứng PASS

Kết quả mới nhất: [storage + direct Finance evidence](../../../docs/verification/README.md). Core đã chạy 3 Spark process, Trino đọc đúng và restart giữ dữ liệu. Các phần ngoài core vẫn chưa được chứng nhận.

Môi trường demo riêng `shopvn-readiness`, chỉ dùng dữ liệu synthetic. Không kết nối hoặc ghi PostgreSQL nguồn. Ba service dài hạn: PostgreSQL catalog, MinIO và Trino; Spark chạy job local[2] riêng. Named volumes giữ dữ liệu sau stop; không dùng `down -v` như cách dừng thông thường.

## Chuẩn bị

`.env` có SPIKE_DB_PASSWORD, SPIKE_MINIO_USER, SPIKE_MINIO_PASSWORD (đã có cục bộ; template là placeholder). Dùng Docker Linux containers. Máy hiện cấp khoảng 6.77 GiB cho Docker, nên không bật full platform trong khi test.

Từ root repository:

```powershell
.venv/Scripts/python.exe scripts/fetch_spike_artifacts.py
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml build minio spark
.venv/Scripts/python.exe scripts/run_lakehouse_spike.py
```

Linux dùng `.venv/bin/python`. Downloader lưu cache trong `runtime/spike-build/`, xác minh checksum mạnh nhất Maven Central công bố (Iceberg: SHA512; PG JDBC release này: SHA1, đồng thời ghi fingerprint SHA256). `.dockerignore` chỉ gửi artifacts được allowlist và Dockerfiles, không gửi `.env`, source CSV hoặc dữ liệu profiling cho Docker build. Có thể dùng --only theo filename để tải độc lập.

## Tiêu chí pass của runner

1. Spark tạo Iceberg v2 trên MinIO qua PostgreSQL JDBC Catalog.
2. MERGE một record từ 100 thành 125, thêm record 200, schema thêm source_system.
3. Chạy 3 **Spark process mới**, mỗi lần count=2, distinct_keys=2, sum=325.00.
4. Trino đọc cùng bảng, cùng sum/count.
5. Restart catalog/object store/Trino và đọc lại đúng kết quả.

Runner ghi `runtime/spike/spike-report.json`, `spark-result.json`, tự stop các service spike khi kết thúc (giữ volumes). Lần kiểm chứng hiện tại đã có report.status = passed và [verified-lock.json](verified-lock.json). Khi rebuild/đổi dependencies, phải chạy lại spike và xuất evidence mới; bản lock chỉ chứng minh image đã test, không đảm bảo mọi lần rebuild qua apt đều bit-for-bit giống nhau.

Nếu cần giữ service để debug: thêm `--keep-running`. Chỉ bind host ports loopback 19000 (MinIO) và 18080 (Trino). Trino API trong runner chỉ tin service loopback của spike, không dùng cho cluster bên ngoài.

## Các phiên bản/nguồn đã resolve

- Spark distribution: apache/spark:3.5.9, manifest digest ghim trong Dockerfile.
- Spark runtime đã build/test: Python3.11.15 slim Bookworm + Java17.0.21.
- Iceberg runtime 3.5_2.12 và aws bundle 1.12.0; PostgreSQL JDBC42.7.8.
- Trino484 và PostgreSQL16: registry digest ghim trong Compose.
- MinIO source commit 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a, Go builder và Alpine runtime có digest ghim. Digest Python base và image/JAR identities có trong verified-lock.json.

MinIO upstream hiện archived/source-only; build này phục vụ kiểm chứng đồ án, không phải recommendation production. Tham chiếu [upstream MinIO](https://github.com/minio/minio), [Iceberg JDBC](https://iceberg.apache.org/docs/latest/jdbc/), [Spark catalog](https://iceberg.apache.org/docs/latest/spark-configuration/), [Trino catalog](https://trino.io/docs/current/object-storage/metastores.html).

## Phạm vi chưa test trong spike này

Airflow submit/provider, Kafka integration và Superset chart chưa nằm trong core storage spike. Các version đó vẫn là candidate đến khi vertical slice kiểm chứng, không ghi toàn bộ stack là đã khóa runtime.
