# Kiểm thử

59 unit/reference tests: business rules, contracts, runtime paths, import boundaries, exact Decimal reconciliation, Airflow callbacks/finalizer và deadline. Spark layer suite có 4 ca chạy trên engine thật, tách khỏi host pytest để không yêu cầu Java/PySpark trên máy host.

## Unit / CI

```powershell
python -m pip install -r requirements-preparation.txt
python -m pip install --no-deps -e .
python -m pytest -q
ruff check scripts src tests dags
python scripts/check_repository.py
```

## Spark integration — fixture tổng hợp

```powershell
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml run --rm --no-deps spark --master "local[2]" --driver-memory 1g /workspace/tests/integration/direct_layers_spark.py
```

Suite dùng các hàm application thật: normalizing PostgreSQL boolean, reject invalid encoding/source total, fractional discount remainder, zero-quantity lines, refunded-only amount và FIN02. Không ghi vào bảng nghiệp vụ.

## Replay snapshot đã lưu

```powershell
python tests/integration/verify_direct_replay.py
```

Đọc snapshot trong runtime/direct/latest-snapshot.json, chạy toàn bộ Bronze/Silver/Gold thành candidate riêng rồi query Trino. So sánh count/amount và snapshot certified mart trước/sau để chứng minh không publish trong bài test này. Không cần PostgreSQL nguồn hoạt động; đây là replay dữ liệu đã lưu, không phải fresh extraction. Có thể truyền --snapshot-id.

## Airflow end-to-end

```powershell
python scripts/verify_airflow.py --business-date 2026-06-30
```

Cần PostgreSQL nguồn và core/ops đang hoạt động. Negative test phải đi qua extract/transform thành công rồi fail đúng validate_candidate; lỗi nguồn không được tính thay cho DQ failure injection. Test tiếp theo fail DQ một lần, retry thành công, publish/post-check và resolve alerts.

## Phạm vi

Các runner storage/direct trước đó đã kiểm tra 3 fresh Spark processes và persistence qua restart. CI chạy unit/reference, package build, lint và Compose validation; chưa tự dựng full stack hoặc nguồn thật. [Evidence](../docs/verification/README.md) ghi rõ phần đã chạy.

Fixtures chỉ chứa dữ liệu tổng hợp. Không commit CSV nguồn, HMAC key, passwords hoặc runtime logs. Kafka duplicate/late-event tests và complete SCD2/as-of sẽ bổ sung khi có implementation.
