# Kiểm thử

84 unit/reference tests: business rules, contracts, runtime paths, import boundaries, exact Decimal reconciliation, Airflow callbacks/finalizer, deadline và inventory cutoff. Spark suites có 4 base + 15 temporal cases trên engine thật, tách khỏi host pytest để không yêu cầu Java/PySpark trên máy host. Thêm 3 clock checks dùng actual Pendulum trong container Airflow.

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

```powershell
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml run --rm --no-deps spark --master "local[2]" --driver-memory 1g /workspace/tests/integration/stage1_temporal_spark.py
Get-Content -Raw tests/integration/airflow_time_runtime.py | docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml exec -T airflow-scheduler python -
```

Temporal tests dùng namespace shopvn.stage1_tests: versions thay đổi/không đổi, replay/stale/same-clock conflict, overlap/missing lookups, day-boundaries, dated fact joins, late/EOD conflict và immutable policy. Clock tests không ghi metadata/source.

## Replay snapshot đã lưu

```powershell
python tests/integration/verify_direct_replay.py
```

Đọc snapshot trong runtime/direct/latest-snapshot.json, chạy toàn bộ Bronze/Silver/Gold thành candidate riêng rồi query Trino. So sánh count/amount và snapshot certified mart trước/sau để chứng minh không publish trong bài test này. Không cần PostgreSQL nguồn hoạt động; đây là replay dữ liệu đã lưu, không phải fresh extraction. Có thể truyền --snapshot-id.

Chặng 1 yêu cầu manifest contract 2 đủ 9 bảng. Không apply Finance snapshot cũ hơn observation watermark của customer/product: lấy source snapshot mới để restate, không rollback dimension. Runner `verify_stage1_replays.py` chạy 3 fresh processes trên cùng latest full snapshot; chạy ngay sau Finance test, trước lần inventory observation mới advance watermark.

`python tests/integration/verify_actual_eod_replay.py` chỉ chạy sau actual scheduled EOD success: dùng capture đã lưu, không fake timestamp và không re-extract. Sau đó `python scripts/export_stage1_verification.py` xuất aggregate evidence khi tất cả gates pass.

## Airflow end-to-end

```powershell
python scripts/verify_airflow.py --business-date 2026-06-30
```

Cần PostgreSQL nguồn và core/ops đang hoạt động. Negative test phải đi qua extract/transform thành công rồi fail đúng validate_candidate; lỗi nguồn không được tính thay cho DQ failure injection. Test tiếp theo fail DQ một lần, retry thành công, publish/post-check và resolve alerts.

## Phạm vi

Các runner storage/direct trước đó đã kiểm tra 3 fresh Spark processes và persistence qua restart. CI chạy unit/reference, package build, lint và Compose validation; chưa tự dựng full stack hoặc nguồn thật. [Evidence](../docs/verification/README.md) ghi rõ phần đã chạy.

Fixtures chỉ chứa dữ liệu tổng hợp. Không commit CSV nguồn, HMAC key, passwords hoặc runtime logs. Kafka duplicate/late-event tests và source event-time history không thuộc các kiểm thử chặng 1.
