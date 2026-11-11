# ShopVN Data Platform

Dự án phân tích thương mại điện tử với Spark Lakehouse, Iceberg và Kafka mô phỏng. Nghiệp vụ v1.1 đã được duyệt; repository hiện triển khai từng vertical slice, chưa hoàn thành full MVP.

## Trạng thái hiện tại

- Đã tổ chức tài liệu, ghi nhận nghiệp vụ đã duyệt và khởi tạo Git repository.
- Có Compose cho ba nguồn PostgreSQL, Logistics API và SFTP.
- Core Spark–Iceberg–MinIO–JDBC Catalog–Trino đã chạy thành công. Direct Finance có Bronze/Silver/Gold, source reconciliation và biểu đồ từ Trino; đã có Airflow orchestration và Prometheus/Grafana giám sát. Kafka/Superset chưa triển khai.
- ERD và kế hoạch cũ được giữ trong `legacy/`; đọc baseline hiện hành trước khi dùng lại.
- Đã profiling 9 bảng và marketplace files, bổ sung KPI/data contracts, 59 unit/reference tests, 4 Spark layer integration tests, 3 fresh-process replay checks cho core/direct và smoke test Airflow failure → recovery. [Bằng chứng thực thi](docs/verification/README.md) ghi rõ scope đã pass và phần còn thiếu.
- Nghiệp vụ hiện hành v1.1 đã chốt: FIN02 net customer amount là KPI Finance chính cho direct; inventory chỉ current stock và actual snapshots từ go-live, chặn June backfill. [Biên bản BR31/BR32](docs/business/decisions/2026-11-11-finance-inventory.md).

## Tài liệu bắt đầu

| Nội dung | Tài liệu |
|---|---|
| Bản đồ tài liệu và thứ tự ưu tiên | [docs/README.md](docs/README.md) |
| Yêu cầu gốc | [Project requirements](docs/sources/01_project_requirements.md) |
| Mô tả nguồn | [Dataset guide](docs/sources/02_dataset_guide.md) |
| Quy tắc nghiệp vụ đã duyệt | [Business rules v1.1](docs/business/business-rules-v1.md) |
| Kiến trúc và stack đã chọn | [Architecture baseline](docs/architecture/baseline-v1.md) |
| Data model hiện hành và các điểm cần xác minh | [Model baseline](docs/data-model/README.md) |
| Công việc tiếp theo | [Implementation readiness](docs/architecture/implementation-readiness.md) |
| Kết quả profiling thực tế | [Source findings](docs/profiling/findings.md) |
| KPI contracts | [KPI contracts v1](docs/business/kpi-contracts-v1.md) |
| Data dictionary và mapping | [Gold data dictionary](docs/data-model/data-dictionary-v1.md) |
| ERD hiện hành | [Gold ERD](docs/data-model/gold-erd.md) |
| Bộ số nghiệm thu | [Acceptance cases](docs/business/acceptance-cases.md) |
| Core lakehouse spike | [Spike runbook](infra/platform/spike/README.md) |
| Kết quả triển khai direct Finance | [Verification report + chart](docs/verification/README.md) |
| Airflow + Prometheus/Grafana | [Setup và vận hành](infra/platform/airflow/README.md) |
| Cấu trúc code và migration | [Application structure](docs/architecture/code-structure.md) |
| Cách đóng góp | [CONTRIBUTING.md](CONTRIBUTING.md) |

## Cấu trúc thư mục

```text
docs/                   Tài liệu nguồn, nghiệp vụ, kiến trúc, model và runbook
infra/sources/          Compose cho nguồn dữ liệu được cung cấp
infra/platform/         Core lakehouse và Airflow/monitoring Compose
dags/                   Airflow DAGs
src/shopvn/             Package: ingestion, layers, marts, pipelines, jobs, orchestration
schemas/                Schema contract của nguồn và Kafka
sql/                    Trino views và truy vấn đối soát
scripts/                Bootstrap, profiling, backfill, công cụ tài liệu
tests/                  Unit/integration tests và fixture giả lập
.github/workflows/      CI kiểm tra repository
```

Các tầng được tách trong src/shopvn/transformations/bronze, silver, gold; Finance mart ở src/shopvn/marts và workflow ở src/shopvn/pipelines. Scripts chỉ giữ CLI wrappers và công cụ repository. Xem [bản đồ code](docs/architecture/code-structure.md).

## Chuẩn bị môi trường

Docker Desktop/WSL2 với Linux containers, Docker Compose và Git. Python cho application dự kiến là **3.11** trong môi trường riêng/container; Python hiện có trên máy không tự quyết định phiên bản runtime của dự án.

Chạy lệnh từ thư mục gốc repository. Nếu chưa có `.env`:

```powershell
Copy-Item .env.example .env
```

Điền credential của bộ nguồn vào `.env`. Máy hiện tại đã có `.env` cục bộ dựa trên cấu hình demo được cung cấp; file này bị Git bỏ qua. Credential mô tả trong tài liệu nguồn là của môi trường demo, không phải credential triển khai thực tế. Tự đổi giá trị trong `.env` không đảm bảo đổi được tài khoản đã được bake vào image nguồn.

## Kiểm tra và chạy nguồn

```powershell
docker compose --env-file .env -f infra/sources/docker-compose.yml config --quiet
docker compose --env-file .env -f infra/sources/docker-compose.yml up -d
docker compose --env-file .env -f infra/sources/docker-compose.yml ps
```

Các lệnh trên chỉ chạy nguồn, chưa chạy platform. Dừng service và giữ container:

```powershell
docker compose --env-file .env -f infra/sources/docker-compose.yml stop
```

Compose cũ được chuyển từ root sang `infra/sources/`. Các lệnh chạy Compose trong tài liệu nguồn gốc cần được thay bằng lệnh ở README này. Xem [lưu ý migration](docs/architecture/repository-migration.md) nếu máy đang có container từ cấu hình cũ; không chạy đồng thời hai bộ nguồn cùng host port.

Nguồn PostgreSQL/API giữ mặc định `linux/arm64` như cấu hình được cung cấp, có thể cấu hình qua `SHOPVN_SOURCE_PLATFORM`. Kiểm tra image hỗ trợ kiến trúc nào trước khi đổi sang `linux/amd64`.

Platform sau này kết nối qua service name trong network `shopvn-net`:

| Nguồn | Địa chỉ nội bộ | Trên máy host |
|---|---|---|
| PostgreSQL | `postgres:5432` | `localhost:5434` |
| Logistics API | `http://api:8000` | `http://localhost:8000` |
| SFTP | `sftp:22` | `localhost:2222` |

Application đọc PostgreSQL bằng `shopvn_reader`; API nguồn giữ cấu hình tài khoản như bản được cung cấp, tách biệt với tài khoản ingestion.

## Kiểm tra repository

```powershell
python -m pip install -r requirements-preparation.txt
python -m pip install --no-deps -e .
python scripts/check_repository.py
docker compose --env-file .env -f infra/sources/docker-compose.yml config --quiet
```

Kiểm tra này xác nhận cấu trúc, link và env/key riêng không bị track. Tài liệu nguồn gốc chỉ kiểm tra tồn tại, không kiểm tra placeholder trong template. Chạy pytest và integration runners theo [kết quả kiểm chứng](docs/verification/README.md); repository checks không thay thế runtime tests.

## Git và remote

Nhánh chính: `main`. Dùng nhánh ngắn và review trước khi merge. Airflow/monitoring ở commit `2430174`; đợt refactor ở `refactor/product-structure`, chưa tự push hoặc merge. Với bản clone chưa có remote, cấu hình như sau:

```powershell
git remote add origin <REMOTE_URL>
git push -u origin main
```

Giữ `.env`, dữ liệu thật, logs, checkpoints và volumes ngoài Git. Chỉ commit fixture nhỏ được giả lập.
