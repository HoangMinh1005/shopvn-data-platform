# ShopVN Data Platform

Dự án phân tích thương mại điện tử với Spark Lakehouse, Iceberg và Kafka mô phỏng. Nghiệp vụ v1 đã được duyệt ngày 2026-11-11; repository hiện ở giai đoạn chuẩn bị triển khai.

## Trạng thái hiện tại

- Đã tổ chức tài liệu, ghi nhận nghiệp vụ đã duyệt và khởi tạo Git repository.
- Có Compose cho ba nguồn PostgreSQL, Logistics API và SFTP.
- Stack platform đã được chọn ở mức thiết kế; chưa có platform Compose hoặc pipeline chạy end-to-end.
- ERD và kế hoạch cũ được giữ trong `legacy/`; đọc baseline hiện hành trước khi dùng lại.

## Tài liệu bắt đầu

| Nội dung | Tài liệu |
|---|---|
| Bản đồ tài liệu và thứ tự ưu tiên | [docs/README.md](docs/README.md) |
| Yêu cầu gốc | [Project requirements](docs/sources/01_project_requirements.md) |
| Mô tả nguồn | [Dataset guide](docs/sources/02_dataset_guide.md) |
| Quy tắc nghiệp vụ đã duyệt | [Business rules v1](docs/business/business-rules-v1.md) |
| Kiến trúc và stack đã chọn | [Architecture baseline](docs/architecture/baseline-v1.md) |
| Data model hiện hành và các điểm cần xác minh | [Model baseline](docs/data-model/README.md) |
| Công việc tiếp theo | [Implementation readiness](docs/architecture/implementation-readiness.md) |
| Cách đóng góp | [CONTRIBUTING.md](CONTRIBUTING.md) |

## Cấu trúc thư mục

```text
docs/                   Tài liệu nguồn, nghiệp vụ, kiến trúc, model và runbook
infra/sources/          Compose cho nguồn dữ liệu được cung cấp
infra/platform/         Chỗ dành cho platform Compose sau technical spike
infra/config/           Cấu hình service, không chứa secret
dags/                   Airflow DAGs
src/shopvn/             Python/PySpark modules
schemas/                Schema contract của nguồn và Kafka
sql/                    Trino views và truy vấn đối soát
scripts/                Bootstrap, profiling, backfill, công cụ tài liệu
tests/                  Unit/integration tests và fixture giả lập
.github/workflows/      CI kiểm tra repository
```

Các thư mục chứa `.gitkeep` mới chỉ là khung; chưa có implementation.

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
python scripts/check_repository.py
docker compose --env-file .env -f infra/sources/docker-compose.yml config --quiet
```

Kiểm tra này xác nhận cấu trúc, đường dẫn tài liệu được duy trì, env/key riêng không bị track và Compose parse được. Tài liệu nguồn gốc được kiểm tra tồn tại nhưng không kiểm tra các placeholder trong template. Chưa thay thế integration test, data profiling hoặc kiểm chứng stack. Xem [kế hoạch kiểm thử](tests/README.md).

## Git và remote

Nhánh chính: `main`. Dùng nhánh ngắn như `feat/source-profiling` và review trước khi merge. Repository được khởi tạo **cục bộ**; remote GitHub/GitLab chưa được cấu hình. Khi có URL remote của nhóm:

```powershell
git remote add origin <REMOTE_URL>
git push -u origin main
```

Giữ `.env`, dữ liệu thật, logs, checkpoints và volumes ngoài Git. Chỉ commit fixture nhỏ được giả lập.
