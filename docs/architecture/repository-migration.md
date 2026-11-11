# Tái cấu trúc repository — 2026-11-11

Đây là lịch sử migration tài liệu ban đầu. Đợt [refactor application](code-structure.md) sau đó đã bỏ generator ERD JavaScript chỉ dùng cho model cũ; các hình và tài liệu legacy vẫn giữ nguyên.

## Các file đã chuyển

| Vị trí cũ | Vị trí mới |
|---|---|
| 01_project_requirements.md | docs/sources/01_project_requirements.md |
| 02_dataset_guide.md | docs/sources/02_dataset_guide.md |
| 03_design_doc_template.md | docs/sources/03_design_doc_template.md |
| images/ERD.png, datasources.png | docs/sources/images/ |
| 04_data_model_design.md, 04_thiet_ke_mo_hinh_du_lieu.md | docs/data-model/legacy/ |
| images/shopvn_gold_erd.svg, .png | docs/data-model/legacy/images/ |
| 05_ke_hoach_thuc_hien_va_trien_khai.md | docs/architecture/legacy/ |
| scripts/generate_erd.mjs | scripts/legacy/generate_erd.mjs |
| docker-compose.yml | infra/sources/docker-compose.yml |

Nội dung các tài liệu và hình có sẵn được giữ nguyên. Generator cũ chỉ đổi cách tìm output theo vị trí script để không phụ thuộc working directory. Thư mục `tmp/` và `images/` cũ rỗng được giữ trên máy nếu có, không đưa vào Git.

## Thay đổi Compose

- Credential hardcode trong Compose được chuyển sang `.env` cục bộ bị Git ignore; `.env.example` chứa placeholder.
- Image nguồn và restart policy được giữ. Platform mặc định giữ arm64 như bản nguồn, cho phép cấu hình bằng env.
- Bỏ container_name cố định, healthcheck đọc biến môi trường, network có tên rõ `shopvn-net` để platform dùng external network.
- Chưa thêm volume vào PostgreSQL nguồn vì image có bootstrap riêng và cần kiểm tra hành vi trước khi đổi persistence.
- Không đổi tài khoản ứng dụng API nguồn; ingestion dùng tài khoản SELECT-only được mô tả trong dataset guide.

Nếu trước đây đã chạy Compose ở root, container/network có thể mang project name khác hoặc vẫn giữ host ports 5434/8000/2222. Kiểm tra `docker ps` và compose project labels trước khi khởi động cấu hình mới. Để dừng bộ cũ, xác định đúng project/container và dùng thao tác stop; không xóa volume hoặc reset nguồn. Việc tái cấu trúc này không tự dừng, xóa hoặc khởi động container nào.

## Phạm vi Git

Git repository được tạo cục bộ với branch main. Không tạo remote, không publish dữ liệu lên GitHub/GitLab. Credential demo vốn có trong tài liệu nguồn được giữ để bảo toàn bản gốc; credential cấu hình thực nằm trong `.env` bị bỏ qua.
