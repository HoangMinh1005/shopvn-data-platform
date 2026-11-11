# Kế hoạch thực hiện và triển khai ShopVN Data Platform

**Vai trò điều phối:** Nhóm trưởng  
**Giả định lập kế hoạch:** 4 thành viên, 8 tuần  
**Mục tiêu:** Xây dựng data platform batch + streaming chạy end-to-end, có thể demo ổn định và đáp ứng yêu cầu môn học  
**Trạng thái tech stack:** Chưa khóa hoàn toàn; kiến trúc mục tiêu là Spark Lakehouse, Iceberg/Delta, object storage, Airflow và Kafka

---

## 1. Mục tiêu và nguyên tắc thực hiện

### 1.1 Mục tiêu bắt buộc

1. Ingest đủ PostgreSQL, Logistics API và SFTP.
2. Có pipeline Bronze - Silver - Gold.
3. Gold trả lời được bốn nhóm use case Finance, Operations, Marketing và Product.
4. Rerun cùng ngày ít nhất ba lần không sinh duplicate và không thay đổi tổng tiền.
5. Data quality chạy trước khi publish analytics layer.
6. Backfill một khoảng ngày bất kỳ hoạt động an toàn.
7. Dashboard và log cho phép xác định trạng thái pipeline mà không SSH vào container.
8. Có runbook cho ít nhất ba loại sự cố.
9. PII không xuất hiện nguyên bản trong Gold.
10. Kafka được tích hợp như một luồng streaming mô phỏng có mục đích rõ ràng, không chỉ thêm vào để làm đẹp kiến trúc.

### 1.2 Nguyên tắc điều hành

- Batch hằng ngày là nguồn chính thức để publish báo cáo Finance trước 08:00.
- Kafka phục vụ sự kiện mô phỏng gần thời gian thực; không thay thế ba nguồn bắt buộc.
- Không đưa dữ liệu lớn qua XCom.
- Không join các fact chi tiết rồi cộng measure.
- Không hardcode credential.
- Mỗi bảng phải có grain, natural key, load strategy và owner.
- Mỗi KPI phải có metric contract trước khi tạo dashboard.
- Chốt kiến trúc theo decision record; không thay stack bằng thảo luận miệng.
- Mỗi pull request cần ít nhất một người khác review.

---

## 2. Kiến trúc mục tiêu

    PostgreSQL ───────┐
    Logistics API ───┼── Airflow extractors ── MinIO Landing
    SFTP CSV + MD5 ──┘                               │
                                                    ▼
                                           Spark batch jobs
                                                    │
    Event generator ── Kafka ── Spark Streaming ────┤
                                                    ▼
                                      Iceberg/Delta Lakehouse
                                      Bronze → Silver → Gold
                                                    │
                             ┌──────────────────────┼────────────────────┐
                             ▼                      ▼                    ▼
                           Trino                 Superset            DQ / Audit

    Airflow / Spark / Kafka / Catalog / Trino
                             │
                             ▼
                  Prometheus + Loki + Grafana

### 2.1 Vai trò của Kafka

Kafka chỉ nhận các sự kiện mô phỏng mới, ví dụ:

- shopvn.orders.v1
- shopvn.inventory.v1
- shopvn.orders.dlq

Không đẩy lại nguyên file SFTP qua Kafka. Không gọi REST API từ Spark executor. Không tạo cùng một order vừa qua Kafka vừa qua PostgreSQL batch nếu chưa có chiến lược CDC/deduplicate rõ ràng.

Luồng streaming sử dụng:

- event_id làm khóa idempotency;
- order_id làm Kafka message key cho order events;
- product_id làm key cho inventory events;
- event_time và schema_version trong payload;
- checkpoint riêng cho từng streaming query;
- watermark để xử lý late events;
- dead-letter topic cho message sai schema;
- at-least-once ở ingestion và deduplicate để đạt effectively-once ở Silver/Gold.

Streaming chỉ ghi Bronze/Silver near-real-time. Gold Finance chính thức chỉ publish sau reconciliation.

---

## 3. Tech stack dự kiến và decision gate

| Thành phần | Phương án mặc định | Phương án thay thế | Thời điểm khóa |
|---|---|---|---|
| Compute | Spark/PySpark | Không thay nếu đã bắt đầu code | Cuối ngày 2 |
| Table format | Apache Iceberg v2 | Delta Lake | Cuối ngày 3 |
| Object storage | MinIO | S3-compatible khác | Cuối ngày 3 |
| Catalog | Polaris REST Catalog | Nessie / JDBC catalog | Cuối tuần 1 |
| Orchestration | Airflow | Không thay | Cuối ngày 2 |
| Streaming | Kafka KRaft | Redpanda nếu thiếu tài nguyên | Cuối tuần 1 |
| Query engine | Trino | Spark SQL cho MVP | Cuối tuần 2 |
| BI | Superset | Metabase | Cuối tuần 2 |
| Metrics | Prometheus + Grafana | Không thay | Cuối tuần 2 |
| Logs | Loki + Grafana Alloy | File logs cho MVP | Cuối tuần 3 |
| CI | GitHub Actions | CI tương đương | Cuối tuần 1 |

### 3.1 Các ADR phải được chốt

| ADR | Quyết định cần chốt | Tiêu chí |
|---|---|---|
| ADR-001 | Iceberg hay Delta | Multi-engine, MERGE, catalog, thời gian triển khai |
| ADR-002 | Polaris hay Nessie | REST catalog, persistence, quyền, độ phức tạp local |
| ADR-003 | Airflow 2 hay 3 | Yêu cầu SLA của đề bài và provider compatibility |
| ADR-004 | Kafka dùng cho dữ liệu nào | Không duplicate với batch, giá trị demo rõ |
| ADR-005 | Gold sales facts | Base facts riêng nguồn và unified serving view |
| ADR-006 | Refund allocation | Không phân bổ canonical nếu nghiệp vụ chưa duyệt |
| ADR-007 | KPI revenue | Ordered GMV, recognized revenue, settlement dự kiến |

Sau khi ADR được Approved, thay đổi phải qua pull request và nêu ảnh hưởng tới code, dữ liệu, migration và deadline.

---

## 4. Phân công nhóm

Giả sử bốn thành viên A, B, C, D. Tên người thật được điền khi kickoff.

| Vai trò | Trách nhiệm chính | Trách nhiệm phụ |
|---|---|---|
| A - Team Lead / Architecture | Kiến trúc, ADR, Gold model, integration, review | Presentation, risk management |
| B - Platform / DevOps | Docker Compose, Spark, storage, catalog, Airflow, CI/CD | Monitoring infrastructure |
| C - Ingestion / Streaming | PostgreSQL, API, SFTP, Kafka producer/consumer | Failure injection, source contracts |
| D - Transformation / Quality / BI | Silver/Gold transforms, DQ, reconciliation, marts, dashboard | Test automation, documentation |

### 4.1 Quy tắc ownership

- Mỗi work item có một owner và một reviewer.
- Owner không tự approve pull request của mình.
- Module quan trọng phải có secondary owner để tránh phụ thuộc một người.
- Team Lead chịu trách nhiệm chốt grain và KPI, không tự ý thay logic Finance.

### 4.2 RACI cấp cao

| Hạng mục | A | B | C | D |
|---|---|---|---|---|
| Kiến trúc và ADR | A/R | C | C | C |
| Docker và platform | C | A/R | C | C |
| Batch ingestion | C | C | A/R | C |
| Kafka streaming | C | C | A/R | C |
| Data model Gold | A/R | C | C | R |
| Data quality | A | C | C | R |
| Monitoring | A | R | C | R |
| BI dashboard | A | C | C | R |
| CI/CD | A | R | C | C |
| Runbook và demo | A/R | R | R | R |

A = Accountable, R = Responsible, C = Consulted.

---

## 5. Kế hoạch 8 tuần

| Tuần | Milestone | Bằng chứng hoàn thành | Go/No-Go |
|---|---|---|---|
| 1 | Architecture baseline | ADR approved, core stack chạy, Spark ghi/đọc table | Không code pipeline lớn khi chưa khóa table format/catalog |
| 2 | Landing complete | Ba nguồn vào landing, manifest/checksum/audit đầy đủ | Không vào Silver nếu source accounting chưa khép kín |
| 3 | Trusted Silver | Accepted/rejected đối soát, PII được bảo vệ | Không vào Gold nếu grain/DQ chưa pass |
| 4 | Analytics-ready Gold | Dimensions/facts/marts và reconciliation pass | Không làm dashboard bằng số liệu chưa đối soát |
| 5 | Streaming MVP | Kafka → Spark Streaming → Bronze/Silver, dedup/DLQ | Không mở rộng topic nếu một luồng chưa ổn định |
| 6 | End-to-end product | Airflow, Trino, BI và monitoring hoạt động | Không hardening khi pipeline chưa chạy end-to-end |
| 7 | Reliability proof | Idempotency, backfill, failure và scale tests | Không release nếu recovery tạo duplicate |
| 8 | Release/demo | One-command bootstrap, runbook, dashboard, rehearsal | Chỉ tag release sau full clean run |

### Tuần 1 - Chốt kiến trúc và dựng nền tảng

### Mục tiêu

- Chốt ADR bắt buộc.
- Dựng skeleton repository và môi trường core.
- Chứng minh Spark có thể tạo và đọc một table trên object storage qua catalog.

### Công việc

**A**

- Chốt grain và danh sách Gold base facts.
- Chốt metric contract bản đầu.
- Viết ADR-001 đến ADR-005.
- Chia Epic và issue.

**B**

- Tạo pipeline Docker Compose riêng, join shopvn-net.
- Dựng platform PostgreSQL, MinIO, Spark master/worker, catalog và Airflow.
- Tạo bucket và namespace.
- Thêm healthcheck cho từng service.

**C**

- Kiểm thử kết nối PostgreSQL, API và SFTP từ pipeline network.
- Định nghĩa source contract và naming convention.
- Định nghĩa Kafka event schema v1.

**D**

- Viết test framework skeleton.
- Tạo DQ result schema và reconciliation template.
- Chuẩn hóa cấu trúc SQL/PySpark transform.

### Exit criteria

- docker compose core khởi động thành công.
- Spark ghi/đọc được table test.
- Airflow submit được Spark job hello-world.
- Credential chỉ lấy từ môi trường/connection.
- ADR bắt buộc ở trạng thái Approved.
- CI chạy lint và unit test skeleton.

---

### Tuần 2 - Landing và batch ingestion

### Mục tiêu

Ingest đủ ba nguồn vào landing zone, có manifest, checksum và audit.

### Công việc

- PostgreSQL incremental/full-snapshot extractor.
- API extractor với batch tối đa 50 IDs, retry 429/timeout/500 và skip 404.
- SFTP extractor stream file, kiểm tra MD5 trước khi publish.
- Landing path chuẩn:

      landing/<source>/<object>/ingestion_date=YYYY-MM-DD/batch_id=<id>/

- Tạo:
  - ops.pipeline_run
  - ops.source_extract_audit
  - ops.source_file_manifest
  - ops.api_request_result
- Log row count, byte count, checksum, watermark và duration.

### Exit criteria

- Tất cả 9 PostgreSQL tables được landing.
- API accounting: found + not_found + failed_terminal bằng requested.
- File corrupt bị quarantine, không được xử lý.
- Missing file được log và đánh dấu coverage partial.
- Rerun cùng logical date không tạo file publish trùng không kiểm soát.

---

### Tuần 3 - Bronze, Silver và Data Quality

### Mục tiêu

Tạo nguồn dữ liệu sạch, giữ grain và có cơ chế accepted/rejected.

### Công việc

- Tạo Bronze tables có lineage:
  - source_system
  - source_object
  - source_file
  - source_row_number
  - record_hash
  - ingested_at
  - batch_id
- Tạo Silver source-conformed tables.
- Chuẩn hóa timestamp về cùng timezone.
- Mask/drop PII khỏi Silver unrestricted.
- Tạo ops.rejected_record và file quarantine JSONL.
- Kiểm tra:
  - PK uniqueness
  - required null
  - domain/status
  - FK dependency
  - amount rule
  - row reconciliation
- Không tự sửa subtotal để làm khớp item totals.

### Exit criteria

- input_rows = accepted_rows + rejected_rows.
- Mọi reject có reason code và raw lineage.
- Zero quantity không đóng góp units sold.
- Discount lớn hơn subtotal bị đánh dấu và loại khỏi revenue.
- Cost lớn hơn price vẫn được giữ cùng business flag.
- Silver rerun idempotent.

---

### Tuần 4 - Gold dimensions, facts và marts

### Mục tiêu

Materialize Gold star schema và trả lời các business questions cốt lõi.

### Dimensions

- dim_date
- dim_customer SCD2
- dim_product SCD2
- dim_channel
- dim_voucher
- dim_carrier
- dim_geography
- dim_warehouse

### Base facts

- fct_direct_order
- fct_direct_order_item
- fct_marketplace_sale_line
- fct_shipment
- fct_return
- fct_inventory_transaction
- fct_inventory_daily_snapshot
- fct_product_review

### Serving layer

- fct_sales_line_unified
- mart_daily_revenue
- mart_customer_spend_daily/monthly
- mart_campaign_daily
- mart_delivery_daily_carrier
- mart_product_channel_daily
- mart_inventory_health_daily

### Quy tắc bắt buộc

- Marketplace customer_key bằng unknown key 0.
- Settlement date là ngày dự kiến, không gọi là cash received.
- Refund chỉ trừ khi status refunded.
- Không phân bổ refund xuống product trong canonical fact.
- Order-level discount/shipping không bị nhân theo line.
- Inventory snapshot là semi-additive.
- Customer tier trước go-live là unknown/unreliable.

### Exit criteria

- Gold grain tests pass.
- SCD2 không có khoảng hiệu lực chồng lấn.
- Direct Finance total đối soát về source.
- Marketplace total đối soát theo partner/date/file.
- Ba lần rerun cho kết quả count/sum giống nhau.
- Backfill một khoảng ba ngày không ảnh hưởng ngày ngoài phạm vi.

---

### Tuần 5 - Kafka và Spark Structured Streaming

### Mục tiêu

Có một luồng streaming hoàn chỉnh và có giá trị demo, không gây duplicate với batch.

### Công việc

- Dựng Kafka KRaft single-node hoặc cluster nhỏ.
- Tạo event generator có tốc độ cấu hình được.
- Topic:
  - shopvn.orders.v1
  - shopvn.inventory.v1
  - shopvn.orders.dlq
- Định nghĩa JSON Schema hoặc Avro contract.
- Producer dùng event_id duy nhất.
- Spark Structured Streaming:
  - validate schema;
  - route invalid event sang DLQ;
  - watermark;
  - deduplicate theo event_id;
  - checkpoint trên object storage;
  - ghi Bronze streaming table;
  - micro-batch transform sang Silver.
- Test duplicate, out-of-order, late event và restart consumer.

### Exit criteria

- Restart streaming job không mất offset/checkpoint.
- Gửi trùng event_id không tạo duplicate Silver.
- Event quá muộn được xử lý theo policy đã viết.
- Event sai schema vào DLQ và có metric.
- Batch và streaming không dùng trùng namespace/order ID ngoài thiết kế.
- Không tuyên bố exactly-once end-to-end nếu chưa chứng minh.

---

### Tuần 6 - Airflow orchestration, Trino, BI và monitoring

### Mục tiêu

Chạy end-to-end theo lịch, truy vấn được qua Trino và quan sát được trên dashboard.

### DAG đề xuất

    wait_for_sources
        ├── extract_postgres
        ├── extract_logistics_api
        └── extract_sftp
                  │
                  ▼
          validate_landing
                  │
                  ▼
             load_bronze
                  │
                  ▼
          bronze_quality_gate
                  │
                  ▼
          transform_silver
                  │
                  ▼
          silver_quality_gate
                  │
                  ▼
          load_dimensions
                  │
                  ▼
              load_facts
                  │
                  ▼
         gold_reconciliation
                  │
                  ▼
             build_marts
                  │
                  ▼
           publish_metrics

### Dashboard nghiệp vụ

- Finance: revenue, discount, refund, commission, coverage.
- Operations: carrier success/failure, delivery duration, retry attempts.
- Marketing: spend, tier, campaign, repeat rate.
- Product: units, loss flag, inventory, stockout forecast.

### Dashboard kỹ thuật

- Last successful run.
- Data freshness.
- DAG/task duration.
- DQ failures.
- Source vs Gold revenue difference.
- Missing/corrupt SFTP files.
- API status distribution.
- Kafka lag, throughput, DLQ count.
- Spark job/executor status.

### Exit criteria

- Pipeline theo schedule hoàn tất trước SLA trên dữ liệu mẫu.
- Dashboard trả lời được các câu hỏi observability trong yêu cầu.
- Analyst chỉ truy cập Gold/marts, không truy cập raw PII.
- Alert test gửi thành công.

---

### Tuần 7 - Hardening, performance và failure testing

### Mục tiêu

Chứng minh độ tin cậy, khả năng scale và phục hồi sự cố.

### Test bắt buộc

1. Idempotency: chạy cùng ngày ba lần.
2. Backfill: chạy lại một khoảng lịch sử.
3. Flash sale: dữ liệu gấp ba lần ngày thường.
4. Scale test: dữ liệu mô phỏng lớn hơn dataset mẫu.
5. API 429, timeout, 500, 404.
6. SFTP missing và corrupt.
7. Kafka duplicate, late, out-of-order và DLQ.
8. Restart Spark worker giữa job.
9. Restart Kafka/streaming consumer.
10. Catalog/object storage tạm thời không khả dụng.
11. Schema thêm cột.
12. Schema đổi kiểu không tương thích.

### Iceberg/Lakehouse maintenance

- Compact small files.
- Rewrite manifests nếu cần.
- Expire snapshot theo retention.
- Remove orphan files với ngưỡng an toàn.
- Không vacuum/expire quá sớm làm mất khả năng backfill/time travel.

### Exit criteria

- Có báo cáo benchmark.
- Flash sale vẫn đạt SLA hoặc có kế hoạch capacity rõ.
- Ít nhất ba runbook được diễn tập.
- Recovery không tạo duplicate.
- Schema mismatch không âm thầm cast sai.

---

### Tuần 8 - Đóng gói, tài liệu và demo

### Mục tiêu

Tạo một bản release tái lập được và trình diễn ổn định.

### Công việc

- Pin Docker image versions.
- Tạo one-command bootstrap.
- Tạo dữ liệu demo cố định.
- Hoàn thiện README, design document, ADR, runbook.
- Cập nhật architecture diagram và ERD.
- Viết demo script theo phút.
- Chạy rehearsal ít nhất hai lần.
- Đóng issue không còn sử dụng.
- Tạo release tag.

### Exit criteria

- Máy mới có thể chạy theo README.
- Không có secret trong repository.
- Full pipeline chạy end-to-end.
- Dashboard có dữ liệu.
- Runbook đã thử.
- Slide và demo thống nhất với implementation.

---

## 6. Backlog theo Epic

| Epic | Nội dung | Mức ưu tiên |
|---|---|---|
| ARC | ADR, architecture, data contracts, metric contracts | P0 |
| INF | Docker, network, storage, catalog, Spark | P0 |
| ING-PG | PostgreSQL ingestion | P0 |
| ING-API | Logistics API ingestion | P0 |
| ING-SFTP | SFTP ingestion và checksum | P0 |
| BRZ | Bronze tables và lineage | P0 |
| SLV | Cleaning, validation, PII handling | P0 |
| DQ | Quality gates, quarantine, reconciliation | P0 |
| GLD | Dimensions, facts, SCD2, marts | P0 |
| ORC | Airflow DAG, retry, backfill, alert | P0 |
| STR | Kafka và Structured Streaming | P1 |
| QRY | Trino | P1 |
| BI | Superset dashboards | P1 |
| OBS | Prometheus, Grafana, Loki | P1 |
| CICD | Test, lint, image build, release | P1 |
| LIN | OpenLineage/Marquez | P2 |
| DOC | Design, runbook, demo, presentation | P0 |

P0 hoàn thành trước. P1 chỉ được mở rộng sau khi P0 end-to-end chạy. P2 chỉ làm khi không đe dọa deadline.

---

## 7. Môi trường triển khai

### 7.1 Docker Compose profiles

**core**

- platform-postgres
- minio
- catalog
- spark-master
- spark-worker
- airflow scheduler/web/API
- kafka

**analytics**

- trino
- superset

**observability**

- prometheus
- grafana
- loki
- grafana-alloy

**streaming**

- event-generator
- streaming-job

### 7.2 Tối ưu tài nguyên local

- Dùng một platform PostgreSQL instance nhưng tách database cho Airflow, catalog và Superset.
- Spark bắt đầu một master và một worker.
- Trino local dùng single coordinator/worker cho demo.
- Chỉ bật analytics và observability khi cần.
- Máy nên có ít nhất 16 GB RAM; nếu thấp hơn, chạy theo profile.

### 7.3 Bucket/namespace

Buckets:

- shopvn-landing
- shopvn-warehouse
- shopvn-checkpoints
- shopvn-quarantine
- shopvn-logs

Catalog namespaces:

- bronze
- silver
- gold
- ops

---

## 8. Repository structure đề xuất

    final_project/
    ├── dags/
    ├── jobs/
    │   ├── bronze/
    │   ├── silver/
    │   ├── gold/
    │   ├── streaming/
    │   └── maintenance/
    ├── extractors/
    │   ├── postgres/
    │   ├── api/
    │   └── sftp/
    ├── common/
    │   ├── config/
    │   ├── logging/
    │   ├── dq/
    │   └── metrics/
    ├── sql/
    ├── tests/
    │   ├── unit/
    │   ├── integration/
    │   ├── contracts/
    │   └── performance/
    ├── infra/
    │   ├── airflow/
    │   ├── spark/
    │   ├── catalog/
    │   ├── kafka/
    │   ├── trino/
    │   ├── superset/
    │   └── observability/
    ├── dashboards/
    ├── docs/
    │   ├── adr/
    │   ├── runbooks/
    │   └── diagrams/
    ├── scripts/
    ├── docker-compose.yml
    ├── docker-compose.override.yml
    ├── Makefile
    ├── pyproject.toml
    └── README.md

---

## 9. Git workflow và CI/CD

### 9.1 Branching

- main: luôn ở trạng thái demo được.
- develop: integration branch nếu nhóm cần.
- feature/<issue-id>-<short-name>: nhánh tính năng.
- fix/<issue-id>-<short-name>: nhánh sửa lỗi.

Không merge trực tiếp vào main.

### 9.2 Pull request checklist

- Có issue liên kết.
- Mô tả grain/behavior thay đổi.
- Có unit hoặc integration test.
- Không có credential.
- Rerun không tạo duplicate.
- Có log và error handling.
- Có cập nhật tài liệu nếu schema/KPI thay đổi.
- Reviewer đã kiểm tra query plan hoặc sample output.

### 9.3 CI pipeline

1. Ruff/lint.
2. Pytest unit.
3. SQL lint.
4. Airflow DAG import test.
5. Docker Compose validation.
6. Spark local transformation test.
7. Contract test.
8. Build images.
9. Integration smoke test theo lịch hoặc khi merge.

---

## 10. Definition of Done

Một task chỉ Done khi:

- Code đã merge.
- Test pass.
- Grain và key được ghi rõ.
- Idempotency được chứng minh.
- Log có batch_id và business key cần thiết.
- DQ rule và reconciliation được cập nhật.
- Không lộ PII/secret.
- Metric/alert được thêm nếu task ảnh hưởng vận hành.
- Tài liệu và runbook được cập nhật.
- Owner khác có thể chạy lại theo hướng dẫn.

Một milestone chỉ Done khi:

- Có demo được.
- Có số liệu đối soát.
- Có failure case được kiểm thử.
- Không phụ thuộc máy của một thành viên.

---

## 11. KPI contract cần chốt

Mỗi KPI phải ghi:

| Trường | Ví dụ |
|---|---|
| KPI | recognized_marketplace_revenue |
| Fact | fct_marketplace_sale_line |
| Grain | partner + order + product + SKU |
| Date role | order_date |
| Status filter | completed |
| Formula | source reported net sau reconciliation |
| Exclusion | corrupt/missing file, invalid amount |
| Additivity | cộng theo ngày, partner, product |
| Coverage | partial nếu thiếu file |
| Owner | Finance |

KPI ưu tiên:

- Ordered GMV.
- Direct net revenue after refunded returns.
- Marketplace recognized net revenue.
- Expected settlement amount.
- Discount cost.
- Commission cost.
- Delivery success/failure rate.
- Average successful delivery duration.
- Repeat purchase within 30 days.
- End-of-day stock.
- Seven-day sell-through.
- Days to stockout.

---

## 12. Runbook tối thiểu

### RB-01 SFTP checksum mismatch

- Detect: MD5 check fail.
- Impact: partner/date bị thiếu.
- Fix: không process file; chờ/thay file.
- Rerun: cùng logical date và manifest key.
- Verify: checksum, row count, amount reconciliation.

### RB-02 Logistics API rate limit/timeout

- Detect: 429 hoặc timeout.
- Impact: shipment coverage partial.
- Fix: retry đúng policy, giảm concurrency.
- Rerun: chỉ missing order IDs.
- Verify: found + not_found + terminal failures bằng requested.

### RB-03 Gold reconciliation failure

- Detect: source và Gold amount lệch.
- Impact: chặn publish mart.
- Fix: truy theo batch_id, file, partner, date.
- Rerun: rebuild partition bị ảnh hưởng.
- Verify: count/sum và DQ pass.

### RB-04 Kafka consumer/checkpoint failure

- Detect: consumer lag tăng hoặc stream stopped.
- Impact: near-real-time delayed; daily batch chưa chắc bị ảnh hưởng.
- Fix: kiểm tra checkpoint, schema, broker.
- Rerun: resume từ checkpoint; không xóa checkpoint tùy tiện.
- Verify: event_id unique, offsets tiến, DLQ hợp lý.

### RB-05 Small-files degradation

- Detect: số file tăng nhanh, query planning chậm.
- Impact: Spark/Trino latency.
- Fix: compact data files và manifests.
- Verify: file count/size và query duration cải thiện.

---

## 13. Risk register

| Risk | Xác suất | Ảnh hưởng | Cách giảm thiểu |
|---|---|---|---|
| Stack quá nặng cho máy | Cao | Cao | Compose profiles, giảm worker, bật theo nhu cầu |
| Đổi table format giữa dự án | Trung bình | Cao | Chốt ADR tuần 1 |
| Kafka không có use case rõ | Trung bình | Trung bình | Chỉ dùng order/inventory simulator |
| Batch và stream duplicate | Cao | Cao | Namespace, event_id, source contract, dedup |
| Gold metric không thống nhất | Cao | Cao | Metric contract và Finance sign-off |
| Fan-out khi join fact | Cao | Cao | Grain tests, semantic views, drill-across |
| Marketplace settlement bị hiểu sai | Cao | Cao | Đặt tên expected/scheduled, không gọi cash |
| Refund product không xác định | Cao | Trung bình | Không phân bổ canonical |
| SCD2 tạo lịch sử giả | Trung bình | Cao | unknown pre-go-live, history_reliable |
| Small files từ streaming | Cao | Trung bình | Micro-batch hợp lý, compaction |
| Deadline thiếu thời gian | Trung bình | Cao | P0 trước, cắt P2 |
| Demo phụ thuộc internet | Trung bình | Cao | Pin image và chuẩn bị dữ liệu local |

---

## 14. Nhịp làm việc

### Hằng ngày

- Stand-up 15 phút:
  - hôm qua hoàn thành gì;
  - hôm nay làm gì;
  - blocker;
  - có ảnh hưởng grain/KPI/schema không.

### Hai lần mỗi tuần

- Integration window: merge và chạy full smoke test.
- Architecture/Data review: xem ADR, schema và reconciliation.

### Cuối tuần

- Demo nội bộ.
- So sánh milestone với exit criteria.
- Cập nhật risk register.
- Chốt scope tuần tiếp theo.

### Quy tắc escalation

- Blocker quá bốn giờ phải báo nhóm.
- Blocker quá một ngày phải có quyết định của Team Lead.
- Lỗi data correctness ưu tiên hơn tính năng mới.
- Không sửa số liệu bằng hardcode để qua demo.

---

## 15. Demo flow cuối dự án

1. Hiển thị ba nguồn batch.
2. Trigger Airflow DAG.
3. Cho thấy checksum và API retry.
4. Hiển thị Bronze, Silver, rejected records.
5. Chạy Gold reconciliation.
6. Mở Superset dashboard.
7. Gửi Kafka order event.
8. Cho thấy event đến Bronze/Silver streaming.
9. Gửi duplicate và invalid event.
10. Chứng minh dedup và DLQ.
11. Chạy rerun cùng ngày.
12. Chứng minh count/sum không đổi.
13. Mở Grafana để xem SLA, DQ, Kafka lag và revenue reconciliation.

---

## 16. Thứ tự ưu tiên khi phải cắt scope

Giữ bằng mọi giá:

1. Ba nguồn bắt buộc.
2. Bronze/Silver/Gold.
3. Idempotency và reconciliation.
4. Data quality và failure handling.
5. Airflow và backfill.
6. Gold model đúng grain.
7. Monitoring cơ bản.

Cắt theo thứ tự:

1. OpenLineage/Marquez.
2. Schema Registry riêng.
3. Kafka inventory topic; giữ một order topic.
4. Superset dashboard phụ.
5. Loki; giữ log Airflow/Spark và Prometheus.
6. Nhiều Spark/Kafka nodes; giữ single-node demo.

Kafka có thể được thu gọn còn một topic và một streaming query, nhưng không được làm ảnh hưởng chất lượng batch pipeline cốt lõi.
