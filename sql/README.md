# SQL

[views/direct_finance.sql](views/direct_finance.sql): semantic queries cho certified Finance mart.

SQL đối soát PostgreSQL nằm trong [package ingestion](../src/shopvn/ingestion/sql/direct_revenue_baseline.sql) để được đóng gói cùng wheel và dùng độc lập checkout. Chỉ giữ một bản canonical; không copy lại vào sql/reconciliation.

Transformations Spark nằm ở src/shopvn/transformations; mart aggregation/publication ở src/shopvn/marts. Xem [cấu trúc application](../docs/architecture/code-structure.md).
