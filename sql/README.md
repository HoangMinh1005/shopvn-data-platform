# SQL

`views/`: certified Trino views và semantic queries. `reconciliation/`: so sánh count/amount theo nguồn, ngày và grain. Transform Spark thuộc `src/shopvn/transformations/`.

Đã có direct revenue baseline SELECT dùng trong transaction nguồn và Trino SELECT cho mart FIN02 đã kiểm chứng. Các KPI khác chưa có SQL implementation. Mỗi query mới phải tham chiếu business-rule ID tương ứng.
