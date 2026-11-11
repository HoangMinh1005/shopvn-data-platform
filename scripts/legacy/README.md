# Công cụ ERD lịch sử

`generate_erd.mjs` chỉ tạo lại ERD của bản model cũ, không tạo baseline hiện hành.

Từ root:

```powershell
node scripts/legacy/generate_erd.mjs
```

Script luôn ghi [SVG legacy](../../docs/data-model/legacy/images/shopvn_gold_erd.svg) theo vị trí script, không phụ thuộc working directory. PNG cũ được giữ nguyên; script không tự tạo PNG.
