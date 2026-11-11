import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Historical diagram only. See docs/data-model/README.md for the current baseline.

const width = 2600;
const height = 1860;
const rowHeight = 28;
const headerHeight = 48;
const tableTitleHeight = 28;

const entities = [
  {
    id: 'dim_date', kind: 'dimension', x: 40, y: 105, w: 275,
    title: 'DIM_DATE', subtitle: 'Một dòng / ngày',
    columns: [
      ['PK', 'date_key', 'INT'],
      ['', 'full_date', 'DATE'],
      ['', 'month_key', 'INT'],
      ['', 'quarter', 'TINYINT'],
      ['', 'is_flash_sale', 'BOOLEAN']
    ]
  },
  {
    id: 'dim_customer', kind: 'dimension', x: 350, y: 105, w: 310,
    title: 'DIM_CUSTOMER', subtitle: 'SCD Type 2 · Không chứa PII gốc',
    columns: [
      ['PK', 'customer_key', 'BIGINT'],
      ['', 'customer_ref', 'STRING'],
      ['', 'loyalty_tier', 'STRING'],
      ['', 'age_band', 'STRING'],
      ['', 'valid_from / valid_to', 'TIMESTAMP'],
      ['', 'is_current', 'BOOLEAN']
    ]
  },
  {
    id: 'dim_channel', kind: 'dimension', x: 700, y: 105, w: 275,
    title: 'DIM_CHANNEL', subtitle: 'Direct và Marketplace',
    columns: [
      ['PK', 'channel_key', 'INT'],
      ['', 'channel_group', 'STRING'],
      ['', 'channel_name', 'STRING']
    ]
  },
  {
    id: 'dim_voucher', kind: 'dimension', x: 1010, y: 105, w: 300,
    title: 'DIM_VOUCHER', subtitle: 'Một dòng / voucher',
    columns: [
      ['PK', 'voucher_key', 'BIGINT'],
      ['', 'voucher_code', 'STRING'],
      ['', 'campaign_id', 'STRING'],
      ['', 'voucher_type', 'STRING'],
      ['', 'voucher_value', 'DECIMAL']
    ]
  },
  {
    id: 'dim_product', kind: 'dimension', x: 1350, y: 105, w: 335,
    title: 'DIM_PRODUCT', subtitle: 'SCD Type 2 từ ngày go-live',
    columns: [
      ['PK', 'product_key', 'BIGINT'],
      ['NK', 'product_id', 'BIGINT'],
      ['', 'sku', 'STRING'],
      ['', 'category / brand', 'STRING'],
      ['', 'base_price / cost_price', 'DECIMAL'],
      ['', 'is_catalog_loss_making', 'BOOLEAN'],
      ['', 'valid_from / valid_to', 'TIMESTAMP']
    ]
  },
  {
    id: 'dim_carrier', kind: 'dimension', x: 1725, y: 105, w: 270,
    title: 'DIM_CARRIER', subtitle: 'GHN · GHTK · Unknown',
    columns: [
      ['PK', 'carrier_key', 'INT'],
      ['', 'carrier_code', 'STRING'],
      ['', 'carrier_name', 'STRING']
    ]
  },
  {
    id: 'dim_geography', kind: 'dimension', x: 2030, y: 105, w: 285,
    title: 'DIM_GEOGRAPHY', subtitle: 'Địa chỉ đã chuẩn hóa',
    columns: [
      ['PK', 'geography_key', 'BIGINT'],
      ['', 'province', 'STRING'],
      ['', 'district', 'STRING']
    ]
  },
  {
    id: 'dim_warehouse', kind: 'dimension', x: 2345, y: 105, w: 215,
    title: 'DIM_WAREHOUSE', subtitle: 'Một dòng / kho',
    columns: [
      ['PK', 'warehouse_key', 'INT'],
      ['NK', 'warehouse_id', 'STRING']
    ]
  },
  {
    id: 'fct_direct_order', kind: 'fact', x: 175, y: 535, w: 425,
    title: 'FCT_DIRECT_ORDER', subtitle: 'Grain: một đơn direct',
    columns: [
      ['PK', 'order_id', 'STRING'],
      ['FK', 'order_date_key', 'INT'],
      ['FK', 'customer_key', 'BIGINT'],
      ['FK', 'channel_key', 'INT'],
      ['FK', 'voucher_key', 'BIGINT'],
      ['', 'total_amount_source', 'DECIMAL'],
      ['', 'refunded_amount', 'DECIMAL'],
      ['', 'net_revenue_after_refund', 'DECIMAL'],
      ['', 'is_revenue_eligible', 'BOOLEAN']
    ]
  },
  {
    id: 'fct_direct_order_item', kind: 'fact', x: 690, y: 535, w: 430,
    title: 'FCT_DIRECT_ORDER_ITEM', subtitle: 'Grain: một item_id',
    columns: [
      ['PK', 'item_id', 'BIGINT'],
      ['FK', 'order_id', 'STRING'],
      ['FK', 'product_key', 'BIGINT'],
      ['', 'quantity_source / units_sold', 'INT'],
      ['', 'unit_price', 'DECIMAL'],
      ['', 'allocated_order_discount', 'DECIMAL'],
      ['', 'allocated_refund_amount', 'DECIMAL'],
      ['', 'net_merchandise_revenue', 'DECIMAL'],
      ['', 'is_line_loss_making', 'BOOLEAN']
    ]
  },
  {
    id: 'fct_marketplace_sale_line', kind: 'fact', x: 1210, y: 535, w: 450,
    title: 'FCT_MARKETPLACE_SALE_LINE', subtitle: 'Grain: partner + order + product + SKU',
    columns: [
      ['PK', 'marketplace_line_key', 'STRING'],
      ['FK', 'order_date_key', 'INT'],
      ['FK', 'settlement_date_key', 'INT'],
      ['FK', 'product_key', 'BIGINT'],
      ['FK', 'channel_key', 'INT'],
      ['', 'quantity_sold', 'INT'],
      ['', 'platform_discount_amount', 'DECIMAL'],
      ['', 'commission_amount', 'DECIMAL'],
      ['', 'recognized_net_revenue', 'DECIMAL'],
      ['', 'status', 'STRING']
    ]
  },
  {
    id: 'fct_shipment', kind: 'fact', x: 1770, y: 535, w: 435,
    title: 'FCT_SHIPMENT', subtitle: 'Grain: shipment mới nhất / direct order',
    columns: [
      ['PK/FK', 'order_id', 'STRING'],
      ['FK', 'carrier_key', 'INT'],
      ['FK', 'geography_key', 'BIGINT'],
      ['FK', 'shipped_date_key', 'INT'],
      ['FK', 'delivery_date_key', 'INT'],
      ['', 'delivery_attempts', 'INT'],
      ['', 'failure_reason', 'STRING'],
      ['', 'delivery_duration_hours', 'DECIMAL']
    ]
  },
  {
    id: 'fct_return', kind: 'fact', x: 120, y: 1040, w: 390,
    title: 'FCT_RETURN', subtitle: 'Grain: một yêu cầu trả hàng',
    columns: [
      ['PK', 'return_id', 'BIGINT'],
      ['FK', 'order_id', 'STRING'],
      ['FK', 'created_date_key', 'INT'],
      ['', 'return_reason', 'STRING'],
      ['', 'return_status', 'STRING'],
      ['', 'refunded_amount', 'DECIMAL']
    ]
  },
  {
    id: 'fct_product_review', kind: 'fact', x: 575, y: 1040, w: 405,
    title: 'FCT_PRODUCT_REVIEW', subtitle: 'Grain: một review_id',
    columns: [
      ['PK', 'review_id', 'BIGINT'],
      ['FK', 'review_date_key', 'INT'],
      ['FK', 'product_key', 'BIGINT'],
      ['FK', 'customer_key', 'BIGINT'],
      ['FK', 'order_id', 'STRING'],
      ['', 'rating', 'TINYINT'],
      ['', 'has_comment', 'BOOLEAN']
    ]
  },
  {
    id: 'fct_inventory_transaction', kind: 'fact', x: 1045, y: 1040, w: 420,
    title: 'FCT_INVENTORY_TRANSACTION', subtitle: 'Grain: một stock movement',
    columns: [
      ['PK', 'txn_id', 'BIGINT'],
      ['FK', 'product_key', 'BIGINT'],
      ['FK', 'order_id (nullable)', 'STRING'],
      ['FK', 'transaction_date_key', 'INT'],
      ['', 'transaction_type', 'STRING'],
      ['', 'qty_change', 'INT']
    ]
  },
  {
    id: 'fct_inventory_daily_snapshot', kind: 'fact', x: 1530, y: 1040, w: 455,
    title: 'FCT_INVENTORY_DAILY_SNAPSHOT', subtitle: 'Grain: ngày × sản phẩm × kho',
    columns: [
      ['PK/FK', 'snapshot_date_key', 'INT'],
      ['PK/FK', 'product_key', 'BIGINT'],
      ['PK/FK', 'warehouse_key', 'INT'],
      ['', 'stock_qty_eod', 'INT'],
      ['', 'units_sold_7d', 'INT'],
      ['', 'avg_daily_units_sold_7d', 'DECIMAL'],
      ['', 'days_to_stockout', 'DECIMAL']
    ]
  },
  {
    id: 'fct_sales_line_unified', kind: 'derived', x: 720, y: 1480, w: 1110,
    title: 'FCT_SALES_LINE_UNIFIED', subtitle: 'Derived UNION ALL · dùng so sánh kênh/sản phẩm, không thay thế fact nguồn',
    columns: [
      ['KEY', 'source_line_id / source_order_id', 'STRING'],
      ['FK', 'order_date_key / channel_key', 'INT'],
      ['FK', 'customer_key / product_key', 'BIGINT'],
      ['', 'units_sold / gross_amount / discount_amount', 'DECIMAL'],
      ['', 'commission_amount / refund_amount / net_revenue', 'DECIMAL'],
      ['', 'data_coverage_status', 'STRING']
    ],
    wide: true
  }
];

for (const e of entities) {
  e.h = headerHeight + tableTitleHeight + e.columns.length * rowHeight + 10;
}

const byId = Object.fromEntries(entities.map((e) => [e.id, e]));

function esc(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function anchor(id, side, fraction = 0.5) {
  const e = byId[id];
  if (side === 'top') return [e.x + e.w * fraction, e.y];
  if (side === 'bottom') return [e.x + e.w * fraction, e.y + e.h];
  if (side === 'left') return [e.x, e.y + e.h * fraction];
  return [e.x + e.w, e.y + e.h * fraction];
}

function makePath(points) {
  return points.map((p, i) => (i === 0 ? 'M ' : 'L ') + p[0] + ' ' + p[1]).join(' ');
}

function orthogonal(from, to, orientation = 'vertical', offset = 0) {
  if (orientation === 'vertical') {
    const mid = (from[1] + to[1]) / 2 + offset;
    return makePath([from, [from[0], mid], [to[0], mid], to]);
  }
  const mid = (from[0] + to[0]) / 2 + offset;
  return makePath([from, [mid, from[1]], [mid, to[1]], to]);
}

const edges = [];
function addEdge(fromId, fromSide, fromFraction, toId, toSide, toFraction, label, options = {}) {
  const from = anchor(fromId, fromSide, fromFraction);
  const to = anchor(toId, toSide, toFraction);
  edges.push({
    d: options.points ? makePath(options.points) : orthogonal(from, to, options.orientation || 'vertical', options.offset || 0),
    label,
    lx: options.lx,
    ly: options.ly,
    cls: options.cls || 'relationship'
  });
}

// Conformed dimensions to facts.
addEdge('dim_customer', 'bottom', 0.35, 'fct_direct_order', 'top', 0.52, 'customer_key', { offset: -5, lx: 470, ly: 475 });
addEdge('dim_customer', 'bottom', 0.75, 'fct_product_review', 'top', 0.32, 'customer_key', {
  points: [[582, 359], [582, 430], [650, 430], [650, 1010], [705, 1010], [705, 1040]],
  lx: 690, ly: 1000
});
addEdge('dim_channel', 'bottom', 0.35, 'fct_direct_order', 'top', 0.78, 'channel_key', { offset: 20, lx: 830, ly: 458 });
addEdge('dim_channel', 'bottom', 0.72, 'fct_marketplace_sale_line', 'top', 0.48, 'channel_key', { offset: 45, lx: 1090, ly: 485 });
addEdge('dim_voucher', 'bottom', 0.45, 'fct_direct_order', 'top', 0.92, 'voucher_key', { offset: 70, lx: 965, ly: 505 });
addEdge('dim_product', 'bottom', 0.12, 'fct_direct_order_item', 'top', 0.72, 'product_key', { offset: 10, lx: 1280, ly: 452 });
addEdge('dim_product', 'bottom', 0.35, 'fct_marketplace_sale_line', 'top', 0.68, 'product_key', { offset: 48, lx: 1485, ly: 493 });
addEdge('dim_product', 'bottom', 0.58, 'fct_product_review', 'top', 0.72, 'product_key', {
  points: [[1544, 387], [1544, 995], [867, 995], [867, 1040]],
  lx: 1200, ly: 985
});
addEdge('dim_product', 'bottom', 0.76, 'fct_inventory_transaction', 'top', 0.55, 'product_key', {
  points: [[1605, 387], [1605, 975], [1276, 975], [1276, 1040]],
  lx: 1430, ly: 965
});
addEdge('dim_product', 'bottom', 0.92, 'fct_inventory_daily_snapshot', 'top', 0.35, 'product_key', {
  points: [[1658, 387], [1658, 980], [1689, 980], [1689, 1040]],
  lx: 1670, ly: 955
});
addEdge('dim_carrier', 'bottom', 0.45, 'fct_shipment', 'top', 0.42, 'carrier_key', { offset: 0, lx: 1855, ly: 450 });
addEdge('dim_geography', 'bottom', 0.45, 'fct_shipment', 'top', 0.75, 'geography_key', { offset: 35, lx: 2070, ly: 482 });
addEdge('dim_warehouse', 'bottom', 0.5, 'fct_inventory_daily_snapshot', 'right', 0.44, 'warehouse_key', {
  points: [[2452, 303], [2452, 1190], [1985, 1190]],
  lx: 2200, ly: 1180
});

// Direct order relationships.
addEdge('fct_direct_order', 'right', 0.28, 'fct_direct_order_item', 'left', 0.28, 'order_id', { orientation: 'horizontal', lx: 625, ly: 655 });
addEdge('fct_direct_order', 'bottom', 0.25, 'fct_return', 'top', 0.38, 'order_id', { offset: 18, lx: 290, ly: 1000 });
addEdge('fct_direct_order', 'right', 0.58, 'fct_shipment', 'left', 0.56, 'order_id', {
  points: [[600, 758], [1715, 758], [1715, 750], [1770, 750]],
  lx: 1160, ly: 745
});
addEdge('fct_direct_order', 'bottom', 0.72, 'fct_product_review', 'top', 0.55, 'order_id', { offset: -12, lx: 580, ly: 992 });
addEdge('fct_direct_order', 'bottom', 0.90, 'fct_inventory_transaction', 'top', 0.22, 'order_id nullable', {
  points: [[557, 873], [557, 1018], [1137, 1018], [1137, 1040]],
  lx: 835, ly: 1008
});

// Role-playing date dimension. Dashed lines intentionally grouped.
const dateBottom = anchor('dim_date', 'bottom', 0.5);
const dateBusX = 82;
const dateTargets = [
  ['fct_direct_order', 0.10, 610],
  ['fct_marketplace_sale_line', 0.08, 650],
  ['fct_shipment', 0.10, 690],
  ['fct_return', 0.10, 1110],
  ['fct_product_review', 0.10, 1140],
  ['fct_inventory_transaction', 0.10, 1170],
  ['fct_inventory_daily_snapshot', 0.10, 1200]
];
for (const [id, fraction, y] of dateTargets) {
  const target = anchor(id, 'left', fraction);
  edges.push({
    d: makePath([dateBottom, [dateBusX, dateBottom[1]], [dateBusX, y], [target[0] - 18, y], [target[0] - 18, target[1]], target]),
    label: '',
    cls: 'date-relationship'
  });
}

// Derived unified fact.
addEdge('fct_direct_order_item', 'bottom', 0.63, 'fct_sales_line_unified', 'top', 0.28, 'UNION ALL: direct', { offset: 12, lx: 950, ly: 1435, cls: 'derived-relationship' });
addEdge('fct_marketplace_sale_line', 'bottom', 0.47, 'fct_sales_line_unified', 'top', 0.72, 'UNION ALL: marketplace', { offset: -20, lx: 1425, ly: 1435, cls: 'derived-relationship' });

function renderEdge(edge) {
  let out = '<path class="' + edge.cls + '" d="' + edge.d + '" />';
  if (edge.label && Number.isFinite(edge.lx) && Number.isFinite(edge.ly)) {
    const labelWidth = Math.max(75, edge.label.length * 7.2 + 18);
    out += '<g class="edge-label">'
      + '<rect x="' + (edge.lx - labelWidth / 2) + '" y="' + (edge.ly - 12) + '" width="' + labelWidth + '" height="23" rx="7" />'
      + '<text x="' + edge.lx + '" y="' + (edge.ly + 4) + '" text-anchor="middle">' + esc(edge.label) + '</text>'
      + '</g>';
  }
  return out;
}

function renderEntity(e) {
  const palette = e.kind === 'dimension'
    ? { header: '#2563EB', soft: '#EFF6FF', border: '#93C5FD', tag: '#DBEAFE' }
    : e.kind === 'fact'
      ? { header: '#C2410C', soft: '#FFF7ED', border: '#FDBA74', tag: '#FFEDD5' }
      : { header: '#6D28D9', soft: '#F5F3FF', border: '#C4B5FD', tag: '#EDE9FE' };

  let out = '<g id="' + e.id + '" class="entity">';
  out += '<rect class="table-shadow" x="' + (e.x + 6) + '" y="' + (e.y + 8) + '" width="' + e.w + '" height="' + e.h + '" rx="14" />';
  out += '<rect x="' + e.x + '" y="' + e.y + '" width="' + e.w + '" height="' + e.h + '" rx="14" fill="' + palette.soft + '" stroke="' + palette.border + '" stroke-width="2" />';
  out += '<path d="M ' + (e.x + 14) + ' ' + e.y + ' H ' + (e.x + e.w - 14) + ' Q ' + (e.x + e.w) + ' ' + e.y + ' ' + (e.x + e.w) + ' ' + (e.y + 14) + ' V ' + (e.y + headerHeight) + ' H ' + e.x + ' V ' + (e.y + 14) + ' Q ' + e.x + ' ' + e.y + ' ' + (e.x + 14) + ' ' + e.y + ' Z" fill="' + palette.header + '" />';
  out += '<text class="table-title" x="' + (e.x + 16) + '" y="' + (e.y + 31) + '">' + esc(e.title) + '</text>';
  out += '<text class="table-subtitle" x="' + (e.x + 16) + '" y="' + (e.y + headerHeight + 20) + '">' + esc(e.subtitle) + '</text>';
  out += '<line x1="' + e.x + '" y1="' + (e.y + headerHeight + tableTitleHeight) + '" x2="' + (e.x + e.w) + '" y2="' + (e.y + headerHeight + tableTitleHeight) + '" stroke="' + palette.border + '" />';

  e.columns.forEach((col, index) => {
    const rowY = e.y + headerHeight + tableTitleHeight + index * rowHeight;
    if (index % 2 === 1) {
      out += '<rect x="' + (e.x + 1) + '" y="' + rowY + '" width="' + (e.w - 2) + '" height="' + rowHeight + '" fill="#FFFFFF" opacity="0.58" />';
    }
    const badgeW = col[0] ? Math.max(30, col[0].length * 7 + 12) : 0;
    if (col[0]) {
      out += '<rect x="' + (e.x + 11) + '" y="' + (rowY + 5) + '" width="' + badgeW + '" height="18" rx="5" fill="' + palette.tag + '" />';
      out += '<text class="badge" x="' + (e.x + 11 + badgeW / 2) + '" y="' + (rowY + 18) + '" text-anchor="middle">' + esc(col[0]) + '</text>';
    }
    out += '<text class="column-name" x="' + (e.x + 18 + (badgeW ? badgeW + 5 : 0)) + '" y="' + (rowY + 19) + '">' + esc(col[1]) + '</text>';
    out += '<text class="column-type" x="' + (e.x + e.w - 12) + '" y="' + (rowY + 19) + '" text-anchor="end">' + esc(col[2]) + '</text>';
  });
  out += '</g>';
  return out;
}

const svg = [
  '<?xml version="1.0" encoding="UTF-8"?>',
  '<svg xmlns="http://www.w3.org/2000/svg" width="' + width + '" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '">',
  '<defs>',
  '<filter id="softShadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="4" stdDeviation="6" flood-color="#0F172A" flood-opacity="0.10"/></filter>',
  '<marker id="oneBar" markerWidth="9" markerHeight="13" refX="2" refY="6.5" orient="auto" markerUnits="strokeWidth"><path d="M2,1 L2,12" fill="none" stroke="#64748B" stroke-width="1.6"/></marker>',
  '<marker id="crowFoot" markerWidth="13" markerHeight="13" refX="11" refY="6.5" orient="auto" markerUnits="strokeWidth"><path d="M1,1 L11,6.5 L1,12 M11,6.5 L1,6.5" fill="none" stroke="#64748B" stroke-width="1.4"/></marker>',
  '<style><![CDATA[',
  'text{font-family:"Segoe UI",Arial,sans-serif;fill:#0F172A}',
  '.table-shadow{fill:#0F172A;opacity:.08;filter:url(#softShadow)}',
  '.table-title{font-size:17px;font-weight:700;fill:#FFFFFF;letter-spacing:.15px}',
  '.table-subtitle{font-size:12px;font-weight:600;fill:#475569}',
  '.column-name{font-size:12px;font-weight:600}',
  '.column-type{font-size:10.5px;fill:#64748B}',
  '.badge{font-size:9.5px;font-weight:800;fill:#334155}',
  '.relationship{fill:none;stroke:#64748B;stroke-width:1.75;marker-start:url(#oneBar);marker-end:url(#crowFoot)}',
  '.date-relationship{fill:none;stroke:#3B82F6;stroke-width:1.25;stroke-dasharray:6 5;opacity:.42;marker-start:url(#oneBar);marker-end:url(#crowFoot)}',
  '.derived-relationship{fill:none;stroke:#7C3AED;stroke-width:2.2;stroke-dasharray:9 6;marker-end:url(#crowFoot)}',
  '.edge-label rect{fill:#FFFFFF;stroke:#CBD5E1;stroke-width:1;opacity:.96}',
  '.edge-label text{font-size:10.5px;font-weight:700;fill:#475569}',
  ']]></style>',
  '</defs>',
  '<rect width="100%" height="100%" fill="#F8FAFC"/>',
  '<rect x="20" y="18" width="' + (width - 40) + '" height="' + (height - 36) + '" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
  '<text x="50" y="58" font-size="27" font-weight="800">SHOPVN ANALYTICS · GOLD LAYER ERD</text>',
  '<text x="50" y="84" font-size="13" fill="#64748B">Spark Lakehouse · Iceberg/Delta compatible logical model · PK = khóa chính · FK = khóa ngoại · NK = khóa nghiệp vụ</text>',
  '<g transform="translate(1870,42)">',
  '<rect x="0" y="0" width="18" height="18" rx="4" fill="#2563EB"/><text x="27" y="14" font-size="12" font-weight="700">DIMENSION</text>',
  '<rect x="150" y="0" width="18" height="18" rx="4" fill="#C2410C"/><text x="177" y="14" font-size="12" font-weight="700">FACT</text>',
  '<rect x="260" y="0" width="18" height="18" rx="4" fill="#6D28D9"/><text x="287" y="14" font-size="12" font-weight="700">DERIVED</text>',
  '<line x1="395" y1="9" x2="437" y2="9" stroke="#3B82F6" stroke-dasharray="6 5"/><text x="445" y="14" font-size="12" font-weight="700">ROLE-PLAYING DATE</text>',
  '</g>',
  '<g class="relationships">',
  edges.map(renderEdge).join(''),
  '</g>',
  '<g class="entities">',
  entities.map(renderEntity).join(''),
  '</g>',
  '<g transform="translate(2070,1500)">',
  '<rect x="0" y="0" width="455" height="245" rx="16" fill="#F8FAFC" stroke="#CBD5E1" stroke-width="1.5"/>',
  '<text x="20" y="32" font-size="16" font-weight="800">GHI CHÚ NGHIỆP VỤ</text>',
  '<circle cx="24" cy="61" r="4" fill="#C2410C"/><text x="38" y="65" font-size="12">Direct và marketplace là hai fact độc lập.</text>',
  '<circle cx="24" cy="91" r="4" fill="#C2410C"/><text x="38" y="95" font-size="12">Marketplace không có customer định danh.</text>',
  '<circle cx="24" cy="121" r="4" fill="#C2410C"/><text x="38" y="125" font-size="12">Return direct không có chi tiết item/product.</text>',
  '<circle cx="24" cy="151" r="4" fill="#2563EB"/><text x="38" y="155" font-size="12">DIM_DATE đóng nhiều vai trò ngày khác nhau.</text>',
  '<circle cx="24" cy="181" r="4" fill="#6D28D9"/><text x="38" y="185" font-size="12">Unified fact chỉ phục vụ so sánh đa kênh.</text>',
  '<text x="20" y="219" font-size="11" font-weight="700" fill="#64748B">Nguồn: 04_thiet_ke_mo_hinh_du_lieu.md</text>',
  '</g>',
  '</svg>'
].join('');

const output = fileURLToPath(new URL('../../docs/data-model/legacy/images/shopvn_gold_erd.svg', import.meta.url));
fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, svg, 'utf8');
console.log(output);
