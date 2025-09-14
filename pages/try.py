import streamlit as st
import duckdb as dd
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

# -----------------------------
# ✅ Page Config & Theming
# -----------------------------
st.set_page_config(
    page_title="Sale Dashboard",
    layout="wide",
    page_icon="🌼"
)

# Minimal CSS ปรับให้ดูโล่ง อ่านง่าย
st.markdown(
    """
    <style>
    .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    .metric-card {border-radius: 16px; padding: 18px 18px 8px 18px; box-shadow: 0 4px 20px rgba(0,0,0,0.06);}
    .section-title {margin-top: 8px;}
    .dataframe td {font-size: 0.92rem;}
    .stTabs [data-baseweb="tab-list"] {gap: 8px;}
    .stTabs [data-baseweb="tab"] {border-radius: 12px; padding: 8px 12px;}
    .footnote {color: #6b7280; font-size: 0.86rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# 🔧 Utils
# -----------------------------
@st.cache_data(show_spinner=False)
def load_tables(db_path: str):
    conn = dd.connect(db_path)
    try:
        dim_customers = conn.execute("SELECT * FROM dim_customers").fetchdf()
        dim_date      = conn.execute("SELECT * FROM dim_date").fetchdf()
        dim_staffs    = conn.execute("SELECT * FROM dim_staffs").fetchdf()
        dim_products  = conn.execute("SELECT * FROM dim_products").fetchdf()
        dim_brands    = conn.execute("SELECT * FROM dim_brands").fetchdf()
        dim_categories= conn.execute("SELECT * FROM dim_categories").fetchdf()
        dim_stores    = conn.execute("SELECT * FROM dim_stores").fetchdf()
        fact_sales    = conn.execute("SELECT * FROM fact_sales").fetchdf()
    finally:
        conn.close()
    return (
        dim_customers, dim_date, dim_staffs, dim_products,
        dim_brands, dim_categories, dim_stores, fact_sales
    )


def baht(x):
    try:
        return f"฿{x:,.0f}"
    except Exception:
        return "-"

def pct(x):
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "-"


def add_period_cols(df):
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year']   = df['order_date'].dt.year
    df['quarter']= df['order_date'].dt.to_period('Q').astype(str)
    df['month']  = df['order_date'].dt.to_period('M').astype(str)
    df['date']   = df['order_date'].dt.date
    return df


def compute_net_sales(df):
    df = df.copy()
    # net_sales = quantity * list_price * (1 - discount)
    df['net_sales'] = df['quantity'] * df['list_price'] * (1 - df['discount'])
    return df


def growth_rate(series: pd.Series):
    if len(series) < 2:
        return 0.0
    prev, curr = series.iloc[-2], series.iloc[-1]
    if prev == 0:
        return np.nan
    return (curr - prev) / prev

# -----------------------------
# 🎛️ Sidebar – ฟิลเตอร์
# -----------------------------
st.sidebar.title("⚙️ ตัวกรองข้อมูล")


DB_PATH = r"/Users/mac/Downloads/Web/data_cube/bikestore.duckdb"


(
    dim_customers, dim_date, dim_staffs, dim_products,
    dim_brands, dim_categories, dim_stores, fact_sales
) = load_tables(DB_PATH)

# ทำงานด้วย pandas ทั้งหมดเพื่อความสม่ำเสมอ
customers = dim_customers.copy()
products  = dim_products.copy()
brands    = dim_brands.copy()
categories= dim_categories.copy()
stores    = dim_stores.copy()
staffs    = dim_staffs.copy()
sales     = fact_sales.copy()

# เตรียมข้อมูลหลัก
sales = compute_net_sales(sales)
sales = add_period_cols(sales)

# Join dims ที่จำเป็น
products_lite = products[['product_id','product_name','category_id','brand_id']]
sales = sales.merge(products_lite, on='product_id', how='left')
sales = sales.merge(categories[['category_id','category_name']], on='category_id', how='left')
sales = sales.merge(brands[['brand_id','brand_name']], on='brand_id', how='left')
sales = sales.merge(stores[['store_id','store_name']], on='store_id', how='left')
sales = sales.merge(customers[['customer_id','customer_city','customer_state']], on='customer_id', how='left')

# วันที่ min-max สำหรับฟิลเตอร์
min_date = pd.to_datetime(sales['order_date']).min()
max_date = pd.to_datetime(sales['order_date']).max()

# ---- Controls ----
period = st.sidebar.selectbox("หน่วยเวลา (สำหรับกราฟแนวโน้ม)", ["month","quarter","year"], index=0)

f_date = st.sidebar.date_input(
    "ช่วงวันสั่งซื้อ",
    value=(min_date.date(), max_date.date()),
    min_value=min_date.date(),
    max_value=max_date.date()
)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    f_store = st.multiselect("สาขา", options=sorted(stores['store_name'].unique()))
with col_b:
    f_brand = st.multiselect("แบรนด์", options=sorted(brands['brand_name'].unique()))

f_category = st.sidebar.multiselect("หมวดหมู่สินค้า", options=sorted(categories['category_name'].unique()))

if st.sidebar.button("รีเซ็ตตัวกรอง"):
    st.experimental_rerun()

# Apply Filters
mask = (
    (sales['order_date'].dt.date >= f_date[0]) &
    (sales['order_date'].dt.date <= f_date[1])
)
if f_store:
    mask &= sales['store_name'].isin(f_store)
if f_brand:
    mask &= sales['brand_name'].isin(f_brand)
if f_category:
    mask &= sales['category_name'].isin(f_category)

f = sales.loc[mask].copy()

# -----------------------------
# 🧭 Header
# -----------------------------
st.title("Bikestore Business Dashboard")
st.header("🛒 Sale Dashboard")
st.caption(f"ช่วงวันที่ {f_date[0].strftime('%d %b %Y')} – {f_date[1].strftime('%d %b %Y')}")

# -----------------------------
# 📊 KPI Cards
# -----------------------------
# KPI หลัก
total_sales   = f['net_sales'].sum()
orders        = f['order_id'].nunique()
customers_cnt = f['customer_id'].nunique()
AOV           = total_sales / orders if orders else 0

# Growth เทียบกับงวดก่อน (ตาม period)
trend_df = (
    f.groupby(period)
     .agg(net_sales=('net_sales','sum'), orders=('order_id','nunique'))
     .reset_index()
     .sort_values(by=period)
)

sales_growth = growth_rate(trend_df['net_sales']) if len(trend_df) >= 2 else np.nan
orders_growth = growth_rate(trend_df['orders']) if len(trend_df) >= 2 else np.nan

# ...existing code...

st.markdown("""
    <style>
    .kpi-row {
        display: flex;
        gap: 18px;
        margin-bottom: 24px;
        justify-content: center;
    }
    .metric-card {
        background: linear-gradient(135deg, #f8bbd0 0%, #fce4ec 100%);
        border-radius: 18px;
        padding: 18px 18px 12px 18px;
        box-shadow: 0 4px 20px rgba(240, 120, 180, 0.10);
        text-align: center;
        color: #ad1457;
        border: 1px solid #f8bbd0;
        min-width: 220px;
        max-width: 220px;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        margin: 0;
    }
    .metric-title {
        font-size: 1.1rem;
        font-weight: bold;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.7rem;
        font-weight: bold;
        margin-bottom: 2px;
    }
    .metric-growth {
        font-size: 1.1rem;
        font-weight: 500;
        color: #c2185b;
        margin-bottom: 0;
    }
    @media (max-width: 900px) {
        .kpi-row { flex-direction: column; align-items: stretch; }
        .metric-card { max-width: 100%; min-width: 0; margin-bottom: 18px; }
    }
    </style>
""", unsafe_allow_html=True)
# -----------------------------
# 📊 KPI Cards
# -----------------------------
# KPI หลัก
total_sales   = f['net_sales'].sum()
orders        = f['order_id'].nunique()
customers_cnt = f['customer_id'].nunique()
AOV           = total_sales / orders if orders else 0

st.markdown("""
    <style>
    .kpi-row {
        display: flex;
        gap: 18px;
        margin-bottom: 24px;
        justify-content: center;
    }
    .metric-card {
        background: linear-gradient(135deg, #9dcaeb 0%, #f8d5f8 100%);
        border-radius: 18px;
        padding: 22px 18px 18px 18px;
        box-shadow: 0 4px 20px rgba(240, 120, 180, 0.10);
        text-align: center;
        color: #990066;
        border: 1px solid #f8bbd0;
        min-width: 220px;
        max-width: 220px;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        margin: 0;
    }
    .metric-title {
        font-size: 1.15rem;
        font-weight: bold;
        margin-bottom: 10px;
        letter-spacing: 0.5px;
    }
    .metric-value {
        font-size: 2.1rem;
        font-weight: bold;
        margin-bottom: 0;
        letter-spacing: 0.5px;
    }
    @media (max-width: 900px) {
        .kpi-row { flex-direction: column; align-items: stretch; }
        .metric-card { max-width: 100%; min-width: 0; margin-bottom: 18px; }
    }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="kpi-row">
        <div class="metric-card">
            <div class="metric-title">ยอดขายรวม</div>
            <div class="metric-value">${total_sales:,.0f}</div>
        </div>
        <div class="metric-card">
            <div class="metric-title">จำนวนออเดอร์</div>
            <div class="metric-value">{orders:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-title">จำนวนลูกค้า</div>
            <div class="metric-value">{customers_cnt:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-title">ค่าเฉลี่ยต่อออเดอร์</div>
            <div class="metric-value">${AOV:,.0f}</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)
# -----------------------------
# 📈 แนวโน้มยอดขาย & จำนวนออเดอร์
# -----------------------------
st.markdown("### แนวโน้มยอดขายและออเดอร์ตามช่วงเวลา")
# trend_option = st.radio(
#     "",  # label ว่าง
#     options=["ยอดขายสุทธิ", "จำนวนออเดอร์"],
#     index=0,
#     horizontal=True
# )
# # ...existing code...

# if trend_option == "ยอดขายสุทธิ":
#     fig_trend = px.line(
#         trend_df, x=period, y='net_sales',
#         markers=True,
#         title=f"แนวโน้มยอดขาย ($) by {period}",
#         labels={period: "ช่วงเวลา", "net_sales": "ยอดขายสุทธิ ($)"}
#     )
#     fig_trend.update_layout(template="plotly_white", legend_title_text="ยอดขายสุทธิ")
# else:
#     fig_trend = px.line(
#         trend_df, x=period, y='orders',
#         markers=True,
#         title=f"แนวโน้มจำนวนออเดอร์ by {period}",
#         labels={period: "ช่วงเวลา", "orders": "จำนวนออเดอร์"}
#     )
#     fig_trend.update_layout(template="plotly_white", legend_title_text="จำนวนออเดอร์")

# st.plotly_chart(fig_trend, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    fig_sales = px.line(
        trend_df, x=period, y='net_sales',
        markers=True,
        title=f"แนวโน้มยอดขาย ($) by {period}",
        labels={period: "ช่วงเวลา", "net_sales": "ยอดขายสุทธิ ($)"}
    )
    fig_sales.update_layout(template="plotly_white", legend_title_text="ยอดขายสุทธิ")
    st.plotly_chart(fig_sales, use_container_width=True, key="sales_trend")

with col2:
    fig_orders = px.line(
        trend_df, x=period, y='orders',
        markers=True,
        title=f"แนวโน้มจำนวนออเดอร์ by {period}",
        labels={period: "ช่วงเวลา", "orders": "จำนวนออเดอร์"}
    )
    fig_orders.update_layout(template="plotly_white", legend_title_text="จำนวนออเดอร์")
    st.plotly_chart(fig_orders, use_container_width=True, key="orders_trend")


# -----------------------------
# 🧱 สรุปยอดขายตามประเภทสินค้า & สินค้าขายดี
st.markdown("### Top10 สินค้าขายดี")

# 2.2 Top Products (Revenue & Qty)
prod_rev = (
    f.groupby(['product_id','product_name'], as_index=False)['net_sales'].sum()
     .sort_values('net_sales', ascending=False).head(10)
)
prod_qty = (
    f.groupby(['product_id','product_name'], as_index=False)['quantity'].sum()
     .sort_values('quantity', ascending=False).head(10)
)

col1, col2 = st.columns(2)

with col1:
    fig_rev = px.bar(
        prod_rev,
        x='product_name',
        y='net_sales',
        text='net_sales',
        title='Top 10 สินค้าขายดีตามรายได้',
        labels={'product_name': 'ชื่อสินค้า', 'net_sales': 'รายได้สุทธิ ($)'}
    )
    fig_rev.update_traces(
        texttemplate='%{text:,.0f}',
        textposition='auto',
        cliponaxis=True
    )
    fig_rev.update_layout(
        template="plotly_white",
        xaxis_tickangle=-35,
        yaxis_title='รายได้สุทธิ ($)',
        showlegend=False,
        margin=dict(t=60, b=80)
    )
    st.plotly_chart(fig_rev, use_container_width=True, key="top10_revenue")

with col2:
    fig_qty = px.bar(
        prod_qty,
        x='product_name',
        y='quantity',
        text='quantity',
        title='Top 10 สินค้าขายดีตามจำนวนชิ้น',
        labels={'product_name': 'ชื่อสินค้า', 'quantity': 'จำนวนชิ้น'}
    )
    fig_qty.update_traces(
        texttemplate='%{text}',
        textposition='auto',
        cliponaxis=True
    )
    fig_qty.update_layout(
        template="plotly_white",
        xaxis_tickangle=-35,
        yaxis_title='จำนวนชิ้น',
        showlegend=False,
        margin=dict(t=60, b=80)
    )
    st.plotly_chart(fig_qty, use_container_width=True, key="top10_qty")

tabs = st.tabs(["ตามรายได้", "ตามจำนวนชิ้น"])
with tabs[0]:
    fig_rev = px.bar(
        prod_rev,
        x='product_name',           # x = ชื่อสินค้า
        y='net_sales',              # y = รายได้สุทธิ ($)
        text='net_sales',
        title='Top 10 สินค้าขายดีตามรายได้',
        labels={'product_name': 'ชื่อสินค้า', 'net_sales': 'รายได้สุทธิ ($)'}
    )
    fig_rev.update_traces(
        texttemplate='%{text:,.0f}',
        textposition='auto',
        cliponaxis=True
    )
    fig_rev.update_layout(
        template="plotly_white",
        xaxis_tickangle=-35,
        yaxis_title='รายได้สุทธิ ($)',
        showlegend=False,
        margin=dict(t=60, b=80)
    )
    st.plotly_chart(fig_rev, use_container_width=True)
with tabs[1]:
    fig_qty = px.bar(
        prod_qty,
        x='product_name',           # x = ชื่อสินค้า
        y='quantity',               # y = จำนวนชิ้น
        text='quantity',
        title='Top 10 สินค้าขายดีตามจำนวนชิ้น',
        labels={'product_name': 'ชื่อสินค้า', 'quantity': 'จำนวนชิ้น'}
    )
    fig_qty.update_traces(
        texttemplate='%{text}',
        textposition='auto',
        cliponaxis=True
    )
    fig_qty.update_layout(
        template="plotly_white",
        xaxis_tickangle=-35,
        yaxis_title='จำนวนชิ้น',
        showlegend=False,
        margin=dict(t=60, b=80)
    )
    st.plotly_chart(fig_qty, use_container_width=True)
# -----------------------------
# 🧩 แบรนด์ × หมวดหมู่ (Treemap)
# -----------------------------
f2 = f.copy()
st.markdown("### สัดส่วนยอดขายตามแบรนด์และหมวดหมู่สินค้า")
brand_cat = (
    f.groupby(['brand_name','category_name'], as_index=False)['net_sales'].sum()
)
fig_tree = px.treemap(brand_cat, path=['brand_name','category_name'], values='net_sales', title="Treemap: แบรนด์ × หมวดหมู่")
fig_tree.update_layout(margin=dict(t=50,l=0,r=0,b=0))
st.plotly_chart(fig_tree, use_container_width=True)

# -----------------------------
# 💸 ผลของส่วนลดต่อปริมาณ/รายได้
# -----------------------------
st.markdown("### ผลของส่วนลดต่อปริมาณ & รายได้")

# แบ่งช่วงส่วนลดเป็น 0-5%, 5-10%, 10-15%, 15-20%
f2['discount_range'] = pd.cut(
    f2['discount'],
    bins=[-0.01, 0.05, 0.10, 0.15, 0.20],
    labels=['0-5%', '5-10%', '10-15%', '15-20%']
)
disc = f2.groupby('discount_range', as_index=False).agg(
    total_qty=('quantity','sum'),
    total_sales=('net_sales','sum')
)

tabD1, tabD2 = st.tabs(["ปริมาณ (ชิ้น)", "รายได้ ($)"])
with tabD1:
    fig_dq = px.bar(
        disc,
        x='discount_range',
        y='total_qty',
        text='total_qty',
        title='ปริมาณที่ขายได้ตามช่วงส่วนลด',
        labels={'discount_range': 'ช่วงส่วนลด (%)', 'total_qty': 'ปริมาณที่ขายได้ (ชิ้น)'}
    )
    fig_dq.update_traces(textposition='auto', cliponaxis=True)
    fig_dq.update_layout(template="plotly_white", xaxis_title="ช่วงส่วนลด (%)", yaxis_title="ปริมาณที่ขายได้ (ชิ้น)")
    st.plotly_chart(fig_dq, use_container_width=True)
with tabD2:
    fig_ds = px.bar(
        disc,
        x='discount_range',
        y='total_sales',
        text='total_sales',
        title='รายได้ที่ขายได้ตามช่วงส่วนลด',
        labels={'discount_range': 'ช่วงส่วนลด (%)', 'total_sales': 'รายได้ที่ขายได้ ($)'}
    )
    fig_ds.update_traces(texttemplate='%{text:,.0f}', textposition='auto', cliponaxis=True)
    fig_ds.update_layout(template="plotly_white", xaxis_title="ช่วงส่วนลด (%)", yaxis_title="รายได้ที่ขายได้ ($)")
    st.plotly_chart(fig_ds, use_container_width=True)