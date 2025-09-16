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


DB_PATH = "data_cube/bikestore.duckdb"


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

col_a, col_b = st.sidebar.columns(2)
with col_a:
    f_store = st.multiselect("สาขา", options=sorted(stores['store_name'].unique()))
with col_b:
    f_brand = st.multiselect("แบรนด์", options=sorted(brands['brand_name'].unique()))

f_category = st.sidebar.multiselect("หมวดหมู่สินค้า", options=sorted(categories['category_name'].unique()))

f_date = st.sidebar.date_input(
    "ช่วงวันสั่งซื้อ",
    value=(min_date.date(), max_date.date()),
    min_value=min_date.date(),
    max_value=max_date.date()
)
# if st.sidebar.button("รีเซ็ตตัวกรอง"):
#     st.experimental_rerun()

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
        color: #222222;
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
# 📊 KPI Cards (เปลี่ยนสีเป็นโทนฟ้า)
# -----------------------------
st.markdown("""
   <style>
   .kpi-row {
       display: flex;
       gap: 18px;
       margin-bottom: 24px;
       justify-content: space-between;
       width: 100%;
   }
   .metric-card {
       background: linear-gradient(135deg, #4f8bc9 0%, #b3d8fd 100%);
       border-radius: 14px;
       padding: 18px 18px 12px 18px;
       box-shadow: 0 4px 16px rgba(79,139,201,0.10);
       text-align: center;
       color: #2d3748;
       border: 1px solid #4f8bc9;
       min-width: 160px;
       max-width: 100%;
       min-height: 100px;
       flex: 1;
       display: flex;
       flex-direction: column;
       align-items: center;
       justify-content: center;
       margin: 0;
   }
   .metric-title {
       font-size: 1.05rem;
       font-weight: bold;
       margin-bottom: 8px;
       letter-spacing: 0.5px;
   }
   .metric-value {
       font-size: 1.7rem;
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


st.markdown("---")


# -----------------------------
# 📈 แนวโน้มยอดขาย & จำนวนออเดอร์ (2 กราฟใน 1 แถว)
# -----------------------------

# แนวโน้มยอดขาย & จำนวนออเดอร์ (2 กราฟใน 1 แถว)
st.markdown("### แนวโน้มยอดขายและออเดอร์ตามช่วงเวลา")
col1, col2 = st.columns([1,1])
with col1:
   fig_sales = px.line(
       trend_df, x=period, y='net_sales',
       markers=True,
       title=f"แนวโน้มยอดขาย ($) by {period}",
       labels={period: "ช่วงเวลา", "net_sales": "ยอดขายสุทธิ ($)"},
       color_discrete_sequence=["#4f8bc9"]
   )
   fig_sales.update_layout(template="plotly_white", legend_title_text="ยอดขายสุทธิ", height=320, margin=dict(t=40, b=40, l=10, r=10))
   st.plotly_chart(fig_sales, use_container_width=True, key="sales_trend")
with col2:
   fig_orders = px.line(
       trend_df, x=period, y='orders',
       markers=True,
       title=f"แนวโน้มจำนวนออเดอร์ by {period}",
       labels={period: "ช่วงเวลา", "orders": "จำนวนออเดอร์"},
       color_discrete_sequence=["#4f8bc9"]
   )
   fig_orders.update_layout(template="plotly_white", legend_title_text="จำนวนออเดอร์", height=320, margin=dict(t=40, b=40, l=10, r=10))
   st.plotly_chart(fig_orders, use_container_width=True, key="orders_trend")



st.markdown("---")


# -----------------------------
# 🧱 Top10 สินค้าขายดี (2 กราฟใน 1 แถว)
# -----------------------------


# Aggregate top 10 products by net sales and quantity
prod_rev = (
   f.groupby('product_name')
   .agg(net_sales=('net_sales', 'sum'))
   .reset_index()
   .sort_values(by='net_sales', ascending=False)
   .head(10)
)


prod_qty = (
   f.groupby('product_name')
   .agg(quantity=('quantity', 'sum'))
   .reset_index()
   .sort_values(by='quantity', ascending=False)
   .head(10)
)


# Top10 สินค้าขายดี (2 กราฟใน 1 แถว)
st.markdown("### Top10 สินค้าขายดี")
col3, col4 = st.columns([1,1])
with col3:
   fig_rev = px.bar(
       prod_rev,
       x='product_name',
       y='net_sales',
       text='net_sales',
       title='Top 10 สินค้าขายดีตามรายได้',
       labels={'product_name': 'ชื่อสินค้า', 'net_sales': 'รายได้สุทธิ ($)'},
       color='net_sales',
       color_continuous_scale='Blues'  # เปลี่ยนเป็นโทนฟ้า
   )
   fig_rev.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False)
   fig_rev.update_layout(template="plotly_white", xaxis_tickangle=-35, yaxis_title='รายได้สุทธิ ($)', showlegend=False, height=320, margin=dict(t=40, b=60, l=10, r=10))
   st.plotly_chart(fig_rev, use_container_width=True, key="top10_revenue")
with col4:
   fig_qty = px.bar(
       prod_qty,
       x='product_name',
       y='quantity',
       text='quantity',
       title='Top 10 สินค้าขายดีตามจำนวนชิ้น',
       labels={'product_name': 'ชื่อสินค้า', 'quantity': 'จำนวนชิ้น'},
       color='quantity',
       color_continuous_scale='Blues'  # เปลี่ยนเป็นโทนฟ้า
   )
   fig_qty.update_traces(texttemplate='%{text}', textposition='outside', cliponaxis=False)
   fig_qty.update_layout(template="plotly_white", xaxis_tickangle=-35, yaxis_title='จำนวนชิ้น', showlegend=False, height=320, margin=dict(t=40, b=60, l=10, r=10))
   st.plotly_chart(fig_qty, use_container_width=True, key="top10_qty")


st.markdown("---")


# -----------------------------
# 🧩 แบรนด์ × หมวดหมู่ (Treemap)
# -----------------------------
# Aggregate net sales by brand and category for treemap
brand_cat = (
   f.groupby(['brand_name', 'category_name'])
   .agg(net_sales=('net_sales', 'sum'))
   .reset_index()
)


# Treemap แบรนด์ × หมวดหมู่
st.markdown("### สัดส่วนยอดขายตามแบรนด์และหมวดหมู่สินค้า")
fig_tree = px.treemap(
   brand_cat,
   path=['brand_name','category_name'],
   values='net_sales',
   title="Treemap: แบรนด์ × หมวดหมู่",
   color='net_sales',
   color_continuous_scale='Blues'  # เปลี่ยนเป็นโทนฟ้า
)
fig_tree.update_layout(margin=dict(t=50,l=0,r=0,b=0), height=400)
st.plotly_chart(fig_tree, use_container_width=True, key="brand_cat_treemap")


st.markdown("---")


# -----------------------------
# 💸 ผลของส่วนลดต่อปริมาณ/รายได้ (2 กราฟใน 1 แถว)
# -----------------------------


# สร้าง discount range และ aggregate ข้อมูล
discount_bins = [0, 0.05, 0.10, 0.15, 0.20, 0.25, 1.0]
discount_labels = ["0-5%", "5-10%", "10-15%", "15-20%", "20-25%", "25%+"]
f['discount_range'] = pd.cut(f['discount'], bins=discount_bins, labels=discount_labels, include_lowest=True, right=False)
disc = f.groupby('discount_range').agg(
   total_qty=('quantity', 'sum'),
   total_sales=('net_sales', 'sum')
).reset_index()


# ผลของส่วนลดต่อปริมาณ/รายได้ (2 กราฟใน 1 แถว)
st.markdown("### ผลของส่วนลดต่อปริมาณ & รายได้")
col5, col6 = st.columns([1,1])
with col5:
   fig_dq = px.bar(
       disc,
       x='discount_range',
       y='total_qty',
       text='total_qty',
       title='ปริมาณที่ขายได้ตามช่วงส่วนลด',
       labels={'discount_range': 'ช่วงส่วนลด (%)', 'total_qty': 'ปริมาณที่ขายได้ (ชิ้น)'},
       color='total_qty',
       color_continuous_scale='Blues'  # เปลี่ยนเป็นโทนฟ้า
   )
   fig_dq.update_traces(textposition='outside', cliponaxis=False)
   fig_dq.update_layout(template="plotly_white", xaxis_title="ช่วงส่วนลด (%)", yaxis_title="ปริมาณที่ขายได้ (ชิ้น)", height=320, margin=dict(t=40, b=40, l=10, r=10))
   st.plotly_chart(fig_dq, use_container_width=True, key="discount_qty")
with col6:
   fig_ds = px.bar(
       disc,
       x='discount_range',
       y='total_sales',
       text='total_sales',
       title='รายได้ที่ขายได้ตามช่วงส่วนลด',
       labels={'discount_range': 'ช่วงส่วนลด (%)', 'total_sales': 'รายได้ที่ขายได้ ($)'},
       color='total_sales',
       color_continuous_scale='Blues'  # เปลี่ยนเป็นโทนฟ้า
   )
   fig_ds.update_traces(texttemplate='%{text:,.0f}', textposition='outside', cliponaxis=False)
   fig_ds.update_layout(template="plotly_white", xaxis_title="ช่วงส่วนลด (%)", yaxis_title="รายได้ที่ขายได้ ($)", height=320, margin=dict(t=40, b=40, l=10, r=10))
   st.plotly_chart(fig_ds, use_container_width=True, key="discount_sales")
