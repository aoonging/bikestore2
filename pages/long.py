import streamlit as st
import duckdb as dd
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go
import duckdb as dd
# -----------------------------
# ✅ Page Config & Theming
# -----------------------------
st.set_page_config(
    page_title="Customer Dashboard",
    layout="wide",
    page_icon="🌞"
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
# ...existing code...
# ...existing code...
# -----------------------------
# 🧭 Header
# -----------------------------
st.title("Bikestore Business Dashboard")
st.header("🚴🏻 Customer Dashboard")
st.caption(f"ช่วงวันที่ {f_date[0].strftime('%d %b %Y')} – {f_date[1].strftime('%d %b %Y')}")


# -----------------------------
st.markdown("### จำนวนลูกค้าตามรัฐ (Treemap)")

# สร้าง ts สำหรับ treemap และ sunburst chart
ts = customers.groupby('customer_state', as_index=False).agg(count=('customer_id', 'nunique'))

fig_treemap = px.treemap(
    ts,
    path=['customer_state'],
    values='count',
    color='count',
    color_continuous_scale='Blues',
    labels={'customer_state': 'รัฐ', 'count': 'จำนวนลูกค้า'},
    title='จำนวนลูกค้าตามรัฐ'
)
fig_treemap.update_layout(margin=dict(t=30, l=0, r=0, b=0))
st.plotly_chart(fig_treemap, use_container_width=True)

# -----------------------------
st.markdown("### อัตราลูกค้าซื้อซ้ำ")
colG1, colG2 = st.columns(2)
# สรุปจำนวนออเดอร์ต่อ customer_id
cust_orders = (
    f.groupby('customer_id', as_index=False)['order_id']
     .nunique()
     .rename(columns={'order_id':'order_count'})
).merge(
    customers[['customer_id','customer_city','customer_state']],
    on='customer_id', how='left'
)
cust_orders['is_repeat'] = cust_orders['order_count'] > 1

# สรุประดับเมือง/รัฐ
repeat_city = cust_orders.groupby('customer_city', as_index=False) \
                         .agg(repeat_rate=('is_repeat','mean'),
                              customers=('customer_id','nunique'))
repeat_state = cust_orders.groupby('customer_state', as_index=False) \
                          .agg(repeat_rate=('is_repeat','mean'),
                               customers=('customer_id','nunique'))

# ตั้งค่าควบคุมกรองขั้นต่ำลูกค้าและจำนวนอันดับที่จะแสดง
max_city = int(repeat_city['customers'].max()) if not repeat_city.empty else 1
max_state = int(repeat_state['customers'].max()) if not repeat_state.empty else 1
max_cust = max(1, max_city, max_state)

min_c = st.slider("ขั้นต่ำจำนวนลูกค้าต่อเมือง", min_value=1, max_value=20, value=min(10, max_cust))
top_n = st.slider("จำนวนอันดับสูงสุดที่แสดง", min_value=1, max_value=50, value=15, step=1)

tabs_geo = st.tabs(["ตามเมือง (กราฟ)", "ตามรัฐ (กราฟ)", "ตาราง"])

# กราฟเมือง
with tabs_geo[0]:
    dfc = repeat_city[repeat_city['customers'] >= min_c] \
          .sort_values('repeat_rate', ascending=False) \
          .head(top_n)

    if dfc.empty:
        st.info("ไม่มีเมืองที่ผ่านเกณฑ์ขั้นต่ำจำนวนลูกค้า")
    else:
        fig_bar_city = px.bar(
            dfc, x='repeat_rate', y='customer_city',
            orientation='h',
            color='repeat_rate',
            color_continuous_scale='Tealrose',
            labels={'repeat_rate':'Repeat Rate', 'customer_city':'เมือง'},
            hover_data={'customers': True, 'repeat_rate': ':.2%'},
            text=dfc['repeat_rate'].map(lambda x: f"{x:.0%}")
        )
        fig_bar_city.update_layout(
            xaxis_tickformat=".0%",
            margin=dict(l=0, r=0, t=30, b=0)
        )
        fig_bar_city.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_bar_city, use_container_width=True)

        fig_sc_city = px.scatter(
            dfc, x='customers', y='repeat_rate', size='customers',
            color='repeat_rate', color_continuous_scale='Viridis',
            hover_name='customer_city',
            labels={'customers':'จำนวนลูกค้า', 'repeat_rate':'Repeat Rate'}
        )
        fig_sc_city.update_layout(yaxis_tickformat=".0%", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_sc_city, use_container_width=True)

# กราฟรัฐ
with tabs_geo[1]:
    dfs = repeat_state[repeat_state['customers'] >= min_c] \
          .sort_values('repeat_rate', ascending=False) \
          .head(top_n)

    if dfs.empty:
        st.info("ไม่มีรัฐที่ผ่านเกณฑ์ขั้นต่ำจำนวนลูกค้า")
    else:
        fig_bar_state = px.bar(
            dfs, x='repeat_rate', y='customer_state',
            orientation='h',
            color='repeat_rate',
            color_continuous_scale='Tealrose',
            labels={'repeat_rate':'Repeat Rate', 'customer_state':'รัฐ'},
            hover_data={'customers': True, 'repeat_rate': ':.2%'},
            text=dfs['repeat_rate'].map(lambda x: f"{x:.0%}")
        )
        fig_bar_state.update_layout(
            xaxis_tickformat=".0%",
            margin=dict(l=0, r=0, t=30, b=0)
        )
        fig_bar_state.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_bar_state, use_container_width=True)

        fig_sc_state = px.scatter(
            dfs, x='customers', y='repeat_rate', size='customers',
            color='repeat_rate', color_continuous_scale='Viridis',
            hover_name='customer_state',
            labels={'customers':'จำนวนลูกค้า', 'repeat_rate':'Repeat Rate'}
        )
        fig_sc_state.update_layout(yaxis_tickformat=".0%", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_sc_state, use_container_width=True)

# ตาราง
with tabs_geo[2]:
    st.write("ตามเมือง")
    st.dataframe(repeat_city.sort_values('repeat_rate', ascending=False), use_container_width=True)
    st.write("ตามรัฐ")
    st.dataframe(repeat_state.sort_values('repeat_rate', ascending=False), use_container_width=True)