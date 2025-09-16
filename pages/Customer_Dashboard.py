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
    page_icon="🚲"
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

# ...existing code...

# -----------------------------
# 🎛️ Sidebar – ฟิลเตอร์
# -----------------------------
st.sidebar.title("⚙️ ตัวกรองข้อมูล")

DB_PATH = "data_cube/bikestore.duckdb"

(
    dim_customers, dim_date, dim_staffs, dim_products,
    dim_brands, dim_categories, dim_stores, fact_sales
) = load_tables(DB_PATH)

# เตรียมข้อมูลหลัก
customers = dim_customers.copy()
products  = dim_products.copy()
brands    = dim_brands.copy()
categories= dim_categories.copy()
stores    = dim_stores.copy()
staffs    = dim_staffs.copy()
sales     = fact_sales.copy()

sales = compute_net_sales(sales)
sales = add_period_cols(sales)

products_lite = products[['product_id','product_name','category_id','brand_id']]
sales = sales.merge(products_lite, on='product_id', how='left')
sales = sales.merge(categories[['category_id','category_name']], on='category_id', how='left')
sales = sales.merge(brands[['brand_id','brand_name']], on='brand_id', how='left')
sales = sales.merge(stores[['store_id','store_name']], on='store_id', how='left')
sales = sales.merge(customers[['customer_id','customer_city','customer_state']], on='customer_id', how='left')

min_date = pd.to_datetime(sales['order_date']).min()
max_date = pd.to_datetime(sales['order_date']).max()

# ---- Controls ----
# ...existing code...

# ---- Controls ----
period = st.sidebar.selectbox(
    "หน่วยเวลา (สำหรับกราฟแนวโน้ม)",
    ["month","quarter","year"],
    index=0,
    key="period_filter"
)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    f_store = st.multiselect(
        "สาขา",
        options=sorted(stores['store_name'].unique()),
        key="store_filter"
    )
with col_b:
    f_brand = st.multiselect(
        "แบรนด์",
        options=sorted(brands['brand_name'].unique()),
        key="brand_filter"
    )

f_category = st.sidebar.multiselect(
    "หมวดหมู่สินค้า",
    options=sorted(categories['category_name'].unique()),
    key="category_filter"
)

f_date = st.sidebar.date_input(
    "ช่วงวันสั่งซื้อ",
    value=(min_date.date(), max_date.date()),
    min_value=min_date.date(),
    max_value=max_date.date(),
    key="date_filter"
)

# if st.sidebar.button("รีเซ็ตตัวกรอง"):
#     st.rerun()

# ...existing code...

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
# ...existing code...

# -----------------------------
# 🧭 Header & KPI
# -----------------------------
# Calculate KPI values
total_customers = f['customer_id'].nunique()
repeat_customers = f.groupby('customer_id').filter(lambda x: len(x) > 1)['customer_id'].nunique()
repeat_rate = repeat_customers / total_customers if total_customers > 0 else 0

st.title("Bikestore Business Dashboard")
st.header("🚴🏻 Customer Dashboard")
st.caption(f"ช่วงวันที่ {f_date[0].strftime('%d %b %Y')} – {f_date[1].strftime('%d %b %Y')}")
# -----------------------------
# 📊 KPI Cards
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
        border-radius: 18px;
        padding: 22px 18px 18px 18px;
        box-shadow: 0 4px 20px rgba(79,139,201,0.10);
        text-align: center;
        color: #2d3748;
        border: 1px solid #4f8bc9;
        min-width: 180px;
        max-width: 100%;
        min-height: 120px;
        flex: 1;
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
        color: #2d3748;
    }
    .metric-value {
        font-size: 2.1rem;
        font-weight: bold;
        margin-bottom: 0;
        letter-spacing: 0.5px;
        color: #2d3748;
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
            <div class="metric-title">จำนวนลูกค้าทั้งหมด</div>
            <div class="metric-value">{total_customers:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-title">ลูกค้าซื้อซ้ำ</div>
            <div class="metric-value">{repeat_customers:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-title">อัตราซื้อซ้ำ</div>
            <div class="metric-value">{repeat_rate:.1%}</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ...existing code...

# -----------------------------
# กราฟ 3 อันใน 1 แถว (กระจายเต็มหน้าจอ)
# -----------------------------
# เตรียมข้อมูล ts สำหรับ Treemap
ts = f.groupby('customer_state').agg(count=('customer_id', 'nunique')).reset_index()

# กราฟหลัก (Treemap + Repeat Rate + เมือง)
# เตรียมข้อมูล
repeat_state = f.groupby('customer_state').agg(
    customers=('customer_id', 'nunique'),
    orders=('order_id', 'count')
).reset_index()
repeat_state_orders = f.groupby(['customer_state', 'customer_id']).agg(order_count=('order_id', 'count')).reset_index()
repeat_state_orders['is_repeat'] = repeat_state_orders['order_count'] > 1
repeat_rate_state = repeat_state_orders.groupby('customer_state')['is_repeat'].mean().reset_index()
repeat_state = repeat_state.merge(repeat_rate_state, on='customer_state', how='left')
repeat_state = repeat_state.rename(columns={'is_repeat': 'repeat_rate'})

repeat_city = f.groupby('customer_city').agg(
    customers=('customer_id', 'nunique'),
    orders=('order_id', 'count')
).reset_index()# ...existing code...
# สร้าง repeat_rate สำหรับแต่ละเมือง
repeat_city_orders = f.groupby(['customer_city', 'customer_id']).agg(order_count=('order_id', 'count')).reset_index()
repeat_city_orders['is_repeat'] = repeat_city_orders['order_count'] > 1
repeat_rate_city = repeat_city_orders.groupby('customer_city')['is_repeat'].mean().reset_index()
repeat_city = repeat_city.merge(repeat_rate_city, on='customer_city', how='left')
repeat_city = repeat_city.rename(columns={'is_repeat': 'repeat_rate'})
# เตรียมข้อมูลสำหรับกราฟ stacked bar
repeat_state_orders = f.groupby(['customer_state', 'customer_id']).agg(order_count=('order_id', 'count')).reset_index()
repeat_state_orders['is_repeat'] = repeat_state_orders['order_count'] > 1
repeat_customers_state = repeat_state_orders[repeat_state_orders['is_repeat']].groupby('customer_state')['customer_id'].nunique().reset_index()
repeat_customers_state = repeat_customers_state.rename(columns={'customer_id': 'repeat_customers'})
repeat_state = repeat_state.merge(repeat_customers_state, on='customer_state', how='left').fillna({'repeat_customers': 0})
repeat_state['non_repeat_customers'] = repeat_state['customers'] - repeat_state['repeat_customers']

# เลือก Top 15 รัฐที่มีลูกค้ามากที่สุด
df_bar = repeat_state.sort_values('customers', ascending=False).head(15)
# -----------------------------
# Layout ครึ่งซ้าย-ขวาเท่ากัน
# -----------------------------
left_col, right_col = st.columns([1, 1])  # ซ้ายขวา 50:50

# ---------- ฝั่งซ้าย ----------
with left_col:
    st.markdown("#### จำนวนลูกค้าตามรัฐ (Treemap)")
    fig_treemap = px.treemap(
        ts,
        path=['customer_state'],
        values='count',
        color='count',
        color_continuous_scale='Blues',
        labels={'customer_state': 'รัฐ', 'count': 'จำนวนลูกค้า'},
        title=''
    )
    fig_treemap.update_layout(margin=dict(t=30, l=0, r=0, b=0), height=700)
    st.plotly_chart(fig_treemap, use_container_width=True, key="treemap_state")

# ---------- ฝั่งขว ----------
with right_col:
    # ใช้ container ซ้อนสองกราฟ
    with st.container():
        st.markdown("#### จำนวนลูกค้าซื้อซ้ำ vs ลูกค้าใหม่ (แต่ละรัฐ)")
        fig_stacked = go.Figure()
        fig_stacked.add_trace(go.Bar(
            y=df_bar['customer_state'],
            x=df_bar['repeat_customers'],
            name='ลูกค้าซื้อซ้ำ',
            orientation='h',
            marker_color='#4f8bc9',
            text=df_bar['repeat_customers'].map(lambda x: f"{int(x):,}")
        ))
        fig_stacked.add_trace(go.Bar(
            y=df_bar['customer_state'],
            x=df_bar['non_repeat_customers'],
            name='ลูกค้าใหม่',
            orientation='h',
            marker_color='#e0e0e0',
            text=df_bar['non_repeat_customers'].map(lambda x: f"{int(x):,}")
        ))
        fig_stacked.update_layout(
            barmode='stack',
            yaxis_title='รัฐ',
            xaxis_title='จำนวนลูกค้า',
            margin=dict(l=0, r=0, t=30, b=0),
            height=340,  # ครึ่งบนของคอลัมน์ขวา
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_stacked, use_container_width=True, key="stacked_repeat_state")

    with st.container():
        st.markdown("#### อัตราลูกค้าซื้อซ้ำตามรัฐ")
        df_repeat_state = repeat_state[repeat_state['customers'] >= 2].sort_values('repeat_rate', ascending=False).head(15)
        fig_repeat_state = px.bar(
            df_repeat_state,
            x='repeat_rate',
            y='customer_state',
            orientation='h',
            color='repeat_rate',
            color_continuous_scale='Blues',
            labels={'repeat_rate':'อัตราซื้อซ้ำ', 'customer_state':'รัฐ'},
            text=df_repeat_state['repeat_rate'].map(lambda x: f"{x:.1%}")
        )
        fig_repeat_state.update_layout(
            margin=dict(l=0, r=0, t=30, b=0),
            height=340  # ครึ่งล่างของคอลัมน์ขวา
        )
        fig_repeat_state.update_traces(textposition="outside", cliponaxis=False)
        st.plotly_chart(fig_repeat_state, use_container_width=True, key="repeat_rate_state")
