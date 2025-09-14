import polars as pl
import duckdb as dd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from streamlit_plotly_events import plotly_events
from datetime import datetime
import numpy as np

st.set_page_config(
    page_title="Interactive Sales Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Sales Data Cube Class ---
class SalesDataCube:
    def __init__(self, fact_sales, dim_customers, dim_date, dim_employees, dim_products):
        self.fact_sales = fact_sales.to_pandas()
        self.dim_customers = dim_customers.to_pandas()
        self.dim_date = dim_date.to_pandas()
        self.dim_employees = dim_employees.to_pandas()
        self.dim_products = dim_products.to_pandas()
        self.cube = None
        
    def create_cube(self):
        """Create comprehensive data cube"""
        cube = self.fact_sales.copy()
        
        cube = cube.merge(
            self.dim_date.add_suffix('_date'), 
            left_on='order_date_key', 
            right_on='date_key_date', 
            how='left'
        )
        
        cube = cube.merge(
            self.dim_customers[['customer_id', 'company_name', 'city', 'country_region', 'state_province']].add_suffix('_customer'),
            left_on='customer_key',
            right_on='customer_id_customer',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_employees[['employee_key', 'full_name', 'job_title', 'city', 'country_region']].add_suffix('_employee'),
            left_on='employee_key',
            right_on='employee_key_employee',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_products[['product_key', 'product_name', 'category', 'standard_cost', 'list_price']].add_suffix('_product'),
            left_on='product_key',
            right_on='product_key_product',
            how='left'
        )
        
        # Calculated measures
        cube['revenue'] = cube['net_amount']
        cube['gross_revenue'] = cube['gross_amount']
        cube['discount_amount'] = cube['gross_amount'] - cube['net_amount']
        cube['total_cost'] = cube['quantity'] * cube['standard_cost_product'].fillna(0)
        cube['profit'] = cube['net_amount'] - cube['total_cost']
        cube['profit_margin'] = (cube['profit'] / cube['net_amount'] * 100).fillna(0)
        cube['order_value'] = cube['net_amount'] + cube['shipping_fee'].fillna(0) + cube['taxes'].fillna(0)
        
        # Time hierarchies
        cube['year_date'] = cube['year_date'].astype('Int64')
        cube['quarter_date'] = cube['quarter_date'].astype('Int64')
        cube['month_date'] = cube['month_date'].astype('Int64')
        cube['year_month'] = cube['year_date'].astype(str) + '-' + cube['month_date'].astype(str).str.zfill(2)
        cube['year_quarter'] = cube['year_date'].astype(str) + '-Q' + cube['quarter_date'].astype(str)
        
        self.cube = cube
        return cube
    
    def get_filtered_data(self, year=None, quarter=None, month=None, city=None, category=None):
        if self.cube is None:
            return pd.DataFrame()
        
        filtered = self.cube.copy()
        if year is not None:
            filtered = filtered[filtered['year_date'] == year]
        if quarter is not None:
            filtered = filtered[filtered['quarter_date'] == quarter]
        if month is not None:
            filtered = filtered[filtered['month_date'] == month]
        if city is not None:
            filtered = filtered[filtered['city_customer'] == city]
        if category is not None:
            filtered = filtered[filtered['category_product'] == category]
            
        return filtered
    
    def get_kpi_summary(self, filtered_data):
        if filtered_data.empty:
            return {
                'total_revenue': 0,
                'total_profit': 0,
                'total_orders': 0,
                'avg_order_value': 0,
                'profit_margin': 0,
                'total_quantity': 0
            }
        
        return {
            'total_revenue': filtered_data['revenue'].sum(),
            'total_profit': filtered_data['profit'].sum(),
            'total_orders': filtered_data['sale_id'].nunique(),
            'avg_order_value': filtered_data['order_value'].mean(),
            'profit_margin': filtered_data['profit_margin'].mean(),
            'total_quantity': filtered_data['quantity'].sum()
        }

# --- Load data ---
@st.cache_data
def load_data():
    conn = dd.connect(r'data_cube/sales_dw.duckdb')
    
    def execute_query(conn, query):
        result = conn.execute(query).fetchdf()
        return pl.from_pandas(result)
    
    dim_customers = execute_query(conn,"SELECT * FROM dim_customers")
    dim_date = execute_query(conn,"SELECT * FROM dim_date")
    dim_employees = execute_query(conn,"SELECT * FROM dim_employees")
    dim_products = execute_query(conn,"SELECT * FROM dim_products")
    fact_sales = execute_query(conn,"SELECT * FROM fact_sales")
    conn.close()
    
    return dim_customers, dim_date, dim_employees, dim_products, fact_sales

dim_customers, dim_date, dim_employees, dim_products, fact_sales = load_data()
sales_cube = SalesDataCube(fact_sales, dim_customers, dim_date, dim_employees, dim_products)
cube_data = sales_cube.create_cube()

# CSS Styling
st.markdown("""
    <style>
    html, body, .main {
        overflow-x: hidden; 
        height: 100vh;
        margin: 0;
        padding: 0;
    }
    .block-container {
        padding: 0.5rem 1rem;
        max-width: 100vw;
    }
    .element-container > div {
        margin-bottom: 0.5rem !important;
    }
    .stPlotlyChart {
        height: 300px !important;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.25rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .metric-title {
        font-size: 0.9rem;
        font-weight: 500;
        opacity: 0.9;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #fff;
    }
    .stButton > button {
        width: 100%;
        height: 2.5rem;
        font-size: 1rem;
        font-weight: bold;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 0.25rem;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    }
    </style>
""", unsafe_allow_html=True)

# Sidebar filters
with st.sidebar:
    st.header("🎯 ตัวกรองข้อมูล")
    
    # แปลงเป็น int เพื่อไม่ให้ทศนิยม
    year_options = [None] + sorted(list({int(y) for y in cube_data["year_date"].dropna().unique()}))
    quarter_options = [None] + sorted(list({int(q) for q in cube_data["quarter_date"].dropna().unique()}))
    month_options = [None] + sorted(list({int(m) for m in cube_data["month_date"].dropna().unique()}))
    city_options = [None] + sorted(cube_data["city_customer"].dropna().unique().tolist())
    category_options = [None] + sorted(cube_data["category_product"].dropna().unique().tolist())
    
    year_filter = st.selectbox("📅 เลือกปี", options=year_options, index=0)
    quarter_filter = st.selectbox("📊 เลือกไตรมาส", options=quarter_options, index=0)
    month_filter = st.selectbox("📆 เลือกเดือน", options=month_options, index=0)
    city_filter = st.selectbox("🏙️ เลือกเมือง", options=city_options, index=0)
    category_filter = st.selectbox("📦 เลือกหมวดหมู่สินค้า", options=category_options, index=0)
    
    if st.button("🔄 รีเซ็ตตัวกรอง"):
        st.rerun()

# Apply filters
filtered_data = sales_cube.get_filtered_data(
    year=year_filter,
    quarter=quarter_filter, 
    month=month_filter,
    city=city_filter,
    category=category_filter
)

# Get KPI summary
kpis = sales_cube.get_kpi_summary(filtered_data)

# Header
st.markdown("### 📊 Interactive Sales Dashboard")

# KPI Cards
kpi_cols = st.columns(6)
kpi_metrics = [
    ("💰 รายได้รวม", f"฿{kpis['total_revenue']:,.0f}"),
    ("💸 กำไรรวม", f"฿{kpis['total_profit']:,.0f}"),
    ("📋 จำนวนคำสั่งซื้อ", f"{kpis['total_orders']:,}"),
    ("📈 มูลค่าเฉลี่ย/คำสั่ง", f"฿{kpis['avg_order_value']:,.0f}"),
    ("📊 อัตรากำไร", f"{kpis['profit_margin']:.1f}%"),
    ("📦 จำนวนสินค้า", f"{kpis['total_quantity']:,}")
]

for i, (title, value) in enumerate(kpi_metrics):
    with kpi_cols[i]:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">{title}</div>
                <div class="metric-value">{value}</div>
            </div>
        """, unsafe_allow_html=True)


st.markdown("---")

# Charts Section
if not filtered_data.empty:
    # Row 1: Top Products, Sales by City, Top Employees
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 10 Products
        top_products = (
            filtered_data.groupby('product_name_product')['revenue']
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        
        fig_products = px.bar(
            top_products,
            x='product_name_product',
            y='revenue',
            title='🏆 Top 10 สินค้าขายดี',
            labels={'revenue': 'รายได้ (฿)', 'product_name_product': 'สินค้า'},
            color='revenue',
            color_continuous_scale='viridis'
        )
        fig_products.update_layout(
            height=350, 
            margin=dict(t=40, b=60, l=10, r=10),
            xaxis_tickangle=-45,
            showlegend=False
        )
        st.plotly_chart(fig_products, use_container_width=True)
    
    with col2:
        # Sales by City
        city_sales = (
            filtered_data.groupby('city_customer')['revenue']
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        
        fig_city = px.bar(
            city_sales,
            x='city_customer',
            y='revenue',
            title='🌍 ยอดขายตามเมือง (Top 10)',
            labels={'revenue': 'รายได้ (฿)', 'city_customer': 'เมือง'},
            color='revenue',
            color_continuous_scale='plasma'
        )
        fig_city.update_layout(
            height=350,
            margin=dict(t=40, b=60, l=10, r=10),
            xaxis_tickangle=-45,
            showlegend=False
        )
        st.plotly_chart(fig_city, use_container_width=True)

    

    # Row 2: Category Treemap and Monthly Trend
    st.markdown("---")
    col3, col4 = st.columns([1, 2])

    with col3:
        # Category Treemap
        category_sales = (
            filtered_data.groupby('category_product')['revenue']
            .sum()
            .reset_index()
        )
        
        fig_category = px.treemap(
            category_sales,
            path=['category_product'],
            values='revenue',
            title='📊 สัดส่วนยอดขายตามหมวดหมู่',
            color='revenue',
            color_continuous_scale='RdYlBu_r'
        )
        fig_category.update_layout(height=400, margin=dict(t=60, b=20, l=10, r=10))
        st.plotly_chart(fig_category, use_container_width=True)
  
    with col4:
        # Monthly Revenue vs Profit
        monthly_financial = (
            filtered_data.groupby('month_name_date')
            .agg({
                'revenue': 'sum',
                'profit': 'sum'
            })
            .reset_index()
        )
        
        fig_financial = go.Figure()
        fig_financial.add_trace(go.Scatter(
            x=monthly_financial['month_name_date'],
            y=monthly_financial['revenue'],
            name='รายได้',
            line=dict(color='#1f77b4', width=3)
        ))
        fig_financial.add_trace(go.Scatter(
            x=monthly_financial['month_name_date'],
            y=monthly_financial['profit'],
            name='กำไร',
            line=dict(color='#ff7f0e', width=3),
            yaxis='y2'
        ))
        
        fig_financial.update_layout(
            title='📊 เปรียบเทียบรายได้และกำไรรายเดือน',
            xaxis_title='เดือน',
            yaxis=dict(title='รายได้ (฿)', side='left', color='#1f77b4'),
            yaxis2=dict(title='กำไร (฿)', side='right', overlaying='y', color='#ff7f0e'),
            height=350,
            margin=dict(t=40, b=20, l=10, r=10),
            legend=dict(x=0.7, y=1)
        )
        st.plotly_chart(fig_financial, use_container_width=True)
    
    # Additional Interactive Summary Table
    st.markdown("---")
    st.markdown("#### 📋 ตารางสรุปข้อมูลที่กรอง")
    
    try:
        if len(filtered_data) > 0:
            # Create summary table
            summary_table = (
                filtered_data.groupby(['category_product', 'city_customer'])
                .agg({
                    'revenue': 'sum',
                    'profit': 'sum',
                    'quantity': 'sum',
                    'sale_id': 'nunique'
                })
                .round(2)
                .reset_index()
            )
            summary_table.columns = ['หมวดหมู่', 'เมือง', 'รายได้', 'กำไร', 'จำนวนสินค้า', 'คำสั่งซื้อ']
            
            # Display interactive table
            st.dataframe(
                summary_table,
                use_container_width=True,
                height=200
            )
            
            # Download button
            csv = summary_table.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="💾 ดาวน์โหลดตาราง CSV",
                data=csv,
                file_name=f'sales_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv'
            )
        else:
            st.info("ไม่มีข้อมูลในตารางสรุป")
            
    except Exception as e:
        st.error(f"Error creating summary table: {e}")
    
else:
    st.warning("⚠️ ไม่มีข้อมูลที่ตรงกับเงื่อนไขการกรองที่เลือก")
    st.info("💡 ลองปรับเงื่อนไขการกรองหรือรีเซ็ตตัวกรองเพื่อดูข้อมูลทั้งหมด")