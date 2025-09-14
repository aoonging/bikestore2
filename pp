st.set_page_config(
    page_title="Sales Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📊"
)

# --- Sales Data Cube Class ---
class SalesDataCube:
    def __init__(self, fact_sales, dim_customers, dim_date, dim_staffs, dim_products, dim_brands, dim_categories, dim_stores):
        self.fact_sales = fact_sales.to_pandas()
        self.dim_customers = dim_customers.to_pandas()
        self.dim_date = dim_date.to_pandas()
        self.dim_staffs = dim_staffs.to_pandas()
        self.dim_products = dim_products.to_pandas()
        self.dim_brands = dim_brands.to_pandas()
        self.dim_categories = dim_categories.to_pandas()
        self.dim_stores = dim_stores.to_pandas()
        self.cube = None
       
    def create_cube(self):
        """Create comprehensive data cube"""
        cube = self.fact_sales.copy()

        # แก้ไขตรงนี้ให้ join กับวันที่ถูกต้อง
        cube = cube.merge(
            self.dim_date.add_suffix('_date'),
            left_on='order_date',            # ใช้ order_date ตาม transform.py
            right_on='date_key_date',        # date_key ใน dim_date + suffix
            how='left'
        )
        
        cube = cube.merge(
            self.dim_customers[['customer_id', 'first_name', 'last_name', 'customer_fullname', 'customer_phone', 'customer_email', 'customer_street', 'customer_city', 'customer_state', 'customer_zipcode']].add_suffix('_customer'),
            left_on='customer_id',
            right_on='customer_id_customer',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_staffs[['staff_id', 'staff_fullname', 'staff_email', 'staff_phone', 'staff_active', 'store_id', 'manager_id']].add_suffix('_staff'),
            left_on='staff_id',
            right_on='staff_id_staff',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_products[['product_id', 'product_name', 'brand_id', 'category_id', 'model_year', 'list_price']].add_suffix('_product'),
            left_on='product_id',
            right_on='product_id_product',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_brands[['brand_id', 'brand_name']].add_suffix('_brand'),
            left_on='brand_id_product',
            right_on='brand_id_brand',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_categories[['category_id', 'category_name']].add_suffix('_category'),
            left_on='category_id_product',
            right_on='category_id_category',
            how='left'
        )
        
        cube = cube.merge(
            self.dim_stores[['store_id', 'store_name', 'store_phone', 'store_email', 'store_street', 'store_city', 'store_state', 'store_zip_code']].add_suffix('_store'),
            left_on='store_id',
            right_on='store_id_store',
            how='left'
        )
        
        # Calculated measures
        cube['revenue'] = cube['net_amount']
        cube['gross_revenue'] = cube['gross_amount']
        cube['discount_amount'] = cube['gross_amount'] - cube['net_amount']
        cube['total_cost'] = cube['quantity'] * cube['list_price_product'].fillna(0)
        cube['profit'] = cube['net_amount'] - cube['total_cost']
        cube['profit_margin'] = (cube['profit'] / cube['net_amount'] * 100).fillna(0)
        cube['order_value'] = cube['net_amount']  # เพิ่มเติมได้ตามต้องการ

        # Time hierarchies (ต้องตรวจสอบว่าคอลัมน์เหล่านี้มีจริงหลัง merge)
        cube['year_date'] = cube['year_date'].astype('Int64')
        cube['quarter_date'] = cube['quarter_date'].astype('Int64')
        cube['month_date'] = cube['month_date'].astype('Int64')
        cube['year_month'] = cube['year_date'].astype(str) + '-' + cube['month_date'].astype(str).str.zfill(2)
        cube['year_quarter'] = cube['year_date'].astype(str) + '-Q' + cube['quarter_date'].astype(str)
        
        self.cube = cube
        return cube

# --- Load data ---
@st.cache_data
def load_data():
    conn = dd.connect(r'/Users/mac/Downloads/Web/data_cube/bikestore.duckdb')
    
    def execute_query(conn, query):
        result = conn.execute(query).fetchdf()
        return pl.from_pandas(result)
    
    dim_customers = execute_query(conn,"SELECT * FROM dim_customers")
    dim_date = execute_query(conn,"SELECT * FROM dim_date")
    dim_staffs = execute_query(conn,"SELECT * FROM dim_staffs")
    dim_products = execute_query(conn,"SELECT * FROM dim_products")
    dim_brands = execute_query(conn,"SELECT * FROM dim_brands")
    dim_categories = execute_query(conn,"SELECT * FROM dim_categories")
    dim_stores = execute_query(conn,"SELECT * FROM dim_stores")
    fact_sales = execute_query(conn,"SELECT * FROM fact_sales")
    conn.close()
    
    return dim_customers, dim_date, dim_staffs, dim_products, dim_brands, dim_categories, dim_stores, fact_sales

dim_customers, dim_date, dim_staffs, dim_products, dim_brands, dim_categories, dim_stores, fact_sales = load_data()
sales_cube = SalesDataCube(fact_sales, dim_customers, dim_date, dim_staffs, dim_products, dim_brands, dim_categories, dim_stores)
cube_data = sales_cube.create_cube()
# ...existing code...

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

# แสดงตัวอย่างข้อมูล 8 ตารางของ bikestore
with st.expander("🚲 ดูตัวอย่างข้อมูล 8 ตาราง (Bikestore Data Sample)"):
    st.write("### dim_customers (ลูกค้า)")
    st.write(dim_customers.head(5))
    st.write("### dim_date (วันที่)")
    st.write(dim_date.head(5))
    st.write("### dim_staffs (พนักงาน)")
    st.write(dim_staffs.head(5))
    st.write("### dim_products (สินค้า)")
    st.write(dim_products.head(5))
    st.write("### dim_brands (แบรนด์)")
    st.write(dim_brands.head(5))
    st.write("### dim_categories (หมวดหมู่สินค้า)")
    st.write(dim_categories.head(5))
    st.write("### dim_stores (สาขา)")
    st.write(dim_stores.head(5))
    st.write("### fact_sales (ยอดขาย)")
    st.write(fact_sales.head(5))

# Sidebar filters
with st.sidebar:
    st.header("🎯 ตัวกรองข้อมูล Bikestore")
    
    year_options = [None] + sorted(list({int(y) for y in cube_data["year_date"].dropna().unique()}))
    quarter_options = [None] + sorted(list({int(q) for q in cube_data["quarter_date"].dropna().unique()}))
    month_options = [None] + sorted(list({int(m) for m in cube_data["month_date"].dropna().unique()}))
    city_options = [None] + sorted(cube_data["city_customer"].dropna().unique().tolist())
    category_options = [None] + sorted(cube_data["category_product"].dropna().unique().tolist())
    
    year_filter = st.selectbox("📅 เลือกปี (Bikestore)", options=year_options, index=0)
    quarter_filter = st.selectbox("📊 เลือกไตรมาส (Bikestore)", options=quarter_options, index=0)
    month_filter = st.selectbox("📆 เลือกเดือน (Bikestore)", options=month_options, index=0)
    city_filter = st.selectbox("🏙️ เลือกเมือง (Bikestore)", options=city_options, index=0)
    category_filter = st.selectbox("📦 เลือกหมวดหมู่สินค้า (Bikestore)", options=category_options, index=0)
    
    if st.button("🔄 รีเซ็ตตัวกรอง Bikestore"):
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

st.markdown("---")
# Header
st.markdown("### 🚲 Bikestore Sales Dashboard")

# KPI Cards
kpi_cols = st.columns(6)
kpi_metrics = [
    ("💰 รายได้รวม (Bikestore)", f"฿{kpis['total_revenue']:,.0f}"),
    ("💸 กำไรรวม (Bikestore)", f"฿{kpis['total_profit']:,.0f}"),
    ("📋 จำนวนคำสั่งซื้อ (Bikestore)", f"{kpis['total_orders']:,}"),
    ("📈 มูลค่าเฉลี่ย/คำสั่ง (Bikestore)", f"฿{kpis['avg_order_value']:,.0f}"),
    ("📊 อัตรากำไร (Bikestore)", f"{kpis['profit_margin']:.1f}%"),
    ("📦 จำนวนสินค้า (Bikestore)", f"{kpis['total_quantity']:,}")
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
    # Row 1: Top Products, Sales by City
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
            title='🏆 Top 10 สินค้าขายดี (Bikestore)',
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
            title='🌍 ยอดขายตามเมือง (Top 10) (Bikestore)',
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
            title='📊 สัดส่วนยอดขายตามหมวดหมู่ (Bikestore)',
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
            name='รายได้ (Bikestore)',
            line=dict(color='#1f77b4', width=3)
        ))
        fig_financial.add_trace(go.Scatter(
            x=monthly_financial['month_name_date'],
            y=monthly_financial['profit'],
            name='กำไร (Bikestore)',
            line=dict(color='#ff7f0e', width=3),
            yaxis='y2'
        ))
        
        fig_financial.update_layout(
            title='📊 เปรียบเทียบรายได้และกำไรรายเดือน (Bikestore)',
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
    st.markdown("#### 📋 ตารางสรุปข้อมูลที่กรอง (Bikestore)")
    
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
                label="💾 ดาวน์โหลดตาราง CSV (Bikestore)",
                data=csv,
                file_name=f'bikestore_sales_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv'
            )
        else:
            st.info("ไม่มีข้อมูลในตารางสรุป (Bikestore)")
            
    except Exception as e:
        st.error(f"Error creating summary table: {e}")
    
else:
    st.warning("⚠️ ไม่มีข้อมูลที่ตรงกับเงื่อนไขการกรองที่เลือก (Bikestore)")
    st.info("💡 ลองปรับเงื่อนไขการกรองหรือรีเซ็ตตัวกรองเพื่อดูข้อมูลทั้งหมด (Bikestore)")

st.title("🚲 Bikestore Interactive Sales Dashboard")