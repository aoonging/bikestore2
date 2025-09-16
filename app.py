import streamlit as st
import duckdb as dd
import polars as pl

conn = dd.connect('data_cube/bikestore.duckdb')

def execute_query(conn, query):
        result = conn.execute(query).fetchdf()
        return pl.from_pandas(result)

st.set_page_config(
    page_title="Dashboard Overview",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Bikestore Dashboard Overview")
st.markdown("""
ข้อมูลนี้เป็นชุดตัวอย่างของร้านขายจักรยาน ที่มี 3 สาขาในประเทศสหรัฐอเมริกา ประกอบด้วยข้อมูล 9 ตาราง ได้แก่
- **brands** : ยี่ห้อของจักรยาน  
- **categories** : ประเภทของจักรยาน  
- **customers** : ข้อมูลลูกค้า  
- **order_items** : รายละเอียดรายการย่อยของใบสั่งซื้อ  
- **orders** : ข้อมูลใบสั่งซื้อ  
- **products** : ข้อมูลสินค้า  
- **staffs** : พนักงาน  
- **stocks** : สินค้าคงเหลือ  
- **stores** : ร้านสาขา
""")
st.write("เลือกดูรายละเอียดของ Dashboard แต่ละหน้าได้จากเมนูด้านซ้าย หรือกดลิงก์ด้านล่าง")

# Main Page Description

# Customers Dashboard
st.subheader("⭐️ Customers Dashboard")
st.write("""
- KPI Cards แสดงจำนวนลูกค้าทั้งหมด, ลูกค้าซื้อซ้ำ, ลูกค้าซื้อซ้ำ
- Treemap แสดงจำนวนลูกค้าตามรัฐ
- กราฟแสดงจำนวนลูกค้าซื้อซ้ำ vs ลูกค้าใหม่ (แต่ละรัฐ)
- อัตราลูกค้าซื้อซ้ำตามรัฐ
""")
st.markdown("[ไปที่ Customers Dashboard](Customer_Dashboard)")

# Employee Dashboard
st.subheader("⭐️ Employee Dashboard")
st.write("""
- KPI Cards แสดงจำนวนพนักงานขาย, ยอดขายเฉลี่ยต่อพนักงาน, และชื่อพนักงานขายยอดเยี่ยม  
- กราฟแสดงยอดขายและจำนวนออเดอร์ของแต่ละสาขา
- กราฟแสดงยอดขายและจำนวนออเดอร์ของพนักงานขายแต่ละคน
""")
st.markdown("[ไปที่ Employee Dashboard](Employee_Dashboard)")

st.subheader("⭐️ Sales Dashboard")
st.write("""
- KPI Cards แสดงยอดขายรวม, จำนวนออเดอร์, จำนวนลูกค้า, ค่าเฉลี่ยต่อออเดอร์**
- กราฟแสดงแนวโน้มยอดขายและออเดอร์ตามช่วงเวลา
- กราฟแสดง Top10 สินค้าขายดี
- Treemap แสดงสัดส่วนยอดขายตามแบรนด์และหมวดหมู่สินค้า
- กราฟแสดงผลของส่วนลดต่อปริมาณ & รายได้
""")
st.markdown("[ไปที่ Sale Dashboard](Sale_Dashboard)")
# st.title("📊 Dashboard Overview")
# st.write("เลือกดูรายละเอียดของ Dashboard แต่ละหน้าได้จากเมนูด้านซ้าย หรือกดลิงก์ด้านล่าง")
    
# # ...existing code...
# dim_customers = execute_query(conn, "SELECT * FROM dim_customers")
# dim_date = execute_query(conn, "SELECT * FROM dim_date")
# dim_staffs = execute_query(conn, "SELECT * FROM dim_staffs")
# dim_products = execute_query(conn, "SELECT * FROM dim_products")
# dim_brands = execute_query(conn, "SELECT * FROM dim_brands")
# dim_categories = execute_query(conn, "SELECT * FROM dim_categories")
# dim_stores = execute_query(conn, "SELECT * FROM dim_stores")
# fact_sales = execute_query(conn, "SELECT * FROM fact_sales")
# conn.close()

# st.write("### dim_customers")
# st.write(dim_customers.head(5))
# st.write("### dim_date")
# st.write(dim_date.head(5))
# st.write("### dim_staffs")
# st.write(dim_staffs.head(5))
# st.write("### dim_products")
# st.write(dim_products.head(5))
# st.write("### dim_brands")
# st.write(dim_brands.head(5))
# st.write("### dim_categories")
# st.write(dim_categories.head(5))
# st.write("### dim_stores")
# st.write(dim_stores.head(5))
# st.write("### fact_sales")
# st.write(fact_sales.head(5))
# # ...existing code...

