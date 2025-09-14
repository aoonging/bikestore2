
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