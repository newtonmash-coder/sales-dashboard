# ==========================================================
# app.py — Reads from local disk cache written by preload.py
# Loads instantly. No Google Drive calls at page load.
# ==========================================================

import os
import streamlit as st
import pandas as pd
import numpy as np

from data_engine import (
    load_sales_data,
    load_targets_data,
    create_time_features,
    merge_sales_with_targets,
)

st.set_page_config(page_title="Spread Masters Sales Dashboard", layout="wide")
st.title("📊 Spread Masters Sales Dashboard")

# ----------------------------------------------------------
# SIDEBAR
# ----------------------------------------------------------
st.sidebar.title("Spread Masters")
st.sidebar.markdown("---")

if st.sidebar.button("🔄 Refresh Data Now"):
    # Clears disk cache + triggers Render redeploy is not possible
    # so this just clears Streamlit's in-memory cache to re-read disk
    st.cache_data.clear()
    st.session_state.pop("df", None)
    st.rerun()

st.sidebar.caption("Data refreshes on server restart")
st.sidebar.markdown("---")

# ----------------------------------------------------------
# LOAD FROM DISK (instant — no Drive calls)
# ----------------------------------------------------------
DATA_DIR = "data_cache"

if not os.path.exists(DATA_DIR) or not os.path.exists(os.path.join(DATA_DIR, "sales.parquet")):
    st.error(
        "⚠️ Data files not found on disk. "
        "This usually means the server just restarted and preload.py hasn't finished yet. "
        "Please wait 60 seconds and refresh the page."
    )
    st.info("If this persists, check your Render logs for PRELOAD errors.")
    st.stop()

if "df" not in st.session_state:
    with st.spinner("Reading data..."):
        sales   = load_sales_data()
        targets = load_targets_data()

        if sales.empty:
            st.error("No sales data found.")
            st.stop()

        sales = create_time_features(sales)
        df    = merge_sales_with_targets(sales, targets)
        st.session_state["df"] = df
else:
    df = st.session_state["df"]

# ----------------------------------------------------------
# COLUMN GUARD
# ----------------------------------------------------------
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.sort_values("Date")

required_cols = ["Year", "Month", "DT_Name", "Brand", "FSR",
                 "Sales", "Sales_Targets", "outlet_code"]
missing_cols  = [c for c in required_cols if c not in df.columns]

if missing_cols:
    st.error(f"Missing columns: {missing_cols}")
    st.info(f"Columns in data: {list(df.columns)}")
    st.stop()

st.success(f"✅ {len(df):,} rows from {df['SourceFile'].nunique()} file(s)")

# ----------------------------------------------------------
# SIDEBAR FILTERS
# ----------------------------------------------------------
st.sidebar.header("Filters")

year_filter  = st.sidebar.selectbox("Select Year",  sorted(df["Year"].dropna().unique(),  reverse=True))
month_filter = st.sidebar.selectbox("Select Month", sorted(df["Month"].dropna().unique()))
dt_filter    = st.sidebar.multiselect("Select DT_Name", sorted(df["DT_Name"].dropna().unique()))
brand_filter = st.sidebar.multiselect("Select Brand",   sorted(df["Brand"].dropna().unique()))
fsr_filter   = st.sidebar.multiselect("Select FSR",     sorted(df["FSR"].dropna().unique()))

# ----------------------------------------------------------
# FILTERING
# ----------------------------------------------------------
base = df.copy()
if dt_filter:    base = base[base["DT_Name"].isin(dt_filter)]
if brand_filter: base = base[base["Brand"].isin(brand_filter)]
if fsr_filter:   base = base[base["FSR"].isin(fsr_filter)]

filtered = base[(base["Year"] == year_filter) & (base["Month"] == month_filter)].copy()

selected_quarter = filtered["Quarter"].max() if not filtered.empty else 1
qtd_data = base[(base["Year"] == year_filter) & (base["Quarter"] == selected_quarter)].copy()
ytd_data = base[base["Year"] == year_filter].copy()

prev_month = month_filter - 1
prev_year  = year_filter
if prev_month == 0:
    prev_month = 12
    prev_year -= 1

previous_data  = base[(base["Year"] == prev_year) & (base["Month"] == prev_month)].copy()
prev_customers = set(previous_data["outlet_code"].dropna().unique())
curr_customers = set(filtered["outlet_code"].dropna().unique())
lost_customers = prev_customers - curr_customers
new_customers  = curr_customers - prev_customers

# ----------------------------------------------------------
# KPIs
# ----------------------------------------------------------
total_sales  = filtered["Sales"].sum()
total_target = filtered["Sales_Targets"].sum()
achievement  = (total_sales / total_target * 100) if total_target > 0 else 0
cust_mtd     = filtered["outlet_code"].nunique()
cust_qtd     = qtd_data["outlet_code"].nunique()
cust_ytd     = ytd_data["outlet_code"].nunique()

# ----------------------------------------------------------
# TABS
# ----------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Executive Summary", "FSR Performance",
    "Customer Analytics", "Brand Analytics", "Raw Data"
])

# ==========================================================
# TAB 1 — EXECUTIVE SUMMARY
# ==========================================================
with tab1:
    st.subheader("📌 Executive KPIs")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sales",         f"{total_sales:,.0f}")
    c2.metric("Targets",       f"{total_target:,.0f}")
    c3.metric("Achievement %", f"{achievement:.1f}%")
    c4.metric("Customers MTD", f"{cust_mtd:,}")
    c5, c6, c7 = st.columns(3)
    c5.metric("Customers QTD",     f"{cust_qtd:,}")
    c6.metric("Customers YTD",     f"{cust_ytd:,}")
    c7.metric("Net Outlet Growth", f"{len(new_customers) - len(lost_customers):,}")

# ==========================================================
# TAB 2 — FSR PERFORMANCE
# ==========================================================
with tab2:
    st.subheader("📌 FSR Performance")
    fsr_table = (
        filtered.groupby(["FSR", "Brand"])
        .agg({"Sales": "sum", "Sales_Targets": "sum"})
        .reset_index()
    )
    fsr_table["Achievement %"] = np.where(
        fsr_table["Sales_Targets"] > 0,
        (fsr_table["Sales"] / fsr_table["Sales_Targets"] * 100).round(1), 0
    )
    st.dataframe(fsr_table, use_container_width=True)

    st.subheader("📌 FSR Reach")
    reach = (
        filtered.groupby("FSR")
        .agg({"Sales": "sum", "outlet_code": pd.Series.nunique})
        .reset_index()
        .rename(columns={"outlet_code": "Customers Billed"})
    )
    reach["Avg Sale per Outlet"] = (reach["Sales"] / reach["Customers Billed"]).round(2)
    st.dataframe(reach, use_container_width=True)

    st.subheader("🏆 FSR Scorecard")
    score = reach.copy()
    score = score.merge(
        fsr_table.groupby("FSR")[["Sales_Targets"]].sum().reset_index(),
        on="FSR", how="left"
    )
    score["Achievement %"]  = np.where(score["Sales_Targets"] > 0, score["Sales"] / score["Sales_Targets"] * 100, 0)
    potential_total          = base["outlet_code"].nunique()
    score["Strike Rate %"]  = score["Customers Billed"] / potential_total * 100
    score["Sales Score"]    = (score["Achievement %"]    / score["Achievement %"].max())    * 40
    score["Reach Score"]    = (score["Customers Billed"] / score["Customers Billed"].max()) * 25
    score["Strike Score"]   = (score["Strike Rate %"]    / score["Strike Rate %"].max())    * 20
    score["Total Score"]    = (score["Sales Score"] + score["Reach Score"] + score["Strike Score"]).round(1)
    score = score.sort_values("Total Score", ascending=False).reset_index(drop=True)
    score["Award"] = ""
    for i, medal in enumerate(["🥇", "🥈", "🥉"]):
        if i < len(score): score.loc[i, "Award"] = medal
    st.dataframe(score, use_container_width=True)

# ==========================================================
# TAB 3 — CUSTOMER ANALYTICS
# ==========================================================
with tab3:
    st.subheader("🅰 Repeat vs New")
    first_purchase = (
        df.groupby("outlet_code")["Date"].min()
        .reset_index().rename(columns={"Date": "First_Date"})
    )
    rpt = filtered.merge(first_purchase, on="outlet_code", how="left")
    rpt["Type"] = np.where(
        (rpt["First_Date"].dt.year == year_filter) & (rpt["First_Date"].dt.month == month_filter),
        "New", "Repeat"
    )
    st.dataframe(rpt.groupby("Type")["outlet_code"].nunique().reset_index(), use_container_width=True)

    st.subheader("🚨 Lost Customers")
    if not previous_data.empty:
        lost_df = previous_data[previous_data["outlet_code"].isin(lost_customers)][
            ["outlet_code", "FSR", "DT_Name", "Brand"]
        ].drop_duplicates()
        st.dataframe(lost_df, use_container_width=True)
    else:
        st.info("No previous month data to compare.")

    st.subheader("🅲 Billing Frequency")
    freq = (
        filtered.groupby("outlet_code").size()
        .reset_index(name="Billing Count")
        .sort_values("Billing Count", ascending=False)
    )
    st.dataframe(freq, use_container_width=True)

# ==========================================================
# TAB 4 — BRAND ANALYTICS
# ==========================================================
with tab4:
    st.subheader("📦 Numeric Distribution")
    num_dist = filtered.groupby("Brand")["outlet_code"].nunique().reset_index()
    num_dist["Numeric Distribution %"] = (
        (num_dist["outlet_code"] / cust_mtd * 100).round(1) if cust_mtd > 0 else 0
    )
    st.dataframe(num_dist, use_container_width=True)

    st.subheader("🏪 Weighted Distribution")
    market       = filtered.groupby("outlet_code")["Sales"].sum().reset_index()
    total_market = market["Sales"].sum()
    brand_df     = filtered.groupby(["Brand", "outlet_code"])["Sales"].sum().reset_index()
    brand_df     = brand_df.merge(market, on="outlet_code", suffixes=("_Brand", "_Outlet"))
    wd           = brand_df.groupby("Brand").agg({"Sales_Outlet": "sum"}).reset_index()
    wd["Weighted Distribution %"] = (
        (wd["Sales_Outlet"] / total_market * 100).round(1) if total_market > 0 else 0
    )
    st.dataframe(wd, use_container_width=True)

    st.subheader("🔎 Brand → SKU Sales")
    sku = (
        filtered.groupby(["Brand", "SKU"]).agg({"Sales": "sum"})
        .reset_index().sort_values("Sales", ascending=False)
    )
    st.dataframe(sku, use_container_width=True)

# ==========================================================
# TAB 5 — RAW DATA
# ==========================================================
with tab5:
    st.subheader("📄 Detailed Raw Data")
    st.dataframe(filtered, use_container_width=True)