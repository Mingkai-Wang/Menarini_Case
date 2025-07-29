#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 22:41:31 2025

@author: mingkaiwang
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from menarini_pipeline import MenariniDataPipeline

st.set_page_config(page_title="Menarini Asia Pacific - Data Pipeline", layout="wide")

st.title("🏥 Menarini Asia Pacific - Data Pipeline Demo")
st.markdown(
    """
    这是一个制药销售数据的端到端数据管道演示系统。  
    上传原始 Excel 文件，自动完成 ETL、数据质量评估、统一数据建模、技术报告生成和可视化分析。
    """
)

uploaded_file = st.file_uploader("上传你的 Excel 原始数据文件", type=["xlsx"])

if uploaded_file is not None:
    with st.spinner("数据处理中，请稍等..."):
        # 将上传文件存为临时文件
        temp_file_path = f"./tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        # 运行数据管道
        pipeline = MenariniDataPipeline()
        results = pipeline.run_complete_pipeline(temp_file_path)

    if results["status"] == "success":
        st.success("✅ 数据管道已成功执行！")

        df = results["unified_data"]
        if not df.empty:
            st.subheader("📊 统一销售数据（部分预览）")
            st.dataframe(df.head(30))

            # 数据筛选区
            st.sidebar.header("筛选与分析选项")
            cols = df.columns.tolist()
            if "市场类型" in cols:
                market_filter = st.sidebar.multiselect(
                    "选择市场类型", options=sorted(df["市场类型"].dropna().unique()), default=None
                )
                if market_filter:
                    df = df[df["市场类型"].isin(market_filter)]

            # 指定产品、地区、数量字段
            product_col = next((c for c in cols if "产品" in c or "Product" in c), None)
            qty_col = next((c for c in cols if "数量" in c or "QTY" in c), None)
            region_col = next((c for c in cols if "区域" in c or "Region" in c or "省份" in c), None)

            # 柱状图：各市场总销量
            if qty_col and "市场类型" in cols:
                st.subheader("各市场总销量")
                plot_data = df.groupby("市场类型")[qty_col].sum().sort_values(ascending=False)
                fig, ax = plt.subplots(figsize=(8, 4))
                plot_data.plot(kind="barh", ax=ax, color="skyblue")
                ax.set_xlabel("总销量")
                ax.set_ylabel("市场类型")
                st.pyplot(fig)

            # 柱状图：Top 10 产品销量
            if qty_col and product_col:
                st.subheader("Top 10 产品销量")
                plot_data = df.groupby(product_col)[qty_col].sum().nlargest(10)
                fig2, ax2 = plt.subplots(figsize=(8, 4))
                plot_data.plot(kind="bar", ax=ax2, color="orange")
                ax2.set_ylabel("销量")
                st.pyplot(fig2)

            # 饼图：各地区销量分布
            if qty_col and region_col:
                st.subheader("主要地区销量分布")
                plot_data = df.groupby(region_col)[qty_col].sum().nlargest(8)
                fig3, ax3 = plt.subplots(figsize=(7, 5))
                plot_data.plot.pie(ax=ax3, autopct="%1.1f%%")
                ax3.set_ylabel("")
                st.pyplot(fig3)

        # 数据字典与技术报告
        with st.expander("📋 数据字典（Data Dictionary）", expanded=False):
            data_dict = results.get("data_dictionary", {})
            if data_dict:
                dd = pd.DataFrame([
                    {
                        "字段": k,
                        "类型": v["data_type"],
                        "分类": v["category"],
                        "空值数": v["null_count"],
                        "唯一值数": v["unique_count"],
                        "示例值": ",".join(map(str, v["sample_values"]))
                    } for k, v in data_dict.items()
                ])
                st.dataframe(dd)
            else:
                st.info("暂无数据字典信息。")

        with st.expander("📄 技术报告 Technical Report", expanded=False):
            st.code(results["technical_report"], language="markdown")

    else:
        st.error(f"❌ 管道执行失败：{results['error']}")
else:
    st.info("请先上传数据文件。")

st.markdown("---")
st.caption("Powered by Streamlit | Demo by Mingkai Wang | 2025")
