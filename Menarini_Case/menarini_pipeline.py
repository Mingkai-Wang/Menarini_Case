#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 22:43:49 2025

@author: mingkaiwang
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MenariniDataPipeline:
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)
        self.raw_data = {}
        self.processed_data = {}
        self.data_quality_report = {}
        self.lineage_tracker = {}
        self.data_lake = {'bronze': {}, 'silver': {}, 'gold': {}}
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False

    def _load_config(self, config_path):
        return {
            'data_sources': {
                'sales_data': 'Case Study - Data & AI Engineer.xlsx',
                'market_segments': ['RX', '电子商务', 'Device', 'Retail', 'CSO&DSO', '非目标市场'],
                'key_metrics': ['QTY数量', 'OrderDate订单日期', 'ItemName产品名称']
            },
            'quality_thresholds': {
                'completeness': 0.8,
                'validity': 0.9,
                'consistency': 0.95
            },
            'business_rules': {
                'min_order_qty': 1,
                'valid_date_range': ['2020-01-01', '2025-12-31'],
                'required_fields': ['ID', 'ItemName产品名称', 'QTY数量']
            }
        }

    def extract_data_from_excel(self, file_path):
        logger.info(f"Extracting from {file_path}")
        try:
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            for sheet_name, df in all_sheets.items():
                self.data_lake['bronze'][sheet_name] = {
                    'data': df,
                    'extracted_at': datetime.now(),
                    'source': file_path,
                    'original_shape': df.shape
                }
                self.lineage_tracker[sheet_name] = {
                    'source': file_path,
                    'extraction_timestamp': datetime.now(),
                    'transformations': []
                }
            logger.info(f"Extracted {len(all_sheets)} sheets")
            return True
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return False

    def validate_data_sources(self):
        validation_results = {}
        for sheet_name, sheet_info in self.data_lake['bronze'].items():
            df = sheet_info['data']
            validation_results[sheet_name] = {
                'is_empty': df.empty,
                'has_duplicates': df.duplicated().any(),
                'missing_data_percentage': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100 if df.shape[0] and df.shape[1] else 0,
                'data_types': dict(df.dtypes)
            }
        logger.info("Validation complete")
        return validation_results

    def clean_column_names(self, df, sheet_name):
        original_columns = df.columns.tolist()
        df.columns = (df.columns
                     .str.strip()
                     .str.replace(r'[\n\r]', '', regex=True)
                     .str.replace(r'\s+', '', regex=True))
        self.lineage_tracker[sheet_name]['transformations'].append({
            'step': 'column_name_cleaning',
            'timestamp': datetime.now(),
            'changes': {
                'before': original_columns,
                'after': df.columns.tolist()
            }
        })
        return df

    def standardize_data_types(self, df, sheet_name):
        qty_cols = [col for col in df.columns if any(k in col for k in ['QTY', '数量'])]
        for col in qty_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate', 'reportmonth'])]
        for col in date_cols:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                try:
                    numeric_dates = pd.to_numeric(df[col], errors='coerce')
                    df[col] = pd.to_datetime('1899-12-30') + pd.to_timedelta(numeric_dates, 'D')
                except:
                    pass
        text_cols = df.select_dtypes(include=['object']).columns
        for col in text_cols:
            if col not in date_cols:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', ''], np.nan)
        return df

    def apply_business_rules(self, df, sheet_name):
        rules_applied = []
        qty_cols = [col for col in df.columns if any(k in col for k in ['QTY', '数量'])]
        if qty_cols:
            qty_col = qty_cols[0]
            invalid_qty_mask = df[qty_col] < self.config['business_rules']['min_order_qty']
            invalid_count = invalid_qty_mask.sum()
            if invalid_count > 0:
                df.loc[invalid_qty_mask, qty_col] = np.nan
                rules_applied.append(f"Removed {invalid_count} invalid qty")
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate'])]
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                start_date = pd.to_datetime(self.config['business_rules']['valid_date_range'][0])
                end_date = pd.to_datetime(self.config['business_rules']['valid_date_range'][1])
                invalid_date_mask = (df[col] < start_date) | (df[col] > end_date)
                invalid_dates = invalid_date_mask.sum()
                if invalid_dates > 0:
                    df.loc[invalid_date_mask, col] = pd.NaT
                    rules_applied.append(f"Removed {invalid_dates} invalid dates in {col}")
        self.lineage_tracker[sheet_name]['transformations'].append({
            'step': 'business_rules_application',
            'timestamp': datetime.now(),
            'rules_applied': rules_applied
        })
        return df

    def enrich_data(self, df, sheet_name):
        if 'RX' in sheet_name:
            df['市场类型'] = 'RX处方药市场'
        elif '电子商务' in sheet_name:
            df['市场类型'] = '电子商务市场'
        elif 'Device' in sheet_name:
            df['市场类型'] = '医疗器械市场'
        elif 'Retail' in sheet_name:
            df['市场类型'] = '零售市场'
        elif 'CSO' in sheet_name or 'DSO' in sheet_name:
            df['市场类型'] = 'CSO&DSO市场'
        elif '非目标' in sheet_name:
            df['市场类型'] = '非目标市场'
        else:
            df['市场类型'] = '其他市场'
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate'])]
        if date_cols and pd.api.types.is_datetime64_any_dtype(df[date_cols[0]]):
            date_col = date_cols[0]
            df['年份'] = df[date_col].dt.year
            df['月份'] = df[date_col].dt.month
            df['季度'] = df[date_col].dt.quarter
            df['星期几'] = df[date_col].dt.dayofweek
        df['数据处理时间'] = datetime.now()
        df['数据来源'] = sheet_name
        return df

    def comprehensive_etl_process(self):
        for sheet_name, sheet_info in self.data_lake['bronze'].items():
            if '说明' in sheet_name:
                continue
            df = sheet_info['data'].copy()
            df = self.clean_column_names(df, sheet_name)
            df = df.dropna(axis=1, how='all')
            df = df.dropna(axis=0, how='all')
            df = self.standardize_data_types(df, sheet_name)
            df = self.apply_business_rules(df, sheet_name)
            df = self.enrich_data(df, sheet_name)
            self.data_lake['silver'][sheet_name] = {
                'data': df,
                'processed_at': datetime.now(),
                'transformations_applied': len(self.lineage_tracker[sheet_name]['transformations']),
                'final_shape': df.shape
            }

    def create_unified_data_model(self):
        unified_data = []
        common_columns = None
        for sheet_name, sheet_info in self.data_lake['silver'].items():
            if '说明' in sheet_name or '产品' in sheet_name:
                continue
            df = sheet_info['data']
            if common_columns is None:
                common_columns = set(df.columns)
            else:
                common_columns = common_columns.intersection(set(df.columns))
        for sheet_name, sheet_info in self.data_lake['silver'].items():
            if '说明' in sheet_name or '产品' in sheet_name:
                continue
            df = sheet_info['data']
            unified_df = pd.DataFrame()
            for col in common_columns:
                if col in df.columns:
                    unified_df[col] = df[col]
                else:
                    unified_df[col] = np.nan
            unified_data.append(unified_df)
        if unified_data:
            combined_df = pd.concat(unified_data, ignore_index=True)
            self.data_lake['gold']['unified_sales_data'] = {
                'data': combined_df,
                'created_at': datetime.now(),
                'description': 'Unified sales data across all market segments',
                'shape': combined_df.shape,
                'common_columns': list(common_columns)
            }
            return combined_df
        return pd.DataFrame()

    def generate_data_quality_report(self):
        quality_report = {}
        for layer in ['bronze', 'silver', 'gold']:
            quality_report[layer] = {}
            for table_name, table_info in self.data_lake[layer].items():
                df = table_info['data']
                total_cells = df.shape[0] * df.shape[1]
                missing_cells = df.isnull().sum().sum()
                quality_metrics = {
                    'completeness': 1 - (missing_cells / total_cells) if total_cells > 0 else 0,
                    'row_count': df.shape[0],
                    'column_count': df.shape[1],
                    'duplicate_rows': df.duplicated().sum(),
                    'missing_data_by_column': df.isnull().sum().to_dict(),
                    'data_types': dict(df.dtypes),
                    'quality_score': 0
                }
                completeness_score = quality_metrics['completeness']
                duplicate_penalty = quality_metrics['duplicate_rows'] / df.shape[0] if df.shape[0] > 0 else 0
                quality_metrics['quality_score'] = max(0, completeness_score - duplicate_penalty)
                quality_report[layer][table_name] = quality_metrics
        self.data_quality_report = quality_report
        return quality_report

    def create_data_dictionary(self):
        data_dictionary = {}
        if 'unified_sales_data' in self.data_lake['gold']:
            df = self.data_lake['gold']['unified_sales_data']['data']
            for column in df.columns:
                if any(k in column for k in ['ID', 'Code', '代码']):
                    category = 'Identifier'
                elif any(k in column for k in ['Name', '名称']):
                    category = 'Name/Description'
                elif any(k in column for k in ['QTY', '数量']):
                    category = 'Quantity/Measure'
                elif any(k in column.lower() for k in ['date', '日期']):
                    category = 'Date/Time'
                elif any(k in column for k in ['Province', 'City', '省份', '城市']):
                    category = 'Geographic'
                elif '市场类型' in column:
                    category = 'Market Segment'
                else:
                    category = 'Other'
                sample_values = df[column].dropna().head(3).tolist()
                data_dictionary[column] = {
                    'category': category,
                    'data_type': str(df[column].dtype),
                    'null_count': int(df[column].isnull().sum()),
                    'unique_count': int(df[column].nunique()),
                    'sample_values': sample_values,
                    'description': column
                }
        return data_dictionary

    def generate_technical_report(self):
        if 'unified_sales_data' not in self.data_lake['gold']:
            return "No processed data available for reporting"
        df = self.data_lake['gold']['unified_sales_data']['data']
        quality_report = self.generate_data_quality_report()
        report = f"""
# MENARINI ASIA PACIFIC - DATA PIPELINE TECHNICAL REPORT
## Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### EXECUTIVE SUMMARY
- Successfully processed {len(self.data_lake['bronze'])} data sources
- Created unified data model with {df.shape[0]:,} records and {df.shape[1]} attributes
- Achieved {(1 - df.isnull().sum().sum()/(df.shape[0]*df.shape[1])):.1%} overall data completeness
"""
        for layer, tables in quality_report.items():
            report += f"\n#### {layer.upper()} LAYER QUALITY\n"
            for table, metrics in tables.items():
                report += f"- {table}: {metrics['quality_score']:.2%} quality score\n"
        return report

    def run_complete_pipeline(self, file_path):
        logger.info("=" * 80)
        logger.info("MENARINI ASIA PACIFIC - DATA PIPELINE EXECUTION")
        logger.info("=" * 80)
        try:
            if not self.extract_data_from_excel(file_path):
                raise Exception("Data extraction failed")
            self.validate_data_sources()
            self.comprehensive_etl_process()
            unified_data = self.create_unified_data_model()
            self.generate_data_quality_report()
            data_dictionary = self.create_data_dictionary()
            technical_report = self.generate_technical_report()
            return {
                'status': 'success',
                'unified_data': unified_data,
                'data_dictionary': data_dictionary,
                'technical_report': technical_report
            }
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {'status': 'failed', 'error': str(e)}
