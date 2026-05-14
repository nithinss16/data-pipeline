import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("LARGE-SCALE DATA PIPELINE")
print("=" * 60)

# ── STAGE 1: INGESTION ────────────────────────────────────────────
print("\n[STAGE 1] Ingesting raw data...")
df = pd.read_csv('raw_transactions.csv', parse_dates=['timestamp'])
print(f"Loaded: {len(df):,} records, {df.shape[1]} columns")
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

# ── STAGE 2: DATA QUALITY VALIDATION ─────────────────────────────
print("\n[STAGE 2] Running data quality checks...")

quality_report = {}

# Check 1: Null values
nulls = df.isnull().sum()
quality_report['null_counts'] = nulls[nulls > 0].to_dict()
print(f"Null values found: {quality_report['null_counts']}")

# Check 2: Duplicate transactions
dupes = df['transaction_id'].duplicated().sum()
quality_report['duplicates'] = dupes
print(f"Duplicate transaction IDs: {dupes}")

# Check 3: Invalid amounts
invalid_amounts = (df['amount'] <= 0).sum()
quality_report['invalid_amounts'] = invalid_amounts
print(f"Invalid amounts (<= 0): {invalid_amounts}")

# Check 4: Revenue anomalies (outliers)
q99 = df['revenue'].quantile(0.99)
anomalies = (df['revenue'] > q99).sum()
quality_report['revenue_anomalies'] = anomalies
print(f"Revenue anomalies (>99th percentile): {anomalies}")

total_issues = dupes + invalid_amounts + anomalies
print(f"Data quality score: {100 - (total_issues / len(df) * 100):.1f}%")

# ── STAGE 3: TRANSFORMATION ───────────────────────────────────────
print("\n[STAGE 3] Transforming data...")

# Fix nulls
df['region'] = df['region'].fillna('Unknown')
df['payment_method'] = df['payment_method'].fillna('Unknown')

# Add business logic features
df['revenue_after_returns'] = df['revenue'] * (1 - df['is_returned'])
df['is_high_value'] = (
    df['revenue'] > df['revenue'].quantile(0.90)).astype(int)
df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday']).astype(int)
df['time_of_day'] = pd.cut(
    df['hour'],
    bins=[0, 6, 12, 18, 24],
    labels=['Night', 'Morning', 'Afternoon', 'Evening'],
    right=False
)

print(f"Cleaned records: {len(df):,}")
print(f"High value transactions: {df['is_high_value'].sum():,}")
print(f"Weekend transactions: {df['is_weekend'].sum():,}")
print("Transformation complete.")

# ── STAGE 4: AGGREGATIONS (Spark-style) ───────────────────────────
print("\n[STAGE 4] Running aggregations...")

# Monthly revenue
monthly = df.groupby('month').agg(
    total_revenue=('revenue_after_returns', 'sum'),
    transaction_count=('transaction_id', 'count'),
    avg_order_value=('revenue', 'mean'),
    unique_customers=('customer_id', 'nunique')
).round(2).reset_index()

# Category performance
category = df.groupby('category').agg(
    total_revenue=('revenue_after_returns', 'sum'),
    transaction_count=('transaction_id', 'count'),
    return_rate=('is_returned', 'mean'),
    avg_order_value=('revenue', 'mean')
).round(4).reset_index().sort_values('total_revenue', ascending=False)

# Regional performance
regional = df.groupby('region').agg(
    total_revenue=('revenue_after_returns', 'sum'),
    transaction_count=('transaction_id', 'count'),
    avg_order_value=('revenue', 'mean')
).round(2).reset_index().sort_values('total_revenue', ascending=False)

# Payment method analysis
payment = df.groupby('payment_method').agg(
    transaction_count=('transaction_id', 'count'),
    total_revenue=('revenue_after_returns', 'sum'),
    avg_value=('revenue', 'mean')
).round(2).reset_index().sort_values('total_revenue', ascending=False)

# Peak hours
hourly = df.groupby('hour').agg(
    transaction_count=('transaction_id', 'count'),
    avg_revenue=('revenue', 'mean')
).round(2).reset_index()

print(f"Monthly aggregation: {len(monthly)} rows")
print(f"Category aggregation: {len(category)} rows")
print(f"Regional aggregation: {len(regional)} rows")

# ── STAGE 5: LOAD TO DATABASE ─────────────────────────────────────
print("\n[STAGE 5] Loading to database...")

engine = create_engine('sqlite:///warehouse.db')

# Load all tables
df.to_sql('transactions', engine, if_exists='replace', index=False)
monthly.to_sql('monthly_revenue', engine, if_exists='replace', index=False)
category.to_sql('category_performance', engine,
                if_exists='replace', index=False)
regional.to_sql('regional_performance', engine,
                if_exists='replace', index=False)
payment.to_sql('payment_analysis', engine, if_exists='replace', index=False)
hourly.to_sql('hourly_patterns', engine, if_exists='replace', index=False)

print("Tables loaded:")
with engine.connect() as conn:
    tables = ['transactions', 'monthly_revenue', 'category_performance',
              'regional_performance', 'payment_analysis', 'hourly_patterns']
    for table in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"  {table}: {count:,} rows")

# ── STAGE 6: ANALYTICAL QUERIES ───────────────────────────────────
print("\n[STAGE 6] Running analytical queries...")

with engine.connect() as conn:
    # Query 1: Top 5 revenue months
    result = conn.execute(text("""
        SELECT month, total_revenue, transaction_count, unique_customers
        FROM monthly_revenue
        ORDER BY total_revenue DESC
        LIMIT 5
    """))
    print("\nTop 5 Revenue Months:")
    for row in result:
        print(f"  Month {row[0]}: £{row[1]:,.2f} "
              f"({row[2]:,} transactions, {row[3]:,} customers)")

    # Query 2: Category return rates
    result = conn.execute(text("""
        SELECT category, return_rate, total_revenue
        FROM category_performance
        ORDER BY return_rate DESC
    """))
    print("\nCategory Return Rates:")
    for row in result:
        print(f"  {row[0]}: {row[1]*100:.1f}% return rate, "
              f"£{row[2]:,.2f} revenue")

# ── STAGE 7: VISUALISATIONS ───────────────────────────────────────
print("\n[STAGE 7] Generating visualisations...")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('E-Commerce Transaction Pipeline Analytics',
             fontsize=16, fontweight='bold')

# Plot 1: Monthly revenue
axes[0, 0].bar(monthly['month'], monthly['total_revenue']/1000,
               color='steelblue', edgecolor='white')
axes[0, 0].set_title('Monthly Revenue (£000s)')
axes[0, 0].set_xlabel('Month')
axes[0, 0].set_ylabel('Revenue (£000s)')

# Plot 2: Category performance
axes[0, 1].barh(category['category'],
                category['total_revenue']/1000,
                color='coral', edgecolor='white')
axes[0, 1].set_title('Revenue by Category (£000s)')
axes[0, 1].set_xlabel('Revenue (£000s)')

# Plot 3: Regional performance
axes[1, 0].bar(regional['region'],
               regional['total_revenue']/1000,
               color='mediumseagreen', edgecolor='white')
axes[1, 0].set_title('Revenue by Region (£000s)')
axes[1, 0].set_xlabel('Region')
axes[1, 0].set_ylabel('Revenue (£000s)')
axes[1, 0].tick_params(axis='x', rotation=45)

# Plot 4: Hourly patterns
axes[1, 1].plot(hourly['hour'], hourly['transaction_count'],
                color='purple', linewidth=2, marker='o', markersize=4)
axes[1, 1].set_title('Transaction Volume by Hour')
axes[1, 1].set_xlabel('Hour of Day')
axes[1, 1].set_ylabel('Transaction Count')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('pipeline_analytics.png', dpi=150, bbox_inches='tight')
plt.close()
print("Saved: pipeline_analytics.png")

print("\n" + "=" * 60)
print("PIPELINE COMPLETE")
print(f"Records processed: {len(df):,}")
print(f"Tables in warehouse: 6")
print(f"Total revenue processed: £{df['revenue_after_returns'].sum():,.2f}")
print("=" * 60)
