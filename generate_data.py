import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
np.random.seed(42)


def generate_transactions(n=100000):
    """Generate realistic e-commerce transaction data"""

    categories = ['Electronics', 'Clothing', 'Food', 'Books',
                  'Sports', 'Home', 'Beauty', 'Toys']

    regions = ['Dublin', 'Cork', 'Galway', 'Limerick',
               'Waterford', 'Belfast', 'London', 'Paris']

    payment_methods = ['Credit Card', 'Debit Card',
                       'PayPal', 'Apple Pay', 'Bank Transfer']

    print(f"Generating {n:,} transactions...")

    # Generate base data
    start_date = datetime(2024, 1, 1)

    data = {
        'transaction_id': [f'TXN{i:08d}' for i in range(n)],
        'customer_id': [f'CUST{random.randint(1000, 9999)}' for _ in range(n)],
        'timestamp': [
            start_date + timedelta(
                days=random.randint(0, 364),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            ) for _ in range(n)
        ],
        'category': [random.choice(categories) for _ in range(n)],
        'region': [random.choice(regions) for _ in range(n)],
        'payment_method': [random.choice(payment_methods) for _ in range(n)],
        'amount': np.round(np.random.exponential(scale=50, size=n) + 5, 2),
        'quantity': np.random.randint(1, 10, size=n),
        'is_returned': np.random.choice([0, 1], size=n, p=[0.92, 0.08])
    }

    df = pd.DataFrame(data)

    # Add derived fields
    df['revenue'] = df['amount'] * df['quantity']
    df['month'] = df['timestamp'].dt.month
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['hour'] = df['timestamp'].dt.hour

    # Simulate some data quality issues (realistic)
    null_indices = random.sample(range(n), int(n * 0.02))
    df.loc[null_indices[:len(null_indices)//2], 'region'] = None
    df.loc[null_indices[len(null_indices)//2:], 'payment_method'] = None

    print(f"Generated {len(df):,} transactions")
    print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Total revenue: £{df['revenue'].sum():,.2f}")
    print(f"Null values introduced: {df.isnull().sum().sum()}")

    return df


if __name__ == "__main__":
    df = generate_transactions(100000)
    df.to_csv('raw_transactions.csv', index=False)
    print("\nSaved: raw_transactions.csv")
    print(df.head())
