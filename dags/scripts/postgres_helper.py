import psycopg2
from sqlalchemy import create_engine, text
import scripts.constants as c
import pandas as pd

# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
)

def run_sql(create_sql):
    with engine.connect() as conn:
        conn.execute(text(create_sql))

def clear_numeric_nan (df,column):
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df = df.loc[~(df[column].isnull())]


def upload_overwrite_table(df, table_name):
    
    df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')

    if table_name == 'customer_reviews_google':
        df = df.loc[df['google_id'].str.startswith('0x')]
        clear_numeric_nan(df, 'review_rating')
    
    if table_name == 'company_profiles_google_maps':
        clear_numeric_nan(df, 'rating')

    # Upload DataFrame to PostgreSQL
    df.to_sql(f'{table_name}', engine, index=False, if_exists='replace')

