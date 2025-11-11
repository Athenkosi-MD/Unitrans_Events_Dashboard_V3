import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# ---- Database connection ----
DB_USER = "postgres"
DB_PASSWORD = "Nicolaas24"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Unitrans_Dashboard"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ---- Load driver_rating table ----
query = "SELECT * FROM driver_rating"
df = pd.read_sql(query, engine)

# ---- Keep only required columns ----
columns = ['assetName', 'driverName', 'dateStart', 'cost', 'distance', '100kmh', 'excessivei', 
           'speedingtr', 'brake', 'accel', 'corner', 'gforce']
df = df[columns]

# Convert dateStart to date only
df['date'] = pd.to_datetime(df['dateStart']).dt.date

# ---- Aggregate totals per asset per day ----
daily_totals = df.groupby(['assetName', 'date']).sum(numeric_only=True).reset_index()

# ---- Compute score per asset per day ----
# score = total distance / total cost (avoid division by zero)
daily_totals['score'] = daily_totals.apply(lambda x: x['distance'] / x['cost'] if x['cost'] else 0, axis=1)

# ---- Reorder columns for display ----
display_columns = ['assetName', 'date', 'distance', 'cost', '100kmh', 'excessivei', 
                   'speedingtr', 'brake', 'accel', 'corner', 'gforce', 'score']
daily_totals = daily_totals[display_columns]

# ---- Streamlit display ----
st.title("Driver Scoring Dashboard - Daily Totals by Asset")
st.dataframe(daily_totals)