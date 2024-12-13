import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from datetime import datetime

@st.cache_resource
def init_connection():
    try:
        connection = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/mudah_cars")
        return connection
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

@st.cache_data(ttl=300)
def load_car_data():
    try:
        engine = init_connection()
        query = """
        SELECT 
            make, model, price, year, location,
            mileage_min, mileage_max, transmission,
            fuel_type, car_type, listing_date, image_count
        FROM public.car_listings
        WHERE price > 0 
        AND price < 1000000
        AND year ~ '^[0-9]{4}$'
        """
        df = pd.read_sql(query, engine)
        process_vehicle_data(df)
        return df
    except Exception as e:
        st.error(f"Failed to load car data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_motorcycle_data():
    try:
        engine = init_connection()
        query = """
        SELECT 
            make, model, price, year, location,
            listing_date, image_count
        FROM public.motorcycle_listings
        WHERE price > 0 
        AND price < 100000
        AND year ~ '^[0-9]{4}$'
        """
        df = pd.read_sql(query, engine)
        process_vehicle_data(df)
        return df
    except Exception as e:
        st.error(f"Failed to load motorcycle data: {str(e)}")
        return pd.DataFrame()

def process_vehicle_data(df):
    """Common data processing for both vehicles"""
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['age'] = datetime.now().year - df['year']
    
    if 'mileage_min' in df.columns and 'mileage_max' in df.columns:
        df['mileage_min'] = pd.to_numeric(df['mileage_min'], errors='coerce')
        df['mileage_max'] = pd.to_numeric(df['mileage_max'], errors='coerce')
        df['mileage_avg'] = (df['mileage_min'] + df['mileage_max']) / 2