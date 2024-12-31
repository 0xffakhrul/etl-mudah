import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
import requests
from utils.constants import MALAYSIA_STATES
import pandas as pd

def load_geojson():
    """Load GeoJSON data from DOSM Malaysia GitHub repository"""
    url = "https://raw.githubusercontent.com/dosm-malaysia/data-open/main/datasets/geodata/administrative_1_state.geojson"
    response = requests.get(url)
    return response.json()

def standardize_location(location):
    """Standardize location names to match the geojson state names"""
    if location in MALAYSIA_STATES:
        return location
    
    location_mapping = {
        'Malacca': 'Melaka',
        'N. Sembilan': 'Negeri Sembilan',
        'Penang': 'Pulau Pinang',
        'P. Pinang': 'Pulau Pinang',
        'KL': 'W.P. Kuala Lumpur',
        'Kuala Lumpur': 'W.P. Kuala Lumpur',
        'Federal Territory of Kuala Lumpur': 'W.P. Kuala Lumpur',
        'Putrajaya': 'W.P. Putrajaya',
        'Labuan': 'W.P. Labuan'
    }
    
    return location_mapping.get(location, location)

def render_regional_analysis(df, vehicle_type="car"):
    st.subheader(f"Regional {vehicle_type.title()} Market Analysis")
    st.markdown(f"""
    Explore how the {vehicle_type} market differs across Malaysia's states and territories. 
    This analysis helps you understand:
    - Where most {vehicle_type}s are being sold
    - Price differences between regions
    - Which states have the most competitive prices
    """)
    
    # Standardize location names in the dataframe
    df['location'] = df['location'].apply(standardize_location)
    
    # Prepare data for the map
    location_stats = df.groupby('location').agg({
        'price': ['count', 'mean', 'median', 'std'],
        'year': 'mean'
    })
    
    location_stats.columns = ['listing_count', 'avg_price', 'median_price', 'price_std', 'avg_year']
    location_stats = location_stats.round(2)
    
    # Add map metric selector
    map_metric = st.radio(
        "Select map view",
        ["Number of Listings", "Average Price"],
        horizontal=True,
        key="map_metric"
    )
    
    # Create choropleth map
    geojson_data = load_geojson()
    
    if map_metric == "Number of Listings":
        color_column = 'listing_count'
        color_label = 'Number of Listings'
        hover_format = ':,d'
    else:
        color_column = 'avg_price'
        color_label = 'Average Price (RM)'
        hover_format = ':,.0f'
    
    fig = px.choropleth_mapbox(
        location_stats.reset_index(),
        geojson=geojson_data,
        locations='location',
        featureidkey="properties.state",
        color=color_column,
        color_continuous_scale="Viridis",
        mapbox_style="carto-positron",
        zoom=5.5,
        center={"lat": 4.2105, "lon": 108.9758},
        opacity=0.7,
        labels={color_column: color_label},
        hover_data={
            'location': True,
            'listing_count': ':,d',
            'avg_price': ':,.0f',
            'median_price': ':,.0f'
        },
        hover_name='location'
    )
    
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        height=500,
        coloraxis_colorbar_title_text=color_label
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Quick insights
    total_listings = location_stats['listing_count'].sum()
    top_region = location_stats.nlargest(1, 'listing_count').index[0]
    top_count = location_stats.loc[top_region, 'listing_count']
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Listings", f"{total_listings:,}")
    with col2:
        st.metric("Most Active Region", top_region)
    with col3:
        st.metric("Listings in Top Region", f"{top_count:,}")

    # Price Analysis Section
    st.markdown("### Price Comparison")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Bar chart of average prices by region
        avg_prices = df.groupby('location')['price'].mean().sort_values(ascending=True)
        
        fig = px.bar(
            x=avg_prices.index,
            y=avg_prices.values,
            title=f'Average {vehicle_type.title()} Prices by Region',
            labels={'x': 'Region', 'y': 'Average Price (RM)'},
            text=avg_prices.values.round(0)
        )
        
        fig.update_traces(
            texttemplate='RM %{text:,.0f}',
            textposition='outside'
        )
        
        fig.update_layout(
            height=400,
            xaxis_tickangle=45,
            xaxis_title="Region",
            yaxis_title="Average Price (RM)",
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Price summary
        price_summary = pd.DataFrame({
            'Region': location_stats.index,
            'Median Price': location_stats['median_price'].round(0),
            'Avg Price': location_stats['avg_price'].round(0),
            'Price Variation': (location_stats['price_std'] / location_stats['avg_price'] * 100).round(1)
        }).sort_values('Median Price', ascending=True)
        
        st.markdown("#### Price Summary")
        st.dataframe(
            price_summary.style.format({
                'Median Price': 'RM {:,.0f}',
                'Avg Price': 'RM {:,.0f}',
                'Price Variation': '{:.1f}%'
            }),
            hide_index=True,
            use_container_width=True
        )

    # Key insights section
    st.markdown("### 📌 Key Insights")
    
    # Calculate insights
    national_median = df['price'].median()
    cheapest_region = price_summary.iloc[0]['Region']
    most_expensive = price_summary.iloc[-1]['Region']
    price_diff = ((price_summary.iloc[-1]['Median Price'] - price_summary.iloc[0]['Median Price']) 
                 / price_summary.iloc[0]['Median Price'] * 100)
    
    st.markdown(f"""
    - **National Median Price**: RM {national_median:,.0f}
    - **Price Range**: {cheapest_region} is typically the most affordable region, while {most_expensive} is the most expensive
    - **Price Difference**: There's a {price_diff:.1f}% difference between the cheapest and most expensive regions
    - **Market Size**: {top_region} dominates with {(top_count/total_listings*100):.1f}% of all listings
    
    💡 **Shopping Tip**: Consider looking in neighboring states for better deals, especially if you're near state borders!
    """) 