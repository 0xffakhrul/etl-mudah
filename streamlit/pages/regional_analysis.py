import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import json
from utils.constants import MALAYSIA_STATES

def load_geojson():
    with open('malaysia_states.geojson') as f:
        return json.load(f)

def standardize_location(location):
    """Standardize location names to match the MALAYSIA_STATES dictionary"""
    if location in MALAYSIA_STATES:
        return location
    
    location_mapping = {
        'Malacca': 'Melaka',
        'N. Sembilan': 'Negeri Sembilan',
        'Penang': 'Pulau Pinang',
        'P. Pinang': 'Pulau Pinang',
        'KL': 'Kuala Lumpur',
        'W.P. Kuala Lumpur': 'Kuala Lumpur',
        'Federal Territory of Kuala Lumpur': 'Kuala Lumpur'
    }
    
    return location_mapping.get(location, location)

def render_regional_analysis(df, vehicle_type="car"):
    st.subheader(f"Regional {vehicle_type.title()} Market Analysis")
    st.markdown(f"""
    Understand how the {vehicle_type} market varies across different regions in Malaysia.
    This can help you identify regional price differences and market opportunities.
    """)
    
    # Prepare data for the map
    location_stats = df.groupby('location').agg({
        'price': ['count', 'mean', 'median'],
        'year': 'mean',
        'make': lambda x: x.mode().iloc[0] if not x.empty else None
    }).round(2)
    
    location_stats.columns = ['listing_count', 'avg_price', 'median_price', 'avg_year', 'popular_make']
    location_stats = location_stats.reset_index()
    
    map_tab1, map_tab2, map_tab3 = st.tabs([
        "Listing Count", 
        "Average Price",
        "Market Overview"
    ])
    
    with map_tab1:
        render_listing_count_map(location_stats, vehicle_type)
    
    with map_tab2:
        render_price_map(location_stats, vehicle_type)
    
    with map_tab3:
        render_market_overview(df, location_stats, vehicle_type)

def render_listing_count_map(location_stats, vehicle_type):
    st.markdown(f"""
    #### Regional Distribution of {vehicle_type.title()} Listings
    See how {vehicle_type} listings are distributed across different regions.
    """)
    
    # Create dataframe with coordinates
    map_data = location_stats.copy()
    map_data['location'] = map_data['location'].apply(standardize_location)
    
    try:
        map_data['lat'] = map_data['location'].map(lambda x: MALAYSIA_STATES[x]['lat'])
        map_data['lon'] = map_data['location'].map(lambda x: MALAYSIA_STATES[x]['lon'])
        
        fig = px.scatter_mapbox(
            map_data,
            lat='lat',
            lon='lon',
            size='listing_count',
            color='listing_count',
            hover_name='location',
            hover_data={
                'listing_count': True,
                'avg_price': ':,.2f',
                'avg_year': ':.1f',
                'popular_make': True,
                'lat': False,
                'lon': False
            },
            color_continuous_scale='Viridis',
            zoom=5,
            title=f'Number of {vehicle_type.title()} Listings by Region',
            size_max=50,
            mapbox_style='carto-positron'
        )
        
        fig.update_layout(height=600, margin={"r":0,"t":30,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)
        
        # Add insights
        total_listings = location_stats['listing_count'].sum()
        top_region = location_stats.nlargest(1, 'listing_count').iloc[0]
        top_percent = (top_region['listing_count'] / total_listings * 100)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - Total listings across Malaysia: {total_listings:,}
        - {top_region['location']} leads with {top_region['listing_count']:,} listings ({top_percent:.1f}% of total)
        - Popular make in {top_region['location']}: {top_region['popular_make']}
        - Average vehicle age in {top_region['location']}: {2024 - top_region['avg_year']:.1f} years
        """)
        
    except Exception as e:
        st.error(f"Error rendering map: {str(e)}")

def render_price_map(location_stats, vehicle_type):
    st.markdown(f"""
    #### Regional Price Distribution
    Understand how {vehicle_type} prices vary across different regions.
    """)
    
    try:
        geojson = load_geojson()
        
        fig = go.Figure(go.Choroplethmapbox(
            geojson=geojson,
            locations=location_stats['location'],
            z=location_stats['avg_price'],
            colorscale='Viridis',
            marker_opacity=0.7,
            marker_line_width=0,
            colorbar_title="Average Price (RM)",
            hovertemplate="<b>%{location}</b><br>" +
                         "Average Price: RM%{z:,.2f}<br>" +
                         "<extra></extra>"
        ))
        
        fig.update_layout(
            mapbox_style="carto-positron",
            mapbox=dict(
                center=dict(lat=4.2105, lon=108.9758),
                zoom=4.5
            ),
            height=600,
            margin={"r":0,"t":0,"l":0,"b":0}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add price comparison insights
        avg_national = location_stats['avg_price'].mean()
        most_expensive = location_stats.nlargest(1, 'avg_price').iloc[0]
        least_expensive = location_stats.nsmallest(1, 'avg_price').iloc[0]
        price_diff = ((most_expensive['avg_price'] - least_expensive['avg_price']) / least_expensive['avg_price'] * 100)
        
        st.markdown(f"""
        💡 **Price Insights**:
        - National average price: RM {avg_national:,.2f}
        - Most expensive region: {most_expensive['location']} (RM {most_expensive['avg_price']:,.2f})
        - Most affordable region: {least_expensive['location']} (RM {least_expensive['avg_price']:,.2f})
        - Price difference between highest and lowest: {price_diff:.1f}%
        """)
        
    except Exception as e:
        st.error(f"Error rendering choropleth map: {str(e)}")

def render_market_overview(df, location_stats, vehicle_type):
    st.markdown(f"""
    #### Detailed Regional Statistics
    Compare market characteristics across different regions.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Regional statistics
        st.subheader("Regional Statistics")
        display_stats = location_stats.copy()
        display_stats['avg_price'] = display_stats['avg_price'].apply(lambda x: f"RM {x:,.2f}")
        display_stats['median_price'] = display_stats['median_price'].apply(lambda x: f"RM {x:,.2f}")
        display_stats['avg_year'] = display_stats['avg_year'].apply(lambda x: f"{2024 - x:.1f} years")
        st.dataframe(display_stats, use_container_width=True)
    
    with col2:
        # Price distribution by region
        st.subheader("Price Distribution by Region")
        fig = px.box(
            df,
            x='location',
            y='price',
            title=f'{vehicle_type.title()} Price Distribution across Regions'
        )
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate regional market concentration
        total_listings = df['location'].count()
        top_3_regions = df['location'].value_counts().head(3)
        top_3_percent = (top_3_regions.sum() / total_listings * 100)
        
        st.markdown(f"""
        💡 **Market Concentration**:
        - Top 3 regions account for {top_3_percent:.1f}% of all listings
        - {top_3_regions.index[0]} has the highest market share with {(top_3_regions.iloc[0]/total_listings*100):.1f}%
        - This concentration suggests {'a centralized' if top_3_percent > 60 else 'a distributed'} market
        """) 