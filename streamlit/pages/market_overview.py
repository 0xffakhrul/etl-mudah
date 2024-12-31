import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.constants import create_price_segment, create_motorcycle_price_segment

def render_market_overview(df, vehicle_type="car"):
    st.markdown(f"""
    ### 📊 {vehicle_type.title()} Market Overview
    This section provides key insights about the Malaysian used {vehicle_type} market. Understanding these patterns
    can help buyers and sellers make informed decisions.
    """)
    
    # Add price segment based on vehicle type
    df['price_segment'] = df['price'].apply(
        create_price_segment if vehicle_type == "car" else create_motorcycle_price_segment
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        render_popular_makes(df, vehicle_type)
    
    with col2:
        render_price_segments(df, vehicle_type)
    
    # Vehicle-specific features section
    if vehicle_type == "car":
        render_car_features(df)
    else:
        render_motorcycle_features(df)

def render_popular_makes(df, vehicle_type):
    st.subheader(f"Most Popular {vehicle_type.title()} Brands")
    make_counts = df['make'].value_counts().head(10)
    
    fig = px.bar(
        x=make_counts.index,
        y=make_counts.values,
        title=f'Top 10 Most Listed {vehicle_type.title()} Brands',
        labels={'x': 'Brand', 'y': f'Number of {vehicle_type.title()}s Available'},
        color=make_counts.values,
        color_continuous_scale='Viridis'
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Dynamic insights based on available data
    insights = ["💡 **Key Insights**:"]
    
    if len(make_counts) > 0:
        insights.append(f"- {make_counts.index[0]} leads the market with {make_counts.values[0]:,} listings")
        
        if len(make_counts) > 1:
            insights.append(f"- {make_counts.index[1]} follows with {make_counts.values[1]:,} listings")
            
            if len(make_counts) > 2:
                insights.append(f"- {make_counts.index[2]} ranks third with {make_counts.values[2]:,} listings")
                # Calculate percentage for top 3 brands
                top_3_percentage = ((make_counts.values[:3].sum() / make_counts.values.sum()) * 100)
                insights.append(f"- These top 3 brands represent {top_3_percentage:.1f}% of all listings")
    else:
        insights.append("- No brands found with current filters")
    
    st.markdown("\n".join(insights))

def render_price_segments(df, vehicle_type):
    st.subheader(f"{vehicle_type.title()} Price Categories")
    segment_counts = df['price_segment'].value_counts()
    
    fig = px.pie(
        values=segment_counts.values,
        names=segment_counts.index,
        title=f'How {vehicle_type.title()}s are Distributed by Price Range',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Calculate percentages for insights
    total_vehicles = segment_counts.sum()
    budget_segment = 'Budget (< RM25k)' if vehicle_type == "car" else 'Budget (< RM5k)'
    premium_segments = ['Premium (RM100k-200k)', 'Luxury (> RM200k)'] if vehicle_type == "car" else ['Premium (RM20k-40k)', 'Luxury (> RM40k)']
    
    budget_percent = (segment_counts.get(budget_segment, 0) / total_vehicles * 100)
    premium_plus = sum(segment_counts.get(seg, 0) for seg in premium_segments) / total_vehicles * 100
    
    st.markdown(f"""
    💡 **Key Insights**:
    - {budget_percent:.1f}% of listings are budget-friendly {vehicle_type}s
    - {premium_plus:.1f}% are premium/luxury vehicles
    - The majority fall in the entry to mid-range categories
    - This distribution reflects the Malaysian market's focus on affordable vehicles
    """)

def render_car_features(df):
    st.markdown("### 🚗 Car Features Distribution")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Transmission Types")
        trans_counts = df['transmission'].value_counts()
        
        # Calculate percentages
        total_trans = trans_counts.sum()
        auto_percent = (trans_counts.get('Auto', 0) / total_trans * 100)
        manual_percent = (trans_counts.get('Manual', 0) / total_trans * 100)
        
        fig = px.pie(
            values=trans_counts.values,
            names=trans_counts.index,
            title='Manual vs Automatic Transmission',
            color_discrete_sequence=['#FF9999', '#66B2FF']
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - {auto_percent:.1f}% of used cars come with automatic transmission
        - Manual transmission makes up {manual_percent:.1f}% of the market
        - The high proportion of automatic vehicles reflects Malaysian drivers' preference for easier city driving
        - This trend aligns with increasing urban traffic conditions
        """)
    
    with col2:
        st.subheader("Fuel Types")
        df['fuel_type'] = df['fuel_type'].str.lower()
        fuel_counts = df['fuel_type'].value_counts()
        
        # Calculate percentages
        total_fuel = fuel_counts.sum()
        petrol_percent = (fuel_counts.get('petrol', 0) / total_fuel * 100)
        diesel_percent = (fuel_counts.get('diesel', 0) / total_fuel * 100)
        electric_percent = (fuel_counts.get('electric', 0) / total_fuel * 100)
        
        fig = px.pie(
            values=fuel_counts.values,
            names=fuel_counts.index.str.capitalize(),
            title='Distribution by Fuel Type',
            color_discrete_sequence=['#99FF99', '#FFCC99', '#FF99CC']
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - Petrol vehicles dominate the market ({petrol_percent:.1f}%)
        - Diesel vehicles represent {diesel_percent:.1f}% of listings
        - Electric vehicles make up {electric_percent:.1f}% of the market
        - The low EV percentage reflects the early stages of electric vehicle adoption in Malaysia
        """)

def render_motorcycle_features(df):
    st.markdown("### 🏍️ Motorcycle Market Characteristics")
    col1, col2 = st.columns(2)
    
    with col1:
        # Engine size distribution (if available)
        st.subheader("Popular Models")
        model_counts = df['model'].value_counts().head(10)
        
        fig = px.bar(
            x=model_counts.index,
            y=model_counts.values,
            title='Top 10 Most Popular Models',
            labels={'x': 'Model', 'y': 'Number of Listings'},
            color=model_counts.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(showlegend=False, coloraxis_showscale=False, xaxis_tickangle=45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate percentages for insights
        total_models = len(df)
        top_models_percent = (model_counts.sum() / total_models * 100)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - The top 10 models represent {top_models_percent:.1f}% of all listings
        - {model_counts.index[0]} is the most listed model with {model_counts.values[0]:,} listings
        - Popular models tend to have better resale value and parts availability
        """)
    
    with col2:
        # Age vs Price correlation
        st.subheader("Age vs Price Relationship")
        fig = px.scatter(
            df,
            x='age',
            y='price',
            title='Price vs Age Distribution',
            labels={'age': 'Age (Years)', 'price': 'Price (RM)'},
            opacity=0.6
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate correlation
        correlation = df['age'].corr(df['price'])
        avg_price = df['price'].mean()
        
        st.markdown(f"""
        💡 **Key Insights**:
        - Average motorcycle price: RM {avg_price:,.2f}
        - Age-Price correlation: {correlation:.2f}
        - Newer motorcycles generally command higher prices
        - Some vintage models may have premium pricing
        """) 