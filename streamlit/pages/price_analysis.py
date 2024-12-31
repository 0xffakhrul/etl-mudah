import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import plotly.figure_factory as ff
import numpy as np

def render_price_analysis(df, vehicle_type="car"):
    st.markdown(f"""
    ### 💰 {vehicle_type.title()} Price Analysis
    Understand how {vehicle_type} prices vary based on different factors like age, mileage, and brand.
    This can help you determine fair market values and identify good deals.
    """)
    
    price_tab1, price_tab2, price_tab3 = st.tabs([
        "📅 Age Impact", 
        "🛣️ Mileage Impact" if vehicle_type == "car" else "📊 Price Distribution",
        "🏢 Brand Analysis"
    ])
    
    with price_tab1:
        render_age_analysis(df, vehicle_type)
    
    with price_tab2:
        if vehicle_type == "car":
            render_mileage_analysis(df)
        else:
            render_price_distribution(df)
    
    with price_tab3:
        render_brand_analysis(df, vehicle_type)

def render_age_analysis(df, vehicle_type):
    st.markdown(f"""
    #### How Age Affects {vehicle_type.title()} Price
    See how prices typically decrease as vehicles get older, and understand the depreciation pattern.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Price vs Age trend with improved readability
        age_df = df.groupby('age')['price'].agg([
            ('mean', 'mean'),
            ('median', 'median'),
            ('count', 'count')
        ]).reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=age_df['age'],
            y=age_df['mean'],
            name='Average Price',
            line=dict(color='#2E86C1', width=3),
            mode='lines+markers'
        ))
        
        fig.add_trace(go.Scatter(
            x=age_df['age'],
            y=age_df['median'],
            name='Typical Price',
            line=dict(color='#E74C3C', width=3, dash='dash'),
            mode='lines+markers'
        ))
        
        fig.update_layout(
            title=f'How {vehicle_type.title()} Prices Change with Age',
            xaxis_title='Age (Years)',
            yaxis_title='Price (RM)',
            hovermode='x unified'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate depreciation rates
        new_price = age_df.loc[age_df['age'] == age_df['age'].min(), 'mean'].iloc[0]
        five_year_price = age_df.loc[age_df['age'] == 5, 'mean'].iloc[0] if 5 in age_df['age'].values else None
        ten_year_price = age_df.loc[age_df['age'] == 10, 'mean'].iloc[0] if 10 in age_df['age'].values else None
        old_price = age_df.loc[age_df['age'] == age_df['age'].max(), 'mean'].iloc[0]
        
        # Calculate depreciation rates
        five_year_drop = ((new_price - five_year_price) / new_price * 100) if five_year_price else None
        ten_year_drop = ((new_price - ten_year_price) / new_price * 100) if ten_year_price else None
        total_drop = ((new_price - old_price) / new_price * 100)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - {vehicle_type.title()}s lose approximately {five_year_drop:.1f}% of their value in the first 5 years
        - After 10 years, the depreciation reaches {ten_year_drop:.1f}%
        - The total depreciation over {int(age_df['age'].max())} years is {total_drop:.1f}%
        - The steepest price drop occurs in the first 3-5 years of ownership
        """)
    
    with col2:
        # Distribution of vehicles by age
        fig = px.histogram(
            df,
            x='age',
            nbins=30,
            title=f'Distribution of {vehicle_type.title()}s by Age',
            labels={'age': 'Age (Years)', 'count': 'Number of Vehicles'}
        )
        
        # Add average line
        avg_age = df['age'].mean()
        fig.add_vline(
            x=avg_age,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Average Age: {avg_age:.1f} years"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate age-related statistics
        below_avg_age = (df['age'] <= avg_age).mean() * 100
        newer_vehicles = (df['age'] <= 5).mean() * 100
        older_vehicles = (df['age'] >= 10).mean() * 100
        
        st.markdown(f"""
        💡 **Market Composition**:
        - Average vehicle age is {avg_age:.1f} years
        - {below_avg_age:.1f}% of vehicles are below average age
        - {newer_vehicles:.1f}% are relatively new (≤ 5 years old)
        - {older_vehicles:.1f}% are older vehicles (≥ 10 years old)
        """)

def render_mileage_analysis(df):
    st.markdown("""
    #### How Mileage Affects Car Price
    See how car prices typically vary based on the distance traveled.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Calculate average mileage
        avg_mileage = df['mileage_avg'].mean()
        
        # Create mileage bins (in 20,000 km intervals)
        max_mileage = 200000  # Cap at 200,000 km for better visualization
        bin_size = 20000
        bins = list(range(0, max_mileage + bin_size, bin_size))
        
        df['mileage_bin'] = pd.cut(
            df['mileage_avg'].clip(upper=max_mileage),
            bins=bins,
            labels=[f"{i:,}-{i+bin_size:,}" for i in bins[:-1]]
        )
        
        # Calculate average prices per mileage bin
        mileage_price = df.groupby('mileage_bin')['price'].agg([
            ('mean', 'mean'),
            ('median', 'median'),
            ('count', 'count')
        ]).reset_index()
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=mileage_price['mileage_bin'],
            y=mileage_price['mean'],
            name='Average Price',
            line=dict(color='#2E86C1', width=3),
            mode='lines+markers'
        ))
        
        fig.add_trace(go.Scatter(
            x=mileage_price['mileage_bin'],
            y=mileage_price['median'],
            name='Typical Price',
            line=dict(color='#E74C3C', width=3, dash='dash'),
            mode='lines+markers'
        ))
        
        fig.update_layout(
            title='How Car Prices Change with Mileage',
            xaxis_title='Mileage Range (km)',
            yaxis_title='Price (RM)',
            hovermode='x unified',
            xaxis_tickangle=45
        )
        st.plotly_chart(fig, use_container_width=True)

        # Calculate price drops at different mileage points
        low_mileage_price = mileage_price.iloc[0]['mean']  # Price at lowest mileage
        mid_mileage_price = mileage_price.iloc[len(mileage_price)//2]['mean']  # Price at middle mileage
        high_mileage_price = mileage_price.iloc[-1]['mean']  # Price at highest mileage
        
        # Calculate percentage drops
        mid_mileage_drop = ((low_mileage_price - mid_mileage_price) / low_mileage_price * 100)
        total_drop = ((low_mileage_price - high_mileage_price) / low_mileage_price * 100)
        
        st.markdown(f"""
        💡 **Key Insights**:
        - Cars lose approximately {mid_mileage_drop:.1f}% of their value at {bin_size*5:,} km
        - The total price drop from {bin_size:,} km to {max_mileage:,} km is {total_drop:.1f}%
        - Price depreciation is steepest in the first {bin_size*3:,} km
        - After {bin_size*8:,} km, the price decrease tends to slow down
        """)
    
    with col2:
        # Distribution of vehicles by mileage
        fig = px.histogram(
            df[df['mileage_avg'] <= max_mileage],
            x='mileage_avg',
            nbins=30,
            title='Distribution of Vehicles by Mileage',
            labels={'mileage_avg': 'Mileage (km)', 'count': 'Number of Vehicles'}
        )
        
        # Add average line
        fig.add_vline(
            x=avg_mileage,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Average: {avg_mileage:,.0f} km"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate mileage-related statistics
        below_avg = (df['mileage_avg'] <= avg_mileage).mean() * 100
        low_mileage = (df['mileage_avg'] <= 50000).mean() * 100
        high_mileage = (df['mileage_avg'] >= 150000).mean() * 100
        
        st.markdown(f"""
        💡 **Market Composition**:
        - Average mileage is {avg_mileage:,.0f} km
        - {below_avg:.1f}% of vehicles have below-average mileage
        - {low_mileage:.1f}% have low mileage (≤ 50,000 km)
        - {high_mileage:.1f}% have high mileage (≥ 150,000 km)
        """)

def render_price_distribution(df):
    st.markdown("""
    #### Price Distribution Analysis
    Understand how motorcycle prices are distributed across different ranges.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Create price distribution histogram
        fig = ff.create_distplot(
            [df['price'].values],
            ['Price Distribution'],
            bin_size=2000,
            show_rug=False
        )
        fig.update_layout(
            title='Price Distribution Curve',
            xaxis_title='Price (RM)',
            yaxis_title='Density'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Price range analysis
        price_ranges = [
            (0, 5000, 'Budget (< RM5k)'),
            (5000, 10000, 'Entry Level (RM5k-10k)'),
            (10000, 20000, 'Mid Range (RM10k-20k)'),
            (20000, 40000, 'Premium (RM20k-40k)'),
            (40000, float('inf'), 'Luxury (> RM40k)')
        ]
        
        range_counts = []
        for min_price, max_price, label in price_ranges:
            count = len(df[(df['price'] >= min_price) & (df['price'] < max_price)])
            range_counts.append({'range': label, 'count': count})
        
        range_df = pd.DataFrame(range_counts)
        
        fig = px.pie(
            range_df,
            values='count',
            names='range',
            title='Distribution by Price Range'
        )
        st.plotly_chart(fig, use_container_width=True)

def render_brand_analysis(df, vehicle_type):
    # Make selection for detailed analysis
    selected_make = st.selectbox(
        f"Select {vehicle_type.title()} Brand", 
        options=sorted(df['make'].unique())
    )
    
    if selected_make:
        make_df = df[df['make'] == selected_make]
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Price distribution by model
            fig = px.box(
                make_df,
                x='model',
                y='price',
                title=f'Price Distribution for {selected_make} Models',
                labels={'price': 'Price (RM)', 'model': 'Model'}
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Average price by model
            model_stats = make_df.groupby('model').agg({
                'price': ['count', 'mean', 'median']
            }).round(2)
            model_stats.columns = ['Count', 'Mean Price', 'Median Price']
            model_stats = model_stats.reset_index()
            
            fig = px.bar(
                model_stats,
                x='model',
                y='Mean Price',
                title=f'Average Price by Model for {selected_make}',
                labels={'Mean Price': 'Average Price (RM)', 'model': 'Model'}
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Detailed statistics table
            st.subheader(f"Detailed Statistics for {selected_make}")
            model_stats['Mean Price'] = model_stats['Mean Price'].apply(lambda x: f"RM {x:,.2f}")
            model_stats['Median Price'] = model_stats['Median Price'].apply(lambda x: f"RM {x:,.2f}")
            st.dataframe(model_stats, use_container_width=True) 