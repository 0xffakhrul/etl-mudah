import streamlit as st
from datetime import datetime
from utils.data_loader import load_car_data, load_motorcycle_data
from pages.market_overview import render_market_overview
from pages.price_analysis import render_price_analysis
from pages.regional_analysis import render_regional_analysis
from pages.price_prediction import render_price_prediction

# Configure the page
st.set_page_config(
    page_title="Malaysian Vehicle Market Analysis",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    st.title("🚗 Malaysian Vehicle Market Analysis")
    st.markdown("Analyze trends and insights from the Malaysian used vehicle market")
    
    # Vehicle type selector
    vehicle_type = st.sidebar.radio(
        "Select Vehicle Type",
        options=["Cars", "Motorcycles"],
        format_func=lambda x: x.title()
    ).lower().rstrip('s')
    
    # Load appropriate data
    df = load_car_data() if vehicle_type == "car" else load_motorcycle_data()
    
    if not df.empty:
        # Sidebar filters
        st.sidebar.markdown(f"""
        # 🔍 Search Filters
        Use these options to find specific {vehicle_type}s you're interested in.
        """)
        
        # Price range filter
        st.sidebar.markdown("### 💰 Price Range")
        price_range = st.sidebar.slider(
            "Select Price Range (RM)",
            min_value=int(df['price'].min()),
            max_value=int(df['price'].max()),
            value=(int(df['price'].min()), int(df['price'].max())),
            format="%d"
        )
        
        # Make filter
        st.sidebar.markdown(f"### 🚗 {vehicle_type.title()} Brand")
        select_all_makes = st.sidebar.checkbox("Select All Brands")
        
        if select_all_makes:
            selected_makes = sorted(df['make'].unique())
        else:
            make_search = st.sidebar.text_input("Search for brands...").lower()
            filtered_makes = sorted([
                make for make in df['make'].unique() 
                if make_search in make.lower()
            ])
            selected_makes = st.sidebar.multiselect(
                "Choose Brands",
                options=filtered_makes
            )
        
        # Year range filter
        st.sidebar.markdown("### 📅 Manufacturing Year")
        year_range = st.sidebar.slider(
            "Select Year Range",
            min_value=int(df['year'].min()),
            max_value=int(df['year'].max()),
            value=(int(df['year'].min()), int(df['year'].max()))
        )
        
        # Apply filters
        filtered_df = df[
            (df['price'].between(price_range[0], price_range[1])) &
            (df['year'].between(year_range[0], year_range[1]))
        ]
        
        if selected_makes:
            filtered_df = filtered_df[filtered_df['make'].isin(selected_makes)]
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Listings", f"{len(filtered_df):,}")
        with col2:
            st.metric("Average Price", f"RM {filtered_df['price'].mean():,.2f}")
        with col3:
            st.metric("Median Price", f"RM {filtered_df['price'].median():,.2f}")
        with col4:
            st.metric("Average Age", f"{filtered_df['age'].mean():.1f} years")
        
        # Create tabs for different analyses
        tab1, tab2, tab3, tab4 = st.tabs([
            "Market Overview",
            "Price Analysis",
            "Regional Insights",
            "Price Prediction"
        ])
        
        with tab1:
            render_market_overview(filtered_df, vehicle_type)
        
        with tab2:
            render_price_analysis(filtered_df, vehicle_type)
        
        with tab3:
            render_regional_analysis(filtered_df, vehicle_type)
        
        with tab4:
            render_price_prediction(filtered_df, vehicle_type)
        
        # Data table
        # st.subheader("Detailed Listings")
        # cols_to_show = ['make', 'model', 'year', 'price', 'location']
        # if vehicle_type == "car":
        #     cols_to_show.extend(['mileage_avg', 'transmission', 'fuel_type'])
        
        # st.dataframe(
        #     filtered_df[cols_to_show].sort_values('price', ascending=False),
        #     use_container_width=True
        # )
    
    else:
        st.error("No data available to display")
    
    # Add refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "Data source: Mudah.my | Last updated: " + 
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

if __name__ == "__main__":
    main()