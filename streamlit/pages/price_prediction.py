import streamlit as st
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import plotly.express as px
import numpy as np

def train_price_prediction_model(df, vehicle_type="car"):
    """Train the price prediction model and return model and encoders"""
    try:
        # Prepare the data
        model_df = df.copy()
        
        # Select features based on vehicle type
        if vehicle_type == "car":
            features = ['make', 'model', 'year', 'mileage_avg', 'transmission', 'fuel_type']
        else:
            features = ['make', 'model', 'year']  # Motorcycles have fewer features
        
        # Verify all required features exist
        missing_features = [f for f in features if f not in model_df.columns]
        if missing_features:
            st.warning(f"Missing features for {vehicle_type}: {missing_features}")
            return None, None, None, None, None
        
        # Remove rows with missing values
        model_df = model_df.dropna(subset=features + ['price'])
        
        if len(model_df) < 100:  # Minimum sample size for training
            st.warning(f"Insufficient data for {vehicle_type} price prediction (n={len(model_df)})")
            return None, None, None, None, None
        
        # Encode categorical variables
        le_dict = {}
        for col in features:
            if model_df[col].dtype == 'object':
                le_dict[col] = LabelEncoder()
                model_df[col] = le_dict[col].fit_transform(model_df[col].astype(str))
        
        # Prepare X and y
        X = model_df[features]
        y = model_df['price']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model with error handling
        try:
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            # Calculate accuracy metrics
            train_score = model.score(X_train, y_train)
            test_score = model.score(X_test, y_test)
            
            return model, le_dict, train_score, test_score, features
            
        except Exception as e:
            st.error(f"Error in model training: {str(e)}")
            return None, None, None, None, None
    
    except Exception as e:
        st.error(f"Error in data preparation: {str(e)}")
        return None, None, None, None, None

def predict_price(prediction_model, encoders, model_features, **inputs):
    """Make price prediction based on input parameters"""
    try:
        # Prepare input data
        input_data = {}
        for feature in model_features:
            if feature in encoders:
                # Handle potential KeyError in LabelEncoder
                try:
                    input_data[feature] = encoders[feature].transform([str(inputs[feature])])[0]
                except ValueError as e:
                    st.warning(f"Unknown category in {feature}: {inputs[feature]}")
                    return None
            else:
                input_data[feature] = inputs[feature]
        
        # Create DataFrame with correct feature order
        input_df = pd.DataFrame([input_data])
        
        # Make prediction
        predicted_price = prediction_model.predict(input_df)[0]
        return predicted_price
    
    except Exception as e:
        st.error(f"Error in prediction: {str(e)}")
        return None

def render_price_prediction(df, vehicle_type="car"):
    st.subheader(f" Used {vehicle_type.title()} Price Prediction")
    st.write(f"Get an estimated price for a used {vehicle_type} based on its characteristics")
    
    # Train the model
    model, le_dict, train_score, test_score, features = train_price_prediction_model(df, vehicle_type)
    
    if model and le_dict and features:
        # Model accuracy metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Training Accuracy", f"{train_score:.2%}")
        with col2:
            st.metric("Testing Accuracy", f"{test_score:.2%}")
        
        # Input form
        st.subheader(f"Enter {vehicle_type.title()} Details")
        col1, col2 = st.columns(2)
        
        inputs = {}
        with col1:
            inputs['make'] = st.selectbox("Make", options=sorted(df['make'].unique()))
            make_models = sorted(df[df['make'] == inputs['make']]['model'].unique())
            inputs['model'] = st.selectbox("Model", options=make_models)
            inputs['year'] = st.number_input(
                "Year",
                min_value=int(df['year'].min()),
                max_value=int(df['year'].max()),
                value=2018
            )
        
        with col2:
            if vehicle_type == "car":
                if 'mileage_avg' in features:
                    inputs['mileage_avg'] = st.number_input(
                        "Mileage (km)",
                        min_value=0,
                        max_value=500000,
                        value=50000,
                        step=1000
                    )
                if 'transmission' in features:
                    inputs['transmission'] = st.selectbox(
                        "Transmission",
                        options=sorted(df['transmission'].unique())
                    )
                if 'fuel_type' in features:
                    inputs['fuel_type'] = st.selectbox(
                        "Fuel Type",
                        options=sorted(df['fuel_type'].unique())
                    )
        
        if st.button("Predict Price", type="primary"):
            predicted_price = predict_price(
                prediction_model=model,
                encoders=le_dict,
                model_features=features,
                **inputs
            )
            
            if predicted_price is not None:
                st.success(f"### Estimated Price: RM {predicted_price:,.2f}")
                
                # Show price range
                confidence_range = 0.15  # 15% range
                lower_bound = predicted_price * (1 - confidence_range)
                upper_bound = predicted_price * (1 + confidence_range)
                st.info(f"Suggested Price Range: RM {lower_bound:,.2f} - RM {upper_bound:,.2f}")
                
                # Show similar listings
                st.subheader("Similar Listings in the Market")
                similar_filter = (
                    (df['make'] == inputs['make']) &
                    (df['model'] == inputs['model']) &
                    (df['year'].between(inputs['year'] - 2, inputs['year'] + 2))
                )
                
                if vehicle_type == "car" and 'mileage_avg' in features:
                    similar_filter &= df['mileage_avg'].between(
                        inputs['mileage_avg'] - 20000, 
                        inputs['mileage_avg'] + 20000
                    )
                
                similar_vehicles = df[similar_filter]
                
                if not similar_vehicles.empty:
                    # Basic display columns
                    display_cols = ['make', 'model', 'year', 'price', 'ad_url'] + \
                                 (['mileage_avg', 'transmission', 'fuel_type'] if vehicle_type == "car" else [])
                    
                    # Create a DataFrame for display
                    display_df = similar_vehicles[display_cols].head(5).copy()
                    
                    # Convert ad_url to clickable links
                    display_df['ad_url'] = display_df['ad_url'].apply(
                        lambda x: f'<a href="{x}" target="_blank">View Listing</a>' if pd.notna(x) else "N/A"
                    )
                    
                    st.write(
                        display_df.to_html(
                            escape=False,
                            index=False
                        ),
                        unsafe_allow_html=True
                    )
                else:
                    st.info("No similar listings found in the database")
                
                # Feature importance plot
                st.subheader("Feature Importance Analysis")
                importance_df = pd.DataFrame({
                    'Feature': features,
                    'Importance': model.feature_importances_
                }).sort_values('Importance', ascending=False)
                
                fig = px.bar(
                    importance_df,
                    x='Feature',
                    y='Importance',
                    title='Factors Affecting Price'
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("Failed to initialize the prediction model. Please check the data and try again.")