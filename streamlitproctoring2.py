import streamlit as st
import pandas as pd
from pymongo import MongoClient
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import os

# Setup MongoDB connection
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = 'scraped_data'
COLLECTION_NAME = 'proctoring'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tech_collection = db[COLLECTION_NAME]

def fetch_aggregated_data():
    pipeline = [
        {"$group": {
            "_id": {"Base_URL": "$Base_URL", "Technology": "$Technology"},
            "Count": {"$sum": 1}
        }},
        {"$sort": {"_id.Base_URL": 1, "_id.Technology": 1}}
    ]
    data = list(tech_collection.aggregate(pipeline))
    df = pd.DataFrame(data)
    if not df.empty:
        df['Base_URL'] = df['_id'].apply(lambda x: x.get('Base_URL', 'Unknown'))
        df['Technology'] = df['_id'].apply(lambda x: x.get('Technology', 'None'))
        df.drop(columns=['_id'], inplace=True)
        df = df[df['Technology'] != 'None']
        pivot_df = df.pivot_table(index='Base_URL', columns='Technology', values='Count', fill_value=0)
        # Reset the index to make Base_URL a column
        pivot_df = pivot_df.reset_index()
        # Replace counts with "Y", "M", and "N"
        def categorize_count(count):
            if count >= 1:
                return 'Y'
            else:
                return 'N'
        # Convert values to numeric, coerce errors to NaN, fill NaNs with 0, and exclude Base_URL from transformation
        transformed_df = pivot_df.drop(columns=['Base_URL']).applymap(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0).astype(int)
        transformed_df = transformed_df.applymap(categorize_count)
        transformed_df.insert(0, 'Base_URL', pivot_df['Base_URL'])
        return transformed_df
    else:
        return pd.DataFrame()

def get_urls(base_url, technology):
    urls_data = tech_collection.find({"Base_URL": base_url, "Technology": technology}, {"URL": 1})
    urls = [url['URL'] for url in urls_data]
    return urls

df = fetch_aggregated_data()
if not df.empty:
    # Setup grid options
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_side_bar()
    gb.configure_selection('single')
    # Configure initial sort
    gb.configure_default_column(sortable=True)
    # Assuming the second column is 'Technology', you can modify as needed
    gb.configure_column(df.columns[1], sort='desc')
    grid_options = gb.build()

    # Display the DataFrame using AgGrid with a specific height to make it scrollable
    grid_response = AgGrid(df, gridOptions=grid_options, update_mode=GridUpdateMode.MODEL_CHANGED, allow_unsafe_jscode=True, height=500)

    # Retrieve 'selected_rows'
    selected_rows = grid_response.get('selected_rows', [])
    selected = pd.DataFrame(selected_rows)

    # Check if the DataFrame is not empty
    if not selected.empty:
        selected_row = selected.iloc[0]  # Access the first row
        base_url = selected_row['Base_URL']
        selected_cols = selected.columns.tolist()
        # Identify the selected technology by checking for "Y" or "M" in the selected row
        for col in selected_cols:
            if col != 'Base_URL' and selected_row[col] in ['Y', 'M']:
                technology = col
                urls = get_urls(base_url, technology)
                st.write(f"URLs where '{technology}' is found under '{base_url}':")
                for url in urls:
                    st.write(url)
                break
        else:
            st.write("No technology selected or no URLs found.")
    else:
        st.write("No rows selected.")
else:
    st.write("No data available.")
