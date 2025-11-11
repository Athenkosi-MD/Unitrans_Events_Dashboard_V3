import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

# ------------------------------
# 1. Database connection
# ------------------------------
engine = create_engine('postgresql://postgres:Nicolaas24@localhost:5432/Unitrans_Dashboard')

# ------------------------------
# 2. Load driver events from database with caching
# ------------------------------
@st.cache_data(ttl=600)
def load_driver_events():
    """
    Load driver event data from the database. This table contains all event types,
    their timestamp, and associated driver (LinkedName_1).
    """
    df = pd.read_sql_table(
        table_name='drivers',
        con=engine,
        columns=['Event Types', 'EventDate', 'LinkedName_1']
    )

    # Ensure proper types
    df['EventDate'] = pd.to_datetime(df['EventDate'], errors='coerce')
    df = df.dropna(subset=['EventDate', 'Event Types', 'LinkedName_1'])
    df['LinkedName_1'] = df['LinkedName_1'].astype(str)
    df['Event Types'] = df['Event Types'].astype(str)

    return df

# Extract driver events
events_df = load_driver_events()

# ------------------------------
# 3. Load trips in batches from database
# ------------------------------
BATCH_SIZE = 100
offset = st.session_state.get('offset', 0)

@st.cache_data(ttl=300)
def load_trips_batch(offset: int, batch_size: int):
    """
    Load a batch of trips from the trips_data table.
    """
    query = f"""
    SELECT id, asset, driver, trip_type, start, "end", distance, max_speed, idle_time,
           start_coords, end_coords, start_odometer, end_odometer
    FROM trips_data
    ORDER BY id
    OFFSET {offset} LIMIT {batch_size};
    """
    df = pd.read_sql(query, engine)

    # Convert dates
    df['start'] = pd.to_datetime(df['start'], errors='coerce')
    df['end'] = pd.to_datetime(df['end'], errors='coerce')
    df = df.dropna(subset=['start', 'end'])

    # Ensure driver column is string
    df['driver'] = df['driver'].astype(str)
    return df

# Load batch of trips
trips_df = load_trips_batch(offset, BATCH_SIZE)

# ------------------------------
# 4. Create dynamic Event Types columns
# ------------------------------
event_types = events_df['Event Types'].unique()
for et in event_types:
    trips_df[et] = 0

# ------------------------------
# 5. Aggregate events per trip
# ------------------------------
for idx, trip in trips_df.iterrows():
    try:
        start = pd.Timestamp(trip['start'])
        end = pd.Timestamp(trip['end'])
    except:
        continue

    driver = str(trip['driver'])
    if driver.lower() == 'nan' or driver == '':
        continue

    # Filter events for this driver and trip time
    driver_events = events_df[
        (events_df['LinkedName_1'] == driver) &
        (events_df['EventDate'] >= start) &
        (events_df['EventDate'] <= end)
    ]

    counts = driver_events['Event Types'].value_counts()
    for et, count in counts.items():
        trips_df.at[idx, et] = count

# ------------------------------
# 6. Display in Streamlit using AgGrid with pagination
# ------------------------------
st.title("Trip Events Dashboard (Batch Loading)")
st.dataframe(trips_df, use_container_width=)
gb = GridOptionsBuilder.from_dataframe(trips_df)
gb.configure_pagination(enabled=True, paginationPageSize=20)
gb.configure_side_bar()
gridOptions = gb.build()

AgGrid(
    trips_df,
    gridOptions=gridOptions,
    enable_enterprise_modules=True,
    height=500,
    fit_columns_on_grid_load=True
)

# ------------------------------
# 7. Pagination buttons
# ------------------------------
cols = st.columns([1, 1, 1])
with cols[0]:
    if st.button("Previous"):
        st.session_state['offset'] = max(0, offset - BATCH_SIZE)
        st.experimental_rerun()
with cols[2]:
    if st.button("Next"):
        st.session_state['offset'] = offset + BATCH_SIZE
        st.experimental_rerun()
