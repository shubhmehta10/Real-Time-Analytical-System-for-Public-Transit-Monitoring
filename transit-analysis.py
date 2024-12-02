# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# import plotly.express as px
# import plotly.graph_objects as go
# import folium
# from folium.plugins import HeatMap
# from pinotdb import connect


# def connect_to_pinot():
#     """Connect to Pinot database"""
#     conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')
#     return conn

# def get_transit_data(conn, hours=24):
#     """Fetch transit data for the last n hours"""
#     start_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
#     print(f"Calculated start_time (seconds): {start_time}")

#     query = f"""
#     SELECT *
#     FROM transit_data
#     WHERE event_time >= {start_time}
#     LIMIT 1000;
#     """
#     print(f"Executing query: {query}")
#     cursor = conn.cursor()
#     cursor.execute(query)
#     rows = cursor.fetchall()
#     print(f"Query returned {len(rows)} rows")
    
#     if rows:
#         columns = [desc[0] for desc in cursor.description]
#         return pd.DataFrame(rows, columns=columns)
#     else:
#         print("No data returned from query")
#         return pd.DataFrame()



# def clean_transit_data(df):
#     """Clean the transit data to handle NaNs and invalid values."""
#     # Drop rows with missing or invalid latitude/longitude
#     df = df.dropna(subset=['latitude', 'longitude'])
#     df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]

#     # Ensure delay is numeric and handle extreme negative delays
#     df['delay'] = pd.to_numeric(df['delay'], errors='coerce')
    
#     # Handle available_seats and estimated_capacity
#     df['availabe_seats'] = pd.to_numeric(df['availabe_seats'], errors='coerce')
#     df['estimated_capacity'] = pd.to_numeric(df['estimated_capacity'], errors='coerce')
#     df = df.dropna(subset=['availabe_seats', 'estimated_capacity'])
#     df = df[df['estimated_capacity'] > 0]
#     df = df[df['availabe_seats'] >= 0]
    
#     # Convert event_time and aimed_arrival_time to datetime
#     for col in ['event_time', 'aimed_arrival_time']:
#         df[col] = pd.to_datetime(df[col], errors='coerce')
#     df = df.dropna(subset=['event_time', 'aimed_arrival_time'])

#     # Drop rows with missing route_number or bus_number
#     df = df.dropna(subset=['route_number', 'bus_number'])

#     # Log dropped rows
#     initial_count = len(df)
#     df = df.reset_index(drop=True)
#     print(f"Data cleaned. Remaining rows: {len(df)} (Dropped {initial_count - len(df)})")

#     if df.empty:
#         raise ValueError("No valid data available after cleaning.")

#     return df


# def create_capacity_utilization_heatmap(df):
#     """Create a heatmap showing capacity utilization by hour and route"""
#     df['hour'] = pd.to_datetime(df['event_time']).dt.hour
#     df['utilization'] = ((df['estimated_capacity'] - df['availabe_seats']) / df['estimated_capacity'] * 100)

#     utilization = df.groupby(['route_number', 'hour'])['utilization'].mean()
#     utilization_matrix = utilization.reset_index().pivot(index='route_number', columns='hour', values='utilization')

#     fig = px.imshow(utilization_matrix,
#                     title='Bus Capacity Utilization by Hour and Route',
#                     labels=dict(x='Hour of Day', y='Route Number', color='Utilization %'),
#                     color_continuous_scale='RdYlBu_r')
#     return fig


# def create_delay_analysis(df):
#     """Create box plots showing delay distribution by route"""
#     df['delay_minutes'] = df['delay'] / 60000

#     fig = px.box(df, x='route_number', y='delay_minutes',
#                  title='Delay Distribution by Route',
#                  labels={'route_number': 'Route Number', 'delay_minutes': 'Delay (minutes)'})
#     return fig


# def create_passenger_load_timeline(df):
#     """Create a line chart showing passenger load over time"""
#     df['datetime'] = pd.to_datetime(df['event_time'])

#     hourly_load = df.set_index('datetime').resample('h').agg({
#         'availabe_seats': 'mean',
#         'estimated_capacity': 'mean'
#     })

#     hourly_load['passenger_count'] = hourly_load['estimated_capacity'] - hourly_load['availabe_seats']

#     fig = go.Figure()
#     fig.add_trace(go.Scatter(x=hourly_load.index, y=hourly_load['passenger_count'], name='Actual Load', mode='lines'))
#     fig.add_trace(go.Scatter(x=hourly_load.index, y=hourly_load['estimated_capacity'], name='Capacity', mode='lines', line=dict(dash='dash')))

#     fig.update_layout(title='Passenger Load vs Capacity Over Time',
#                       xaxis_title='Time',
#                       yaxis_title='Number of Passengers')
#     return fig


# def create_route_map(df):
#     """Create a map showing bus routes with heat map of delays"""
#     df['delay_minutes'] = df['delay'] / 60000

#     center_lat = df['latitude'].mean()
#     center_lon = df['longitude'].mean()

#     m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

#     heat_data = df[['latitude', 'longitude', 'delay_minutes']].values.tolist()
#     HeatMap(heat_data, radius=15, blur=10).add_to(m)

#     return m


# def analyze_transit_data():
#     """Main function to analyze transit data and create visualizations"""
#     try:
#         conn = connect_to_pinot()
#         print(conn)

#         df = get_transit_data(conn, hours=24)
#         print(df)

#         df = clean_transit_data(df)

#         utilization_heatmap = create_capacity_utilization_heatmap(df)
#         delay_analysis = create_delay_analysis(df)
#         passenger_timeline = create_passenger_load_timeline(df)
#         route_map = create_route_map(df)

#         metrics = {
#             'average_delay_minutes': (df['delay'] / 60000).mean(),
#             'max_delay_minutes': (df['delay'] / 60000).max(),
#             'average_utilization': ((df['estimated_capacity'] - df['availabe_seats']) / df['estimated_capacity'] * 100).mean(),
#             'total_buses': df['bus_number'].nunique()
#         }

#         return {
#             'visualizations': {
#                 'utilization_heatmap': utilization_heatmap,
#                 'delay_analysis': delay_analysis,
#                 'passenger_timeline': passenger_timeline,
#                 'route_map': route_map
#             },
#             'metrics': metrics
#         }
#     except Exception as e:
#         print(f"Error in analyze_transit_data: {str(e)}")
#         raise


# if __name__ == "__main__":
#     try:
#         results = analyze_transit_data()

#         for name, fig in results['visualizations'].items():
#             if hasattr(fig, 'show'):
#                 fig.show()
#             else:
#                 fig.save(f'transit_{name}.html')

#         print("\nKey Metrics:")
#         for metric, value in results['metrics'].items():
#             print(f"{metric.replace('_', ' ').title()}: {value:.2f}")

#     except Exception as e:
#         print(f"Error in main: {str(e)}")
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium.plugins import MarkerCluster
from pinotdb import connect
from datetime import datetime, timedelta

def connect_to_pinot():
    """Connect to Pinot database"""
    conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')
    return conn

def get_transit_data(conn, hours=24):
    """Fetch transit data for the last n hours"""
    start_time = int((datetime.now() - timedelta(hours=hours)).timestamp())
    print(f"Calculated start_time (seconds): {start_time}")

    query = f"""
    SELECT *
    FROM transit_data
    WHERE event_time >= {start_time}
    LIMIT 1000;
    """
    print(f"Executing query: {query}")
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"Query returned {len(rows)} rows")
    
    if rows:
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    else:
        print("No data returned from query")
        return pd.DataFrame()

def clean_transit_data(df):
    """Clean the transit data to handle NaNs and invalid values."""
    # Drop rows with missing or invalid latitude/longitude
    df = df.dropna(subset=['latitude', 'longitude'])
    df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]

    # Ensure delay is numeric and handle extreme negative delays
    df['delay'] = pd.to_numeric(df['delay'], errors='coerce')
    
    # Handle available_seats and estimated_capacity
    df['availabe_seats'] = pd.to_numeric(df['availabe_seats'], errors='coerce')
    df['estimated_capacity'] = pd.to_numeric(df['estimated_capacity'], errors='coerce')
    df = df.dropna(subset=['availabe_seats', 'estimated_capacity'])
    df = df[df['estimated_capacity'] > 0]
    df = df[df['availabe_seats'] >= 0]
    
    # Convert event_time and aimed_arrival_time to datetime
    for col in ['event_time', 'aimed_arrival_time']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df = df.dropna(subset=['event_time', 'aimed_arrival_time'])

    # Drop rows with missing route_number or bus_number
    df = df.dropna(subset=['route_number', 'bus_number'])

    # Log dropped rows
    initial_count = len(df)
    df = df.reset_index(drop=True)
    print(f"Data cleaned. Remaining rows: {len(df)} (Dropped {initial_count - len(df)})")

    if df.empty:
        raise ValueError("No valid data available after cleaning.")

    return df

def create_progress_rate_analysis(df):
    """Create a bar chart showing the distribution of progress rates"""
    progress_rate_counts = df['progress_rate'].value_counts()
    
    fig = px.bar(
        x=progress_rate_counts.index, 
        y=progress_rate_counts.values,
        title='Distribution of Bus Progress Rates',
        labels={'x': 'Progress Rate', 'y': 'Number of Buses'}
    )
    return fig

def create_geographic_bus_distribution(df):
    """Create an interactive map showing bus locations with route information"""
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=11)
    
    # Create a marker cluster group
    marker_cluster = MarkerCluster().add_to(m)
    
    # Color mapping for routes
    unique_routes = df['route_number'].unique()
    color_map = {route: f'#{np.random.randint(0, 16777215):06x}' for route in unique_routes}
    
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=5,
            popup=f"Route: {row['route_number']}, Bus: {row['bus_number']}, Delay: {row['delay']/60000:.2f} mins",
            color=color_map[row['route_number']],
            fill=True,
            fillColor=color_map[row['route_number']]
        ).add_to(marker_cluster)
    
    return m

def create_route_delay_boxplot(df):
    """Create a box plot showing delay distribution by route"""
    df['delay_minutes'] = df['delay'] / 60000
    
    fig = px.box(
        df, 
        x='route_number', 
        y='delay_minutes',
        title='Delay Distribution by Route',
        labels={'route_number': 'Route', 'delay_minutes': 'Delay (minutes)'}
    )
    return fig

def create_bus_occupancy_heatmap(df):
    """Create a heatmap showing bus occupancy by route and time"""
    df['hour'] = df['event_time'].dt.hour
    df['occupancy_rate'] = (df['estimated_capacity'] - df['availabe_seats']) / df['estimated_capacity'] * 100
    
    occupancy_pivot = df.pivot_table(
        values='occupancy_rate', 
        index='route_number', 
        columns='hour', 
        aggfunc='mean'
    )
    
    fig = px.imshow(
        occupancy_pivot, 
        title='Bus Occupancy Heatmap by Route and Hour',
        labels=dict(x='Hour of Day', y='Route', color='Occupancy Rate (%)'),
        color_continuous_scale='YlOrRd'
    )
    return fig

def create_bus_number_frequency(df):
    """Create a bar chart showing the frequency of bus numbers by route"""
    bus_route_counts = df.groupby(['route_number', 'bus_number']).size().reset_index(name='frequency')
    
    fig = px.bar(
        bus_route_counts, 
        x='route_number', 
        y='frequency', 
        color='bus_number',
        title='Bus Number Frequency by Route',
        labels={'route_number': 'Route', 'frequency': 'Number of Occurrences'}
    )
    return fig

def analyze_transit_data():
    """Main function to analyze transit data and create visualizations"""
    try:
        conn = connect_to_pinot()
        df = get_transit_data(conn, hours=24)
        df = clean_transit_data(df)

        visualizations = {
            'progress_rate_analysis': create_progress_rate_analysis(df),
            'route_delay_boxplot': create_route_delay_boxplot(df),
            'bus_occupancy_heatmap': create_bus_occupancy_heatmap(df),
            'bus_number_frequency': create_bus_number_frequency(df)
        }

        # Note: Geographic distribution map is saved separately due to its different format
        geographic_map = create_geographic_bus_distribution(df)
        geographic_map.save('bus_geographic_distribution.html')

        metrics = {
            'total_routes': df['route_number'].nunique(),
            'total_buses': df['bus_number'].nunique(),
            'average_delay_minutes': (df['delay'] / 60000).mean(),
            'max_delay_minutes': (df['delay'] / 60000).max(),
            'average_occupancy_rate': ((df['estimated_capacity'] - df['availabe_seats']) / df['estimated_capacity'] * 100).mean()
        }

        return {
            'visualizations': visualizations,
            'metrics': metrics
        }
    except Exception as e:
        print(f"Error in analyze_transit_data: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        results = analyze_transit_data()

        for name, fig in results['visualizations'].items():
            fig.show()
            fig.write_html(f'transit_{name}.html')

        print("\nKey Metrics:")
        for metric, value in results['metrics'].items():
            print(f"{metric.replace('_', ' ').title()}: {value:.2f}")

    except Exception as e:
        print(f"Error in main: {str(e)}")
