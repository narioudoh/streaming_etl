import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import os
import time
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Real-time ETL Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize DuckDB connection
@st.cache_resource
def get_db_connection():
    duckdb_path = os.getenv('DUCKDB_PATH', '/data/etl_data.duckdb')
    return duckdb.connect(duckdb_path)

def load_data():
    """Load data from DuckDB"""
    db = get_db_connection()
    
    try:
        # Load user events
        user_events = db.execute("""
            SELECT * FROM user_events 
            ORDER BY timestamp DESC 
            LIMIT 1000
        """).fetchdf()
        
        # Load metrics
        metrics = db.execute("""
            SELECT * FROM metrics 
            ORDER BY timestamp DESC 
            LIMIT 1000
        """).fetchdf()
        
        # Load processing stats
        processing_stats = db.execute("""
            SELECT * FROM processed_data 
            ORDER BY timestamp DESC 
            LIMIT 100
        """).fetchdf()
        
        return user_events, metrics, processing_stats
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def create_user_events_chart(user_events):
    """Create user events visualization"""
    if user_events.empty:
        return None
    
    # Convert timestamp to datetime
    user_events['timestamp'] = pd.to_datetime(user_events['timestamp'])
    
    # Group by event type and count
    event_counts = user_events.groupby('event_type').size().reset_index(name='count')
    
    # Create pie chart
    fig = px.pie(
        event_counts, 
        values='count', 
        names='event_type',
        title='User Events Distribution',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_layout(height=400)
    return fig

def create_metrics_timeline(metrics):
    """Create metrics timeline visualization"""
    if metrics.empty:
        return None
    
    # Convert timestamp to datetime
    metrics['timestamp'] = pd.to_datetime(metrics['timestamp'])
    
    # Create line chart for each metric
    fig = go.Figure()
    
    for metric_name in metrics['metric_name'].unique():
        metric_data = metrics[metrics['metric_name'] == metric_name]
        fig.add_trace(
            go.Scatter(
                x=metric_data['timestamp'],
                y=metric_data['metric_value'],
                mode='lines+markers',
                name=metric_name,
                line=dict(width=2)
            )
        )
    
    fig.update_layout(
        title='Metrics Timeline',
        xaxis_title='Time',
        yaxis_title='Value',
        height=400,
        hovermode='x unified'
    )
    
    return fig

def create_processing_stats(processing_stats):
    """Create processing statistics visualization"""
    if processing_stats.empty:
        return None
    
    # Convert timestamp to datetime
    processing_stats['timestamp'] = pd.to_datetime(processing_stats['timestamp'])
    
    # Group by source topic and count
    topic_counts = processing_stats.groupby('source_topic').size().reset_index(name='count')
    
    # Create bar chart
    fig = px.bar(
        topic_counts,
        x='source_topic',
        y='count',
        title='Records Processed by Topic',
        color='source_topic',
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    
    fig.update_layout(height=400)
    return fig

def create_real_time_metrics(metrics):
    """Create real-time metrics dashboard"""
    if metrics.empty:
        return None
    
    # Get latest values for each metric
    latest_metrics = metrics.groupby('metric_name').last().reset_index()
    
    # Create metric cards
    cols = st.columns(len(latest_metrics))
    
    for i, (_, metric) in enumerate(latest_metrics.iterrows()):
        with cols[i]:
            st.metric(
                label=metric['metric_name'].replace('_', ' ').title(),
                value=f"{metric['metric_value']:.2f}",
                delta=f"Updated {pd.Timestamp.now() - pd.to_datetime(metric['timestamp']).tz_localize(None):.0f}s ago"
            )

def main():
    st.title("🚀 Real-time ETL Pipeline Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)", 
        min_value=1, 
        max_value=60, 
        value=10
    )
    
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📈 Real-time Metrics")
        metrics_container = st.container()
        
    with col2:
        st.subheader("⚡ Processing Status")
        status_container = st.container()
    
    # Load data
    user_events, metrics, processing_stats = load_data()
    
    # Display real-time metrics
    with metrics_container:
        create_real_time_metrics(metrics)
    
    # Display processing status
    with status_container:
        if not processing_stats.empty:
            total_processed = processing_stats['record_count'].sum()
            st.metric("Total Records Processed", total_processed)
            
            latest_processing = processing_stats.iloc[0]
            st.metric(
                "Latest Processing", 
                f"{latest_processing['source_topic']}",
                f"{pd.Timestamp.now() - pd.to_datetime(latest_processing['timestamp']).tz_localize(None):.0f}s ago"
            )
        else:
            st.info("No processing data available yet")
    
    st.markdown("---")
    
    # Charts section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 User Events Analysis")
        user_events_chart = create_user_events_chart(user_events)
        if user_events_chart:
            st.plotly_chart(user_events_chart, use_container_width=True)
        else:
            st.info("No user events data available yet")
    
    with col2:
        st.subheader("📊 Processing Statistics")
        processing_chart = create_processing_stats(processing_stats)
        if processing_chart:
            st.plotly_chart(processing_chart, use_container_width=True)
        else:
            st.info("No processing statistics available yet")
    
    # Full-width metrics timeline
    st.subheader("⏰ Metrics Timeline")
    metrics_timeline = create_metrics_timeline(metrics)
    if metrics_timeline:
        st.plotly_chart(metrics_timeline, use_container_width=True)
    else:
        st.info("No metrics data available yet")
    
    # Data tables
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📋 Recent User Events")
        if not user_events.empty:
            st.dataframe(
                user_events[['user_id', 'event_type', 'timestamp']].head(10),
                use_container_width=True
            )
        else:
            st.info("No user events data available yet")
    
    with col2:
        st.subheader("📋 Recent Metrics")
        if not metrics.empty:
            st.dataframe(
                metrics[['metric_name', 'metric_value', 'timestamp']].head(10),
                use_container_width=True
            )
        else:
            st.info("No metrics data available yet")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "**Dashboard last updated:** " + 
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()



