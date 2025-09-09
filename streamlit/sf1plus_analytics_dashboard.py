"""
SF1+ Analytics Dashboard
Streamlit app for visualizing SF1+ streaming platform analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime, timedelta
import os

# Page config
st.set_page_config(
    page_title="SF1+ Analytics Dashboard",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ff6b35;
    }
    .main-header {
        color: #1f77b4;
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def get_snowflake_connection():
    """Create Snowflake connection (configure with your credentials)"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER', 'your_user'),
        password=os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
        account=os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'your_warehouse'),
        database='SF1PLUS_DB',
        schema='GOLD'
    )

@st.cache_data(ttl=300)  # Cache for 5 minutes
def run_query(query):
    """Execute query and return DataFrame"""
    try:
        conn = get_snowflake_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return pd.DataFrame()

def main():
    # Header
    st.markdown('<h1 class="main-header">📺 SF1+ Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("Real-time insights into streaming platform performance and customer behavior")
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    
    # Date range selector
    date_range = st.sidebar.selectbox(
        "Time Period",
        ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1
    )
    
    # Convert to SQL filter
    date_filter = {
        "Last 7 days": "AND summary_date >= CURRENT_DATE() - 7",
        "Last 30 days": "AND summary_date >= CURRENT_DATE() - 30", 
        "Last 90 days": "AND summary_date >= CURRENT_DATE() - 90",
        "All time": ""
    }[date_range]
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Main dashboard
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "👥 Customers", "📺 Content", "📱 Devices"])
    
    with tab1:
        st.header("Platform Overview")
        
        # Key metrics
        metrics_query = f"""
        SELECT 
            SUM(total_events) as total_events,
            SUM(unique_viewers) as total_viewers,
            SUM(total_watch_hours) as total_watch_hours,
            AVG(avg_bitrate) as avg_bitrate,
            AVG(identified_viewer_rate) as id_rate
        FROM DAILY_SUMMARY 
        WHERE 1=1 {date_filter}
        """
        
        metrics_df = run_query(metrics_query)
        
        if not metrics_df.empty:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "Total Events",
                    f"{metrics_df['TOTAL_EVENTS'].iloc[0]:,.0f}",
                    help="Total streaming events"
                )
            
            with col2:
                st.metric(
                    "Unique Viewers", 
                    f"{metrics_df['TOTAL_VIEWERS'].iloc[0]:,.0f}",
                    help="Distinct customers who watched content"
                )
            
            with col3:
                st.metric(
                    "Watch Hours",
                    f"{metrics_df['TOTAL_WATCH_HOURS'].iloc[0]:,.0f}",
                    help="Total hours of content consumed"
                )
            
            with col4:
                st.metric(
                    "Avg Bitrate",
                    f"{metrics_df['AVG_BITRATE'].iloc[0]:,.0f} kbps",
                    help="Average streaming quality"
                )
            
            with col5:
                st.metric(
                    "ID Rate",
                    f"{metrics_df['ID_RATE'].iloc[0]:.1%}",
                    help="% of events with customer ID"
                )
        
        # Daily trends
        daily_query = f"""
        SELECT 
            summary_date,
            unique_viewers,
            total_watch_hours,
            total_ad_minutes,
            avg_bitrate
        FROM DAILY_SUMMARY 
        WHERE 1=1 {date_filter}
        ORDER BY summary_date
        """
        
        daily_df = run_query(daily_query)
        
        if not daily_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_viewers = px.line(
                    daily_df, 
                    x='SUMMARY_DATE', 
                    y='UNIQUE_VIEWERS',
                    title='Daily Unique Viewers',
                    labels={'UNIQUE_VIEWERS': 'Unique Viewers', 'SUMMARY_DATE': 'Date'}
                )
                fig_viewers.update_layout(height=400)
                st.plotly_chart(fig_viewers, use_container_width=True)
            
            with col2:
                fig_hours = px.line(
                    daily_df,
                    x='SUMMARY_DATE',
                    y='TOTAL_WATCH_HOURS', 
                    title='Daily Watch Hours',
                    labels={'TOTAL_WATCH_HOURS': 'Watch Hours', 'SUMMARY_DATE': 'Date'}
                )
                fig_hours.update_layout(height=400)
                st.plotly_chart(fig_hours, use_container_width=True)
        
        # Device distribution
        device_query = """
        SELECT 
            device_type,
            SUM(total_events) as events,
            COUNT(DISTINCT unique_users) as users,
            AVG(avg_bitrate) as bitrate
        FROM DEVICE_ANALYTICS 
        GROUP BY device_type
        ORDER BY events DESC
        """
        
        device_df = run_query(device_query)
        
        if not device_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_pie = px.pie(
                    device_df,
                    values='EVENTS',
                    names='DEVICE_TYPE',
                    title='Events by Device Type'
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                fig_bar = px.bar(
                    device_df,
                    x='DEVICE_TYPE',
                    y='BITRATE',
                    title='Average Bitrate by Device',
                    labels={'BITRATE': 'Avg Bitrate (kbps)', 'DEVICE_TYPE': 'Device Type'}
                )
                st.plotly_chart(fig_bar, use_container_width=True)
    
    with tab2:
        st.header("Customer Analytics")
        
        # Customer segments
        segment_query = """
        SELECT 
            viewer_segment,
            COUNT(*) as customers,
            AVG(engagement_score) as avg_engagement,
            AVG(total_watch_hours) as avg_watch_hours,
            AVG(estimated_clv) as avg_clv
        FROM CUSTOMER_360 
        WHERE customer_id IS NOT NULL
        GROUP BY viewer_segment
        ORDER BY avg_engagement DESC
        """
        
        segment_df = run_query(segment_query)
        
        if not segment_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_seg = px.bar(
                    segment_df,
                    x='VIEWER_SEGMENT',
                    y='CUSTOMERS',
                    title='Customers by Segment',
                    labels={'CUSTOMERS': 'Number of Customers', 'VIEWER_SEGMENT': 'Viewer Segment'}
                )
                st.plotly_chart(fig_seg, use_container_width=True)
            
            with col2:
                fig_clv = px.bar(
                    segment_df,
                    x='VIEWER_SEGMENT', 
                    y='AVG_CLV',
                    title='Average CLV by Segment',
                    labels={'AVG_CLV': 'Avg Customer Lifetime Value', 'VIEWER_SEGMENT': 'Viewer Segment'}
                )
                st.plotly_chart(fig_clv, use_container_width=True)
        
        # Top customers table
        st.subheader("Top Customers by Engagement")
        top_customers_df = run_query("SELECT * FROM TOP_CUSTOMERS LIMIT 20")
        
        if not top_customers_df.empty:
            st.dataframe(
                top_customers_df[['CUSTOMER_ID', 'FIRST_NAME', 'LAST_NAME', 'SUBSCRIPTION_LEVEL', 
                                'ENGAGEMENT_SCORE', 'TOTAL_WATCH_HOURS', 'PREFERRED_DEVICE', 'REGION']],
                use_container_width=True
            )
    
    with tab3:
        st.header("Content Performance")
        
        # Programme performance
        st.subheader("Top Programmes (Last 7 Days)")
        prog_df = run_query("SELECT * FROM TOP_PROGRAMMES LIMIT 20")
        
        if not prog_df.empty:
            st.dataframe(
                prog_df[['PROGRAMME_ID', 'PROGRAMME_DATE', 'PROGRAMME_HOUR', 
                        'UNIQUE_VIEWERS', 'TOTAL_WATCH_HOURS', 'COMPLETION_RATE', 'TOP_REGION']],
                use_container_width=True
            )
        
        # Peak hours analysis
        st.subheader("Peak Viewing Hours")
        peak_df = run_query("SELECT * FROM PEAK_HOURS ORDER BY avg_unique_viewers DESC LIMIT 24")
        
        if not peak_df.empty:
            fig_heatmap = px.density_heatmap(
                peak_df,
                x='VIEWING_HOUR',
                y='DAY_NAME', 
                z='AVG_UNIQUE_VIEWERS',
                title='Viewing Patterns by Hour and Day',
                labels={'AVG_UNIQUE_VIEWERS': 'Avg Viewers', 'VIEWING_HOUR': 'Hour of Day'}
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with tab4:
        st.header("Device & Technical Analytics")
        
        # Device performance table
        st.subheader("Device Performance Summary")
        device_detail_df = run_query("""
            SELECT 
                device_type,
                os_name,
                connection_type,
                total_events,
                unique_users,
                avg_bitrate,
                avg_buffer_events,
                avg_rebuffer_ratio
            FROM DEVICE_ANALYTICS 
            ORDER BY total_events DESC
            LIMIT 20
        """)
        
        if not device_detail_df.empty:
            st.dataframe(device_detail_df, use_container_width=True)
        
        # Quality metrics
        quality_query = f"""
        SELECT 
            summary_date,
            avg_bitrate,
            avg_buffer_events,
            avg_rebuffer_ratio
        FROM DAILY_SUMMARY 
        WHERE 1=1 {date_filter}
        ORDER BY summary_date
        """
        
        quality_df = run_query(quality_query)
        
        if not quality_df.empty:
            fig_quality = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Average Bitrate', 'Buffer Events', 'Rebuffer Ratio', 'Quality Trend'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            fig_quality.add_trace(
                go.Scatter(x=quality_df['SUMMARY_DATE'], y=quality_df['AVG_BITRATE'], name='Bitrate'),
                row=1, col=1
            )
            fig_quality.add_trace(
                go.Scatter(x=quality_df['SUMMARY_DATE'], y=quality_df['AVG_BUFFER_EVENTS'], name='Buffer Events'),
                row=1, col=2
            )
            fig_quality.add_trace(
                go.Scatter(x=quality_df['SUMMARY_DATE'], y=quality_df['AVG_REBUFFER_RATIO'], name='Rebuffer Ratio'),
                row=2, col=1
            )
            
            fig_quality.update_layout(height=600, title_text="Streaming Quality Metrics")
            st.plotly_chart(fig_quality, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("🎯 **SF1+ Analytics Dashboard** | Built with Streamlit & Snowflake | Last updated: " + 
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()
