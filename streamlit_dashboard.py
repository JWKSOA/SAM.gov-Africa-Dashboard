#!/usr/bin/env python3
"""
streamlit_dashboard.py - Fixed version with correct data queries
UI remains exactly the same, only data handling is fixed
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Page config MUST be first Streamlit command
st.set_page_config(
    page_title="🌍 SAM.gov Africa Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import utilities
try:
    from sam_utils import get_system, CountryManager, logger
except ImportError as e:
    st.error("❌ Critical Error: Cannot import sam_utils module")
    st.error(f"Error details: {e}")
    st.stop()

# Initialize system
@st.cache_resource
def init_system():
    """Initialize SAM data system (cached)"""
    try:
        return get_system()
    except Exception as e:
        st.error(f"Failed to initialize system: {e}")
        return None

@st.cache_data(ttl=300)
def get_period_counts() -> dict:
    """Get contract counts for each time period"""
    try:
        system = init_system()
        if not system:
            return {'last_7_days': 0, 'last_30_days': 0, 'last_year': 0, 
                   'last_5_years': 0, 'all_time': 0}
        
        # Get statistics directly from database manager
        stats = system.db_manager.get_statistics()
        
        return {
            'last_7_days': stats.get('recent_7_days', 0),
            'last_30_days': stats.get('recent_30_days', 0),
            'last_year': stats.get('recent_year', 0),
            'last_5_years': 0,  # Will calculate if needed
            'all_time': stats.get('total_records', 0)
        }
        
    except Exception as e:
        st.warning(f"Error getting counts: {e}")
        return {'last_7_days': 0, 'last_30_days': 0, 'last_year': 0,
               'last_5_years': 0, 'all_time': 0}

@st.cache_data(ttl=300)
def load_data_by_period(days: int = None, limit: int = 100000) -> pd.DataFrame:
    """Load data for specific time period"""
    try:
        system = init_system()
        if not system:
            return pd.DataFrame()
        
        with system.db_manager.get_connection() as conn:
            # Build query based on period
            if days is not None:
                today = datetime.now().date().isoformat()
                start_date = (datetime.now().date() - timedelta(days=days)).isoformat()
                
                query = """
                    SELECT 
                        NoticeId,
                        Title,
                        "Department/Ind.Agency" as Department,
                        "Sub-Tier" as SubTier,
                        Office,
                        PostedDate,
                        PostedDate_normalized,
                        Type,
                        PopCountry,
                        PopCity,
                        PopState,
                        Active,
                        ResponseDeadLine,
                        SetASide,
                        NaicsCode,
                        AwardNumber,
                        AwardDate,
                        "Award$" as AwardAmount,
                        Awardee,
                        Link,
                        Description,
                        PrimaryContactFullName,
                        PrimaryContactEmail,
                        PrimaryContactPhone
                    FROM opportunities
                    WHERE PostedDate_normalized >= ?
                      AND PostedDate_normalized <= ?
                    ORDER BY PostedDate_normalized DESC
                    LIMIT ?
                """
                df = pd.read_sql_query(query, conn, params=(start_date, today, limit))
            else:
                # All data
                query = """
                    SELECT 
                        NoticeId,
                        Title,
                        "Department/Ind.Agency" as Department,
                        "Sub-Tier" as SubTier,
                        Office,
                        PostedDate,
                        PostedDate_normalized,
                        Type,
                        PopCountry,
                        PopCity,
                        PopState,
                        Active,
                        ResponseDeadLine,
                        SetASide,
                        NaicsCode,
                        AwardNumber,
                        AwardDate,
                        "Award$" as AwardAmount,
                        Awardee,
                        Link,
                        Description,
                        PrimaryContactFullName,
                        PrimaryContactEmail,
                        PrimaryContactPhone
                    FROM opportunities
                    ORDER BY PostedDate_normalized DESC
                    LIMIT ?
                """
                df = pd.read_sql_query(query, conn, params=(limit,))
            
            # Parse dates for visualization
            if not df.empty:
                # Use normalized date for parsed date
                if 'PostedDate_normalized' in df.columns:
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate_normalized'], errors='coerce')
                else:
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate'], errors='coerce')
            
            return df
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Visualization functions (UNCHANGED FROM ORIGINAL)
def create_map_visualization(df: pd.DataFrame, title_suffix: str = "") -> go.Figure:
    """Create interactive map of opportunities"""
    if df.empty or 'PopCountry' not in df.columns:
        return go.Figure()
    
    # Extract ISO codes
    df['iso3'] = df['PopCountry'].apply(
        lambda x: x.split('(')[-1].rstrip(')') if pd.notna(x) and '(' in str(x) else None
    )
    
    summary = df.groupby('iso3').size().reset_index(name='Opportunities')
    summary = summary[summary['iso3'].notna()]
    
    if summary.empty:
        return go.Figure()
    
    fig = px.choropleth(
        summary,
        locations='iso3',
        locationmode='ISO-3',
        color='Opportunities',
        hover_name='iso3',
        color_continuous_scale='Viridis',
        title=f'Contract Opportunities by Country {title_suffix}',
    )
    
    fig.update_geos(
        scope='africa',
        showcoastlines=True,
        coastlinecolor='RebeccaPurple',
        showland=True,
        landcolor='LightGray'
    )
    
    fig.update_layout(height=500, margin=dict(t=30, b=0, l=0, r=0))
    return fig

def create_timeline_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create timeline chart"""
    if df.empty or 'PostedDate_parsed' not in df.columns:
        return go.Figure()
    
    valid_dates = df[df['PostedDate_parsed'].notna()]
    if valid_dates.empty:
        return go.Figure()
    
    timeline = valid_dates.groupby(valid_dates['PostedDate_parsed'].dt.date).size().reset_index()
    timeline.columns = ['Date', 'Count']
    
    fig = px.line(timeline, x='Date', y='Count', title=title)
    fig.update_traces(mode='lines+markers')
    fig.update_layout(height=300, margin=dict(t=30, b=0, l=0, r=0))
    return fig

def display_period_content(df: pd.DataFrame, period_name: str):
    """Display content for a specific time period"""
    
    if df.empty:
        st.warning(f"No data available for {period_name}")
        return
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Opportunities", f"{len(df):,}")
    with col2:
        if 'PopCountry' in df.columns:
            st.metric("Countries", f"{df['PopCountry'].nunique()}")
        else:
            st.metric("Countries", "N/A")
    with col3:
        if 'Department' in df.columns:
            st.metric("Agencies", f"{df['Department'].nunique()}")
        else:
            st.metric("Agencies", "N/A")
    with col4:
        if 'PostedDate_parsed' in df.columns and df['PostedDate_parsed'].notna().any():
            latest = df['PostedDate_parsed'].max()
            st.metric("Latest Post", latest.strftime("%Y-%m-%d") if pd.notna(latest) else "N/A")
        else:
            st.metric("Latest Post", "N/A")
    
    # Show actual date range
    if 'PostedDate_parsed' in df.columns and df['PostedDate_parsed'].notna().any():
        min_date = df['PostedDate_parsed'].min()
        max_date = df['PostedDate_parsed'].max()
        if pd.notna(min_date) and pd.notna(max_date):
            st.info(f"📅 Showing data from {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📍 Map View", "📈 Trends", "📋 Data Table"])
    
    with tab1:
        if not df.empty and 'PopCountry' in df.columns:
            map_fig = create_map_visualization(df, f"({period_name})")
            st.plotly_chart(map_fig, use_container_width=True)
            
            # Top countries
            country_counts = df['PopCountry'].value_counts().head(10)
            if not country_counts.empty:
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.dataframe(
                        pd.DataFrame({
                            'Country': country_counts.index,
                            'Count': country_counts.values
                        }),
                        hide_index=True
                    )
                with col2:
                    fig = px.pie(values=country_counts.values, names=country_counts.index)
                    fig.update_layout(showlegend=False, height=300)
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        if not df.empty and 'PostedDate_parsed' in df.columns:
            timeline_fig = create_timeline_chart(df, f"Daily Postings - {period_name}")
            st.plotly_chart(timeline_fig, use_container_width=True)
    
    with tab3:
        # Prepare display columns
        display_cols = ['PostedDate', 'Title', 'Department', 'PopCountry', 'Type', 'Link']
        available_cols = [col for col in display_cols if col in df.columns]
        
        if available_cols:
            display_df = df[available_cols].head(100)
            
            # Clean up any NaN values for display
            display_df = display_df.fillna('')
            
            # Format links if present
            if 'Link' in display_df.columns:
                display_df['Link'] = display_df['Link'].apply(
                    lambda x: f'[View]({x})' if x and x.startswith('http') else x
                )
            
            st.dataframe(display_df, hide_index=True, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                f"sam_africa_{period_name.lower().replace(' ', '_')}.csv",
                "text/csv"
            )
        else:
            st.warning("No displayable columns available")

# Main dashboard (UI UNCHANGED)
def main():
    """Main dashboard application"""
    
    system = init_system()
    if not system:
        st.error("❌ Failed to initialize system")
        st.info("Please ensure the database exists and run: python bootstrap_historical.py")
        st.stop()
    
    st.title("🌍 SAM.gov Africa Contract Opportunities Dashboard")
    st.markdown("*Tracking U.S. government contracting opportunities in African countries*")
    
    # Display current date for reference
    st.caption(f"📅 Today's Date: {datetime.now().strftime('%B %d, %Y')}")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.divider()
        
        # Statistics
        period_counts = get_period_counts()
        
        st.subheader("📊 Statistics")
        st.metric("Last 7 Days", f"{period_counts['last_7_days']:,}")
        st.metric("Last 30 Days", f"{period_counts['last_30_days']:,}")
        st.metric("Last Year", f"{period_counts['last_year']:,}")
        st.metric("Last 5 Years", f"{period_counts['last_5_years']:,}")
        st.metric("All Time", f"{period_counts['all_time']:,}")
        
        st.divider()
        st.markdown("""
        **Data Source:** SAM.gov  
        **Updates:** Daily at midnight  
        **Coverage:** All 54 African countries
        """)
    
    # Main tabs with date ranges
    today = datetime.now().date()
    tabs_info = [
        (f"📅 Last 7 Days ({(today - timedelta(days=7)).strftime('%b %d')} - {today.strftime('%b %d')})", 7),
        (f"📅 Last 30 Days ({(today - timedelta(days=30)).strftime('%b %d')} - {today.strftime('%b %d')})", 30),
        ("📅 Last Year", 365),
        ("📅 Last 5 Years", 1825),
        ("🗃️ Archive (All Time)", None)
    ]
    
    tabs = st.tabs([info[0] for info in tabs_info])
    
    for tab, (tab_name, days) in zip(tabs, tabs_info):
        with tab:
            if days is not None:
                df = load_data_by_period(days=days)
                period_name = tab_name.split('(')[0].strip().replace('📅', '').strip()
            else:
                df = load_data_by_period(days=None)
                period_name = "All Time"
                if not df.empty:
                    st.info(f"📚 Archive contains {len(df):,} total records from all time")
            
            display_period_content(df, period_name)

if __name__ == "__main__":
    main()