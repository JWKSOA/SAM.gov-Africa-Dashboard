#!/usr/bin/env python3
"""
streamlit_dashboard.py - Fixed version with proper date filtering
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
        system = get_system()
        # Ensure PostedDate_normalized column exists
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(opportunities)")
            columns = {row[1] for row in cur.fetchall()}
            
            if 'PostedDate_normalized' not in columns:
                st.warning("Database needs updating. Please run: python download_and_update.py")
        
        return system
    except Exception as e:
        st.error(f"Failed to initialize system: {e}")
        return None

@st.cache_data(ttl=300)
def get_period_counts() -> dict:
    """Get contract counts for each time period using normalized dates"""
    try:
        system = init_system()
        if not system:
            return {'last_7_days': 0, 'last_30_days': 0, 'last_year': 0, 
                   'last_5_years': 0, 'all_time': 0}
        
        counts = {}
        today = datetime.now().date().isoformat()
        
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Check if PostedDate_normalized exists
            cur.execute("PRAGMA table_info(opportunities)")
            columns = {row[1] for row in cur.fetchall()}
            
            if 'PostedDate_normalized' in columns:
                # Use normalized dates for accurate counting
                periods = [
                    ('last_7_days', 7),
                    ('last_30_days', 30),
                    ('last_year', 365),
                    ('last_5_years', 1825)
                ]
                
                for period_name, days in periods:
                    cutoff_date = (datetime.now().date() - timedelta(days=days)).isoformat()
                    cur.execute("""
                        SELECT COUNT(*) FROM opportunities 
                        WHERE PostedDate_normalized >= ?
                        AND PostedDate_normalized <= ?
                    """, (cutoff_date, today))
                    counts[period_name] = cur.fetchone()[0]
            else:
                # Fallback: use original PostedDate with text comparison
                st.warning("Database needs updating for accurate date filtering")
                
                for period_name, days in [('last_7_days', 7), ('last_30_days', 30), 
                                         ('last_year', 365), ('last_5_years', 1825)]:
                    cutoff_date = (datetime.now().date() - timedelta(days=days)).isoformat()
                    cur.execute("""
                        SELECT COUNT(*) FROM opportunities 
                        WHERE date(PostedDate) >= date(?)
                    """, (cutoff_date,))
                    counts[period_name] = cur.fetchone()[0]
            
            # All time count
            cur.execute("SELECT COUNT(*) FROM opportunities")
            counts['all_time'] = cur.fetchone()[0]
            
        return counts
        
    except Exception as e:
        st.warning(f"Error getting counts: {e}")
        return {'last_7_days': 0, 'last_30_days': 0, 'last_year': 0,
               'last_5_years': 0, 'all_time': 0}

@st.cache_data(ttl=300)
def load_data_by_period(days: int = None, limit: int = 100000) -> pd.DataFrame:
    """Load data for specific time period using normalized dates"""
    try:
        system = init_system()
        if not system:
            return pd.DataFrame()
        
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Check if PostedDate_normalized exists
            cur.execute("PRAGMA table_info(opportunities)")
            columns = {row[1] for row in cur.fetchall()}
            has_normalized = 'PostedDate_normalized' in columns
            
            if has_normalized:
                # Use normalized dates for accurate filtering
                if days is not None:
                    cutoff_date = (datetime.now().date() - timedelta(days=days)).isoformat()
                    today = datetime.now().date().isoformat()
                    
                    query = """
                        SELECT 
                            NoticeID, Title, "Department/Ind.Agency" as Department,
                            PopCountry, CountryCode, PostedDate, 
                            PostedDate_normalized, Type,
                            AwardNumber, AwardDate, "Award$" as AwardAmount,
                            Awardee, Link, Description,
                            PrimaryContactTitle, PrimaryContactFullName,
                            PrimaryContactEmail, PrimaryContactPhone,
                            OrganizationType, "Sub-Tier" as SubTier, Office
                        FROM opportunities
                        WHERE PostedDate_normalized >= ?
                        AND PostedDate_normalized <= ?
                        ORDER BY PostedDate_normalized DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(cutoff_date, today, limit))
                else:
                    # All data
                    query = """
                        SELECT 
                            NoticeID, Title, "Department/Ind.Agency" as Department,
                            PopCountry, CountryCode, PostedDate,
                            PostedDate_normalized, Type,
                            AwardNumber, AwardDate, "Award$" as AwardAmount,
                            Awardee, Link, Description,
                            PrimaryContactTitle, PrimaryContactFullName,
                            PrimaryContactEmail, PrimaryContactPhone,
                            OrganizationType, "Sub-Tier" as SubTier, Office
                        FROM opportunities
                        ORDER BY PostedDate_normalized DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(limit,))
                
                # Parse normalized dates
                if not df.empty and 'PostedDate_normalized' in df.columns:
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate_normalized'], errors='coerce')
                
            else:
                # Fallback: use original PostedDate
                st.warning("Please run 'python download_and_update.py' to enable proper date filtering")
                
                if days is not None:
                    cutoff_date = (datetime.now().date() - timedelta(days=days)).isoformat()
                    query = """
                        SELECT * FROM opportunities
                        WHERE date(PostedDate) >= date(?)
                        ORDER BY PostedDate DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(cutoff_date, limit))
                else:
                    query = "SELECT * FROM opportunities ORDER BY PostedDate DESC LIMIT ?"
                    df = pd.read_sql_query(query, conn, params=(limit,))
                
                # Try to parse dates
                if not df.empty and 'PostedDate' in df.columns:
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate'], errors='coerce')
            
            return df
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Visualization functions
def create_map_visualization(df: pd.DataFrame, title_suffix: str = "") -> go.Figure:
    """Create interactive map of opportunities"""
    if df.empty:
        return go.Figure()
    
    # Extract ISO codes
    df['iso3'] = df['PopCountry'].apply(
        lambda x: x.split('(')[-1].rstrip(')') if '(' in str(x) else None
    )
    
    summary = df.groupby('iso3').size().reset_index(name='Opportunities')
    summary = summary[summary['iso3'].notna()]
    
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
        st.metric("Countries", f"{df['PopCountry'].nunique()}")
    with col3:
        st.metric("Agencies", f"{df['Department'].nunique()}")
    with col4:
        if 'PostedDate_parsed' in df.columns and df['PostedDate_parsed'].notna().any():
            latest = df['PostedDate_parsed'].max()
            st.metric("Latest Post", latest.strftime("%Y-%m-%d") if pd.notna(latest) else "N/A")
        else:
            st.metric("Latest Post", "N/A")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📍 Map View", "📈 Trends", "📋 Data Table"])
    
    with tab1:
        if not df.empty:
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
        # Display table
        display_cols = ['PostedDate', 'Title', 'Department', 'PopCountry', 'Type']
        display_df = df[display_cols].head(100) if not df.empty else pd.DataFrame()
        
        if not display_df.empty:
            st.dataframe(display_df, hide_index=True, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                f"sam_africa_{period_name.lower().replace(' ', '_')}.csv",
                "text/csv"
            )

# Main dashboard
def main():
    """Main dashboard application"""
    
    system = init_system()
    if not system:
        st.error("❌ Failed to initialize system")
        st.stop()
    
    st.title("🌍 SAM.gov Africa Contract Opportunities Dashboard")
    st.markdown("*Tracking U.S. government contracting opportunities in African countries*")
    
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
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📅 Last 7 Days",
        "📅 Last 30 Days",
        "📅 Last Year",
        "📅 Last 5 Years",
        "🗃️ Archive (All Time)"
    ])
    
    with tab1:
        df = load_data_by_period(days=7)
        display_period_content(df, "Last 7 Days")
    
    with tab2:
        df = load_data_by_period(days=30)
        display_period_content(df, "Last 30 Days")
    
    with tab3:
        df = load_data_by_period(days=365)
        display_period_content(df, "Last Year")
    
    with tab4:
        df = load_data_by_period(days=1825)
        display_period_content(df, "Last 5 Years")
    
    with tab5:
        df = load_data_by_period(days=None)
        if not df.empty:
            st.info(f"📚 Archive contains {len(df):,} total records from all time")
        display_period_content(df, "All Time")

if __name__ == "__main__":
    main()