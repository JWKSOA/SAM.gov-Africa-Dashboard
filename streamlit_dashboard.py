#!/usr/bin/env python3
"""
streamlit_dashboard.py - Fixed version with proper column handling and date filtering
Handles SAM.gov column names correctly and shows accurate date ranges
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
                st.warning("Database needs updating. Please run: python fix_database.py")
        
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
        today = datetime.now().date()
        
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Check if PostedDate_normalized exists
            cur.execute("PRAGMA table_info(opportunities)")
            columns = {row[1] for row in cur.fetchall()}
            
            if 'PostedDate_normalized' in columns:
                # Use normalized dates for accurate counting
                periods = [
                    ('last_7_days', (today - timedelta(days=7)).isoformat(), today.isoformat()),
                    ('last_30_days', (today - timedelta(days=30)).isoformat(), today.isoformat()),
                    ('last_year', (today - timedelta(days=365)).isoformat(), today.isoformat()),
                    ('last_5_years', (today - timedelta(days=1825)).isoformat(), today.isoformat())
                ]
                
                for period_name, start_date, end_date in periods:
                    cur.execute("""
                        SELECT COUNT(*) FROM opportunities 
                        WHERE PostedDate_normalized >= ?
                        AND PostedDate_normalized <= ?
                    """, (start_date, end_date))
                    counts[period_name] = cur.fetchone()[0]
            else:
                # Fallback: Try to parse dates on the fly
                for period_name, days in [('last_7_days', 7), ('last_30_days', 30), 
                                         ('last_year', 365), ('last_5_years', 1825)]:
                    cutoff_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
                    # Try different date formats
                    cur.execute("""
                        SELECT COUNT(*) FROM opportunities 
                        WHERE (
                            date(PostedDate) >= date(?) 
                            OR date(substr(PostedDate, 1, 10)) >= date(?)
                        )
                    """, (cutoff_date, cutoff_date))
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
    """Load data for specific time period with proper column name mapping"""
    try:
        system = init_system()
        if not system:
            return pd.DataFrame()
        
        with system.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Check available columns
            cur.execute("PRAGMA table_info(opportunities)")
            available_columns = {row[1] for row in cur.fetchall()}
            
            # Build column selection with proper aliases
            # Map database columns to friendly names
            column_mapping = []
            
            # Essential columns
            column_mapping.append('NoticeID')
            column_mapping.append('Title')
            
            # Handle Department column (might be stored as "Department/Ind.Agency")
            if '"Department/Ind.Agency"' in available_columns or 'Department/Ind.Agency' in available_columns:
                column_mapping.append('"Department/Ind.Agency" as Department')
            elif 'Department' in available_columns:
                column_mapping.append('Department')
            else:
                column_mapping.append('NULL as Department')
            
            # Country columns
            column_mapping.append('PopCountry' if 'PopCountry' in available_columns else 'NULL as PopCountry')
            column_mapping.append('CountryCode' if 'CountryCode' in available_columns else 'NULL as CountryCode')
            
            # Date columns
            column_mapping.append('PostedDate' if 'PostedDate' in available_columns else 'NULL as PostedDate')
            if 'PostedDate_normalized' in available_columns:
                column_mapping.append('PostedDate_normalized')
            
            # Other important columns
            column_mapping.append('Type' if 'Type' in available_columns else 'NULL as Type')
            column_mapping.append('AwardNumber' if 'AwardNumber' in available_columns else 'NULL as AwardNumber')
            column_mapping.append('AwardDate' if 'AwardDate' in available_columns else 'NULL as AwardDate')
            column_mapping.append('"Award$" as AwardAmount' if 'Award$' in available_columns else 'NULL as AwardAmount')
            column_mapping.append('Awardee' if 'Awardee' in available_columns else 'NULL as Awardee')
            column_mapping.append('Link' if 'Link' in available_columns else 'NULL as Link')
            column_mapping.append('Description' if 'Description' in available_columns else 'NULL as Description')
            
            # Contact columns
            if 'PrimaryContactFullName' in available_columns:
                column_mapping.append('PrimaryContactFullName')
            if 'PrimaryContactEmail' in available_columns:
                column_mapping.append('PrimaryContactEmail')
            if 'PrimaryContactPhone' in available_columns:
                column_mapping.append('PrimaryContactPhone')
            
            # Organization columns
            if 'OrganizationType' in available_columns:
                column_mapping.append('OrganizationType')
            if '"Sub-Tier"' in available_columns or 'Sub-Tier' in available_columns:
                column_mapping.append('"Sub-Tier" as SubTier')
            if 'Office' in available_columns:
                column_mapping.append('Office')
            
            columns_str = ', '.join(column_mapping)
            
            # Build query based on period
            if 'PostedDate_normalized' in available_columns:
                # Use normalized dates for accurate filtering
                if days is not None:
                    today = datetime.now().date()
                    start_date = (today - timedelta(days=days)).isoformat()
                    end_date = today.isoformat()
                    
                    query = f"""
                        SELECT {columns_str}
                        FROM opportunities
                        WHERE PostedDate_normalized >= ?
                        AND PostedDate_normalized <= ?
                        ORDER BY PostedDate_normalized DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(start_date, end_date, limit))
                else:
                    # All data
                    query = f"""
                        SELECT {columns_str}
                        FROM opportunities
                        ORDER BY PostedDate_normalized DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(limit,))
            else:
                # Fallback without normalized dates
                if days is not None:
                    cutoff_date = (datetime.now().date() - timedelta(days=days)).strftime('%Y-%m-%d')
                    query = f"""
                        SELECT {columns_str}
                        FROM opportunities
                        WHERE date(substr(PostedDate, 1, 10)) >= date(?)
                        ORDER BY PostedDate DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(cutoff_date, limit))
                else:
                    query = f"""
                        SELECT {columns_str}
                        FROM opportunities
                        ORDER BY PostedDate DESC
                        LIMIT ?
                    """
                    df = pd.read_sql_query(query, conn, params=(limit,))
            
            # Parse dates for visualization
            if not df.empty:
                if 'PostedDate_normalized' in df.columns:
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate_normalized'], errors='coerce')
                elif 'PostedDate' in df.columns:
                    # Try multiple date formats
                    df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate'], errors='coerce', utc=True)
                    # Convert to local timezone and remove timezone info for display
                    df['PostedDate_parsed'] = df['PostedDate_parsed'].dt.tz_localize(None)
            
            return df
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Visualization functions
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
        # Prepare display columns - only include columns that exist
        available_display_cols = []
        for col in ['PostedDate', 'Title', 'Department', 'PopCountry', 'Type', 'Link']:
            if col in df.columns:
                available_display_cols.append(col)
        
        if available_display_cols:
            display_df = df[available_display_cols].head(100)
            
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

# Main dashboard
def main():
    """Main dashboard application"""
    
    system = init_system()
    if not system:
        st.error("❌ Failed to initialize system")
        st.info("Please ensure the database exists and run: python fix_database.py")
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