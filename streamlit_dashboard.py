#!/usr/bin/env python3
"""
streamlit_dashboard.py - Optimized SAM.gov Africa Dashboard
Fixed version with proper page config placement
"""

import os
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ============================================================================
# CRITICAL: Page configuration MUST be the first Streamlit command
# ============================================================================
st.set_page_config(
    page_title="🌍 SAM.gov Africa Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# NOW we can do other Streamlit operations
# ============================================================================

# AUTO-COMBINE DATABASES ON STREAMLIT CLOUD (if needed)
# This now happens AFTER set_page_config
def ensure_database():
    """Ensure database exists and is properly combined"""
    db_path = Path("data/opportunities.db")
    
    # Check if database exists and is valid (not just a Git LFS pointer)
    if not db_path.exists() or db_path.stat().st_size < 1000000:
        try:
            # Try to import combine_databases
            from combine_databases import combine_databases
            with st.spinner("🔄 Preparing database (first time setup, this may take a minute)..."):
                success = combine_databases()
                if success:
                    st.success("✅ Database ready!")
                    return True
                else:
                    st.error("Failed to prepare database")
                    return False
        except ImportError:
            # combine_databases.py might not be available on Streamlit Cloud
            st.warning("Database preparation module not available")
            return False
        except Exception as e:
            st.error(f"Database preparation failed: {e}")
            st.info("The dashboard will attempt to work with available data")
            return False
    return True

# Ensure database is ready
ensure_database()

# Import utilities
try:
    from sam_utils import get_system, CountryManager, logger
except ImportError as e:
    st.error("❌ Critical Error: Cannot import sam_utils module")
    st.error(f"Error details: {e}")
    st.info("Please ensure sam_utils.py is in the repository")
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

# Database queries with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_summary_stats() -> Dict[str, Any]:
    """Load summary statistics"""
    try:
        system = init_system()
        if not system:
            return {
                'total_records': 0,
                'by_country': {},
                'recent_records': 0,
                'size_mb': 0,
                'error': 'System initialization failed'
            }
        return system.db_manager.get_statistics()
    except Exception as e:
        return {
            'total_records': 0,
            'by_country': {},
            'recent_records': 0,
            'size_mb': 0,
            'error': str(e)
        }

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_by_period(days: Optional[int] = None, limit: int = 100000) -> pd.DataFrame:
    """Load data for specific time period"""
    try:
        system = init_system()
        if not system:
            return pd.DataFrame()
        
        query = """
            SELECT 
                NoticeID, Title, "Department/Ind.Agency" as Department,
                PopCountry, CountryCode, PostedDate, Type,
                AwardNumber, AwardDate, "Award$" as AwardAmount,
                Awardee, Link, Description,
                PrimaryContactTitle, PrimaryContactFullName,
                PrimaryContactEmail, PrimaryContactPhone,
                OrganizationType, "Sub-Tier" as SubTier, Office
            FROM opportunities
            {where_clause}
            ORDER BY PostedDate DESC
            LIMIT ?
        """
        
        if days is not None:
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            where_clause = "WHERE date(PostedDate) >= date(?)"
            params = (cutoff, limit)
        else:
            where_clause = ""
            params = (limit,)
        
        query = query.format(where_clause=where_clause)
        
        with system.db_manager.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        
        # Parse dates
        if not df.empty and 'PostedDate' in df.columns:
            df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate'], errors='coerce', utc=True)
        
        return df
        
    except Exception as e:
        st.warning(f"Error loading data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_historical_summary() -> pd.DataFrame:
    """Load historical summary by month"""
    try:
        system = init_system()
        if not system:
            return pd.DataFrame()
        
        query = """
            SELECT 
                strftime('%Y-%m', PostedDate) as Month,
                PopCountry,
                COUNT(*) as Count
            FROM opportunities
            WHERE PostedDate IS NOT NULL
            GROUP BY strftime('%Y-%m', PostedDate), PopCountry
            ORDER BY Month DESC
        """
        
        with system.db_manager.get_connection() as conn:
            df = pd.read_sql_query(query, conn)
        
        return df
        
    except Exception as e:
        st.warning(f"Error loading historical data: {e}")
        return pd.DataFrame()

# Visualization functions
def create_map_visualization(df: pd.DataFrame, title_suffix: str = "") -> go.Figure:
    """Create interactive map of opportunities"""
    if df.empty:
        return go.Figure()
    
    # Extract ISO codes and aggregate
    df['iso3'] = df['PopCountry'].apply(
        lambda x: x.split('(')[-1].rstrip(')') if '(' in str(x) else None
    )
    
    summary = df.groupby('iso3').size().reset_index(name='Opportunities')
    summary = summary[summary['iso3'].notna()]
    
    # Create choropleth map
    fig = px.choropleth(
        summary,
        locations='iso3',
        locationmode='ISO-3',
        color='Opportunities',
        hover_name='iso3',
        color_continuous_scale='Viridis',
        title=f'Contract Opportunities by Country {title_suffix}',
        labels={'Opportunities': 'Number of Opportunities'}
    )
    
    fig.update_geos(
        scope='africa',
        showcoastlines=True,
        coastlinecolor='RebeccaPurple',
        showland=True,
        landcolor='LightGray',
        showcountries=True,
        countrycolor='White'
    )
    
    fig.update_layout(
        height=500,
        margin=dict(t=30, b=0, l=0, r=0)
    )
    
    return fig

def create_timeline_chart(df: pd.DataFrame, title: str = "Daily Contract Postings") -> go.Figure:
    """Create timeline chart of opportunities"""
    if df.empty or 'PostedDate_parsed' not in df.columns:
        return go.Figure()
    
    # Group by date
    timeline = df.groupby(df['PostedDate_parsed'].dt.date).size().reset_index()
    timeline.columns = ['Date', 'Count']
    
    fig = px.line(
        timeline,
        x='Date',
        y='Count',
        title=title,
        labels={'Count': 'Number of Contracts', 'Date': 'Posted Date'}
    )
    
    fig.update_traces(mode='lines+markers')
    fig.update_layout(
        height=300,
        margin=dict(t=30, b=0, l=0, r=0),
        showlegend=False
    )
    
    return fig

def create_agency_chart(df: pd.DataFrame) -> go.Figure:
    """Create agency distribution chart"""
    if df.empty:
        return go.Figure()
    
    top_agencies = df.groupby('Department').size().nlargest(15).reset_index()
    top_agencies.columns = ['Agency', 'Count']
    
    fig = px.bar(
        top_agencies,
        x='Count',
        y='Agency',
        orientation='h',
        title='Top 15 Agencies by Opportunity Count',
        labels={'Count': 'Number of Opportunities', 'Agency': ''}
    )
    
    fig.update_layout(
        height=400,
        margin=dict(t=30, b=0, l=0, r=0),
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

def display_period_content(df: pd.DataFrame, period_name: str, period_description: str):
    """Display content for a specific time period"""
    
    # Filter controls in sidebar
    with st.sidebar:
        st.subheader(f"🔍 {period_name} Filters")
        
        if not df.empty:
            # Country filter
            countries = sorted(df['PopCountry'].dropna().unique())
            selected_country = st.selectbox(
                f"Country ({period_name})",
                ["All Countries"] + list(countries),
                index=0,
                key=f"country_{period_name}"
            )
            
            # Agency filter
            agencies = sorted(df['Department'].dropna().unique())
            selected_agency = st.selectbox(
                f"Agency ({period_name})",
                ["All Agencies"] + list(agencies)[:50],
                index=0,
                key=f"agency_{period_name}"
            )
        else:
            selected_country = "All Countries"
            selected_agency = "All Agencies"
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_country != "All Countries":
        filtered_df = filtered_df[filtered_df['PopCountry'] == selected_country]
    
    if selected_agency != "All Agencies":
        filtered_df = filtered_df[filtered_df['Department'] == selected_agency]
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Opportunities", f"{len(filtered_df):,}")
    
    with col2:
        unique_countries = filtered_df['PopCountry'].nunique()
        st.metric("Countries", f"{unique_countries}")
    
    with col3:
        unique_agencies = filtered_df['Department'].nunique()
        st.metric("Agencies", f"{unique_agencies}")
    
    with col4:
        if not filtered_df.empty and 'PostedDate_parsed' in filtered_df.columns:
            latest_date = filtered_df['PostedDate_parsed'].max()
            if pd.notna(latest_date):
                st.metric("Latest Post", latest_date.strftime("%Y-%m-%d"))
            else:
                st.metric("Latest Post", "N/A")
        else:
            st.metric("Latest Post", "N/A")
    
    # Create sub-tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📍 Map View", "📈 Trends", "🏢 Agencies", "📋 Data Table"])
    
    with tab1:
        st.subheader(f"Geographic Distribution - {period_description}")
        
        if not filtered_df.empty:
            map_fig = create_map_visualization(filtered_df, f"({period_description})")
            st.plotly_chart(map_fig, use_container_width=True)
            
            # Top countries table
            st.subheader("Top Countries by Opportunity Count")
            country_counts = filtered_df['PopCountry'].value_counts().head(10)
            
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.dataframe(
                    country_counts.reset_index().rename(
                        columns={'index': 'Country', 'PopCountry': 'Opportunities'}
                    ),
                    hide_index=True,
                    use_container_width=True
                )
            
            with col2:
                if not country_counts.empty:
                    fig = px.pie(
                        values=country_counts.values,
                        names=country_counts.index,
                        title="Distribution by Country"
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(showlegend=False, height=300)
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No data available for {period_description}")
    
    with tab2:
        st.subheader(f"Temporal Trends - {period_description}")
        
        if not filtered_df.empty and 'PostedDate_parsed' in filtered_df.columns:
            timeline_fig = create_timeline_chart(
                filtered_df, 
                f"Daily Contract Postings - {period_description}"
            )
            st.plotly_chart(timeline_fig, use_container_width=True)
            
            # Monthly summary
            st.subheader("Monthly Summary")
            
            monthly = filtered_df.copy()
            monthly['Month'] = monthly['PostedDate_parsed'].dt.to_period('M')
            monthly_summary = monthly.groupby('Month').agg({
                'NoticeID': 'count',
                'PopCountry': 'nunique',
                'Department': 'nunique'
            }).reset_index()
            monthly_summary.columns = ['Month', 'Opportunities', 'Countries', 'Agencies']
            monthly_summary['Month'] = monthly_summary['Month'].astype(str)
            
            st.dataframe(
                monthly_summary.sort_values('Month', ascending=False).head(12),
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No temporal data available for {period_description}")
    
    with tab3:
        st.subheader(f"Agency Analysis - {period_description}")
        
        if not filtered_df.empty:
            agency_fig = create_agency_chart(filtered_df)
            st.plotly_chart(agency_fig, use_container_width=True)
            
            # Agency statistics table
            st.subheader("Agency Statistics")
            
            agency_stats = filtered_df.groupby('Department').agg({
                'NoticeID': 'count',
                'PopCountry': lambda x: len(x.unique()),
                'PostedDate': 'max'
            }).reset_index()
            agency_stats.columns = ['Agency', 'Opportunities', 'Countries', 'Last Post']
            agency_stats = agency_stats.sort_values('Opportunities', ascending=False).head(20)
            
            st.dataframe(
                agency_stats,
                hide_index=True,
                use_container_width=True
            )
        else:
            st.info(f"No agency data available for {period_description}")
    
    with tab4:
        st.subheader(f"Detailed Contract Data - {period_description}")
        
        if not filtered_df.empty:
            # Display controls
            col1, col2 = st.columns([3, 1])
            
            with col1:
                search_term = st.text_input(
                    "Search in titles and descriptions",
                    placeholder="Enter keywords...",
                    key=f"search_{period_name}"
                )
            
            with col2:
                show_count = st.selectbox(
                    "Show rows",
                    [25, 50, 100, 200],
                    index=1,
                    key=f"show_{period_name}"
                )
            
            # Apply search filter
            display_df = filtered_df.copy()
            
            if search_term:
                mask = (
                    display_df['Title'].str.contains(search_term, case=False, na=False) |
                    display_df['Description'].str.contains(search_term, case=False, na=False)
                )
                display_df = display_df[mask]
            
            # Prepare display columns
            display_cols = [
                'PostedDate', 'Title', 'Department', 'PopCountry',
                'Type', 'Link'
            ]
            
            # Ensure columns exist
            for col in display_cols:
                if col not in display_df.columns:
                    display_df[col] = ''
            
            # Sort and limit
            display_df = display_df.sort_values('PostedDate', ascending=False).head(show_count)
            
            # Create clickable links
            if 'Link' in display_df.columns:
                display_df['Link'] = display_df['Link'].apply(
                    lambda x: f'<a href="{x}" target="_blank">View on SAM.gov</a>' 
                    if x and str(x) != 'nan' else ''
                )
            
            # Display table
            st.markdown(
                display_df[display_cols].to_html(escape=False, index=False),
                unsafe_allow_html=True
            )
            
            # Download button
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"sam_africa_{period_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key=f"download_{period_name}"
            )
        else:
            st.info(f"No data available for {period_description}")

# Main dashboard
def main():
    """Main dashboard application"""
    
    # Initialize
    system = init_system()
    
    if not system:
        st.error("❌ Failed to initialize the system")
        st.info("Please check that all required files are present in the repository")
        st.stop()
    
    # Header
    st.title("🌍 SAM.gov Africa Contract Opportunities Dashboard")
    st.markdown("*Real-time tracking of U.S. government contracting opportunities in African countries*")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Data refresh button
        if st.button("🔄 Trigger Data Update", use_container_width=True):
            # Clear caches
            st.cache_data.clear()
            
            # Trigger GitHub Action if configured
            github_token = st.secrets.get("github_token", "")
            if github_token:
                import requests
                owner = st.secrets.get("github_owner", "JWKSOA")
                repo = st.secrets.get("github_repo", "SAM.gov-Africa-Dashboard")
                workflow = st.secrets.get("github_workflow", "update-sam-db.yml")
                
                url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow}/dispatches"
                headers = {
                    "Authorization": f"Bearer {github_token}",
                    "Accept": "application/vnd.github+json"
                }
                
                try:
                    response = requests.post(
                        url, 
                        headers=headers, 
                        json={"ref": "main"}, 
                        timeout=10
                    )
                    if response.status_code in (201, 204):
                        st.success("✅ Update triggered! Check GitHub Actions.")
                    else:
                        st.error(f"❌ Failed: {response.status_code}")
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
            else:
                st.info("ℹ️ Configure github_token in secrets to enable updates")
        
        st.divider()
        
        # Load overall statistics
        stats = load_summary_stats()
        
        st.subheader("📊 Overall Statistics")
        
        if 'error' in stats:
            st.warning(f"Stats unavailable: {stats['error']}")
        else:
            st.metric("Total Records", f"{stats['total_records']:,}")
            st.metric("Database Size", f"{stats['size_mb']:.1f} MB")
            
            unique_countries = len([c for c in stats['by_country'] if stats['by_country'][c] > 0])
            st.metric("Active Countries", f"{unique_countries}/54")
        
        # Info section
        st.divider()
        st.subheader("ℹ️ About")
        st.markdown("""
        This dashboard tracks U.S. government contract opportunities
        posted on SAM.gov that involve African countries.
        
        **Data Sources:**
        - Current: SAM.gov daily extract
        - Historical: FY1998-present archives
        
        **Updates:**
        - Automated daily at 04:30 UTC
        - Manual refresh available
        """)
    
    # Main content area - Time Period Tabs
    st.divider()
    
    # Create main tabs for different time periods
    tab_7days, tab_30days, tab_1year, tab_5years, tab_archive = st.tabs([
        "📅 Last 7 Days",
        "📅 Last 30 Days", 
        "📅 Last Year",
        "📅 Last 5 Years",
        "🗃️ Archive (All Time)"
    ])
    
    with tab_7days:
        df_7days = load_data_by_period(days=7)
        display_period_content(df_7days, "Last 7 Days", "Past Week")
    
    with tab_30days:
        df_30days = load_data_by_period(days=30)
        display_period_content(df_30days, "Last 30 Days", "Past Month")
    
    with tab_1year:
        df_1year = load_data_by_period(days=365)
        display_period_content(df_1year, "Last Year", "Past Year")
    
    with tab_5years:
        df_5years = load_data_by_period(days=1825)  # 365 * 5
        display_period_content(df_5years, "Last 5 Years", "Past 5 Years")
    
    with tab_archive:
        df_archive = load_data_by_period(days=None)  # All data
        
        # Display archive summary first
        st.info(f"""
        📚 **Archive Contains All Historical Data**
        - Total Records: {len(df_archive):,}
        - Date Range: {df_archive['PostedDate'].min() if not df_archive.empty else 'N/A'} to {df_archive['PostedDate'].max() if not df_archive.empty else 'N/A'}
        - This includes all data from FY1998 to present
        """)
        
        display_period_content(df_archive, "Archive", "All Time")
    
    # Footer
    st.divider()
    
    with st.expander("📊 System Information"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **Database Location:** `{system.config.db_path}`  
            **Total Records:** {stats.get('total_records', 0):,}  
            **Database Size:** {stats.get('size_mb', 0):.1f} MB
            """)
        
        with col2:
            st.markdown("""
            **Top 5 Countries:**
            """)
            if 'by_country' in stats:
                for country, count in list(stats['by_country'].items())[:5]:
                    st.markdown(f"- {country}: {count:,}")

if __name__ == "__main__":
    main()