#!/usr/bin/env python3
"""
streamlit_dashboard.py - Optimized SAM.gov Africa Dashboard
High-performance dashboard with caching and efficient queries
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

# Import utilities - will be created if not exists
try:
    from sam_utils import get_system, CountryManager, logger
except ImportError:
    st.error("Please ensure sam_utils.py is in the same directory")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="🌍 SAM.gov Africa Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize system
@st.cache_resource
def init_system():
    """Initialize SAM data system (cached)"""
    return get_system()

# Database queries with caching
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_summary_stats() -> Dict[str, Any]:
    """Load summary statistics"""
    system = init_system()
    return system.db_manager.get_statistics()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_recent_data(days: int = 30, limit: int = 10000) -> pd.DataFrame:
    """Load recent data with efficient query"""
    system = init_system()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    
    query = """
        SELECT 
            NoticeID, Title, "Department/Ind.Agency" as Department,
            PopCountry, CountryCode, PostedDate, Type,
            AwardNumber, AwardDate, "Award$" as AwardAmount,
            Awardee, Link, Description,
            PrimaryContactEmail, PrimaryContactPhone
        FROM opportunities
        WHERE date(PostedDate) >= date(?)
        ORDER BY PostedDate DESC
        LIMIT ?
    """
    
    with system.db_manager.get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=(cutoff, limit))
    
    # Parse dates
    if not df.empty and 'PostedDate' in df.columns:
        df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate'], errors='coerce')
    
    return df

@st.cache_data(ttl=3600)
def load_historical_summary() -> pd.DataFrame:
    """Load historical summary by month"""
    system = init_system()
    
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

@st.cache_data(ttl=3600)
def load_agency_summary() -> pd.DataFrame:
    """Load agency summary statistics"""
    system = init_system()
    
    query = """
        SELECT 
            "Department/Ind.Agency" as Agency,
            COUNT(*) as Total,
            COUNT(DISTINCT PopCountry) as Countries,
            MAX(PostedDate) as LastPost
        FROM opportunities
        WHERE "Department/Ind.Agency" IS NOT NULL
        GROUP BY "Department/Ind.Agency"
        ORDER BY Total DESC
        LIMIT 20
    """
    
    with system.db_manager.get_connection() as conn:
        df = pd.read_sql_query(query, conn)
    
    return df

# Visualization functions
def create_map_visualization(df: pd.DataFrame) -> go.Figure:
    """Create interactive map of opportunities"""
    if df.empty:
        return go.Figure()
    
    # Extract ISO codes and aggregate
    country_manager = CountryManager()
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
        title='Contract Opportunities by Country',
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

def create_timeline_chart(df: pd.DataFrame) -> go.Figure:
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
        title='Daily Contract Postings',
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

# Main dashboard
def main():
    """Main dashboard application"""
    
    # Initialize
    system = init_system()
    
    # Header
    st.title("🌍 SAM.gov Africa Contract Opportunities Dashboard")
    st.markdown("*Real-time tracking of U.S. government contracting opportunities in African countries*")
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Data refresh
        if st.button("🔄 Trigger Data Update", use_container_width="stretch"):
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
        
        # Filters
        st.subheader("🔍 Filters")
        
        date_range = st.select_slider(
            "Date Range",
            options=[7, 30, 90, 180, 365],
            value=30,
            format_func=lambda x: f"Last {x} days"
        )
        
        # Load data based on selection
        df = load_recent_data(days=date_range)
        
        if not df.empty:
            # Country filter
            countries = sorted(df['PopCountry'].dropna().unique())
            selected_country = st.selectbox(
                "Country",
                ["All Countries"] + list(countries),
                index=0
            )
            
            # Agency filter
            agencies = sorted(df['Department'].dropna().unique())
            selected_agency = st.selectbox(
                "Agency",
                ["All Agencies"] + list(agencies)[:50],  # Limit to top 50
                index=0
            )
        else:
            selected_country = "All Countries"
            selected_agency = "All Agencies"
        
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
        
        **Coverage:**
        - All 54 African countries
        - Federal contracts & grants
        - Award notifications
        """)
    
    # Main content area
    
    # Load statistics
    stats = load_summary_stats()
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Opportunities",
            f"{stats['total_records']:,}",
            delta=None
        )
    
    with col2:
        st.metric(
            "Recent (30 days)",
            f"{stats['recent_records']:,}",
            delta=None
        )
    
    with col3:
        unique_countries = len([c for c in stats['by_country'] if stats['by_country'][c] > 0])
        st.metric(
            "Active Countries",
            f"{unique_countries}/54",
            delta=None
        )
    
    with col4:
        db_size = stats['size_mb']
        st.metric(
            "Database Size",
            f"{db_size:.1f} MB",
            delta=None
        )
    
    # Apply filters to dataframe
    filtered_df = df.copy()
    
    if selected_country != "All Countries":
        filtered_df = filtered_df[filtered_df['PopCountry'] == selected_country]
    
    if selected_agency != "All Agencies":
        filtered_df = filtered_df[filtered_df['Department'] == selected_agency]
    
    # Visualizations
    st.divider()
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📍 Map View", "📈 Trends", "🏢 Agencies", "📋 Data Table"])
    
    with tab1:
        st.subheader("Geographic Distribution")
        
        if not filtered_df.empty:
            map_fig = create_map_visualization(filtered_df)
            st.plotly_chart(map_fig, use_container_width="stretch")
            
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
                    use_container_width="stretch"
                )
            
            with col2:
                fig = px.pie(
                    values=country_counts.values,
                    names=country_counts.index,
                    title="Distribution by Country"
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(showlegend=False, height=300)
                st.plotly_chart(fig, use_container_width="stretch")
        else:
            st.info("No data available for selected filters")
    
    with tab2:
        st.subheader("Temporal Trends")
        
        if not filtered_df.empty and 'PostedDate_parsed' in filtered_df.columns:
            # Timeline chart
            timeline_fig = create_timeline_chart(filtered_df)
            st.plotly_chart(timeline_fig, use_container_width="stretch")
            
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
                use_container_width="stretch"
            )
        else:
            st.info("No temporal data available")
    
    with tab3:
        st.subheader("Agency Analysis")
        
        if not filtered_df.empty:
            # Agency chart
            agency_fig = create_agency_chart(filtered_df)
            st.plotly_chart(agency_fig, use_container_width="stretch")
            
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
                use_container_width="stretch"
            )
        else:
            st.info("No agency data available")
    
    with tab4:
        st.subheader("Detailed Contract Data")
        
        if not filtered_df.empty:
            # Display controls
            col1, col2 = st.columns([3, 1])
            
            with col1:
                search_term = st.text_input(
                    "Search in titles and descriptions",
                    placeholder="Enter keywords..."
                )
            
            with col2:
                show_count = st.selectbox(
                    "Show rows",
                    [25, 50, 100, 200],
                    index=1
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
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"sam_africa_contracts_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
        else:
            st.info("No data available for selected filters")
    
    # Footer
    st.divider()
    
    with st.expander("📊 System Information"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **Database Location:** `{system.config.db_path}`  
            **Last Update:** {stats.get('last_update', 'Unknown')}  
            **Total Records:** {stats['total_records']:,}  
            **Database Size:** {stats['size_mb']:.1f} MB
            """)
        
        with col2:
            st.markdown("""
            **Top 5 Countries:**
            """)
            for country, count in list(stats['by_country'].items())[:5]:
                st.markdown(f"- {country}: {count:,}")

if __name__ == "__main__":
    main()