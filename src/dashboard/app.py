import sys
import streamlit as st
import pandas as pd

import os
import plotly.express as px # Keep plotly for potential future use or if other charts are added

st.set_page_config(page_title="JSearch Analytics", layout="wide")
st.title("JSearch Job Market Analytics")

st.markdown("This dashboard visualizes real-time job market data ingested via Python APIs, processed by Apache Flink, and stored in PostgreSQL.")

import sqlalchemy

def get_connection():
    host = os.environ.get("POSTGRES_HOST", "localhost")
    engine = sqlalchemy.create_engine(f"postgresql+pg8000://admin:password@{host}:5432/jobs_db")
    return engine.connect()

@st.cache_data(ttl=5) # Refresh every 5 seconds
def fetch_data():
    try:
        conn = get_connection()
        query = "SELECT * FROM jobs;"
        df = pd.read_sql(query, conn)
        conn.close()
        
        if 'date_posted' in df.columns and not df.empty:
            df['date_posted_dt'] = pd.to_datetime(df['date_posted'], errors='coerce', utc=True)
            df = df[df['date_posted_dt'].dt.year >= 2026].copy()
            df = df.drop(columns=['date_posted_dt'])
            
        return df
    except Exception as e:
        st.error(f"Failed to fetch data from PostgreSQL: {e}")
        return pd.DataFrame()

df = fetch_data()

if df.empty:
    st.info("No data available yet. Start the Flink pipeline and ingest some data.")
else:
    # Calculate metrics
    total_jobs = len(df)
    remote_jobs = len(df[df['remote'] == True])
    remote_percentage = (remote_jobs / total_jobs * 100) if total_jobs > 0 else 0

    col1, col2 = st.columns(2)
    col1.metric("Total Jobs Indexed", total_jobs)
    col2.metric("Remote Jobs", f"{remote_jobs} ({remote_percentage:.1f}%)")

    st.markdown("---")

    # Layout for charts
    row1_c1, row1_c2 = st.columns(2)

    with row1_c1:
        # Group by company
        st.subheader("Top Hiring Companies")
        top_companies = df['company'].value_counts().head(10).reset_index()
        top_companies.columns = ['company', 'count']
        fig1 = px.bar(top_companies, x='company', y='count', color='count', color_continuous_scale='Blues')
        st.plotly_chart(fig1, use_container_width=True)

    with row1_c2:
        # If the skills array is stored as a string, attempt to parse/count it
        st.subheader("Top Skills Requested")
        if 'skills' in df.columns:
            try:
                skill_list = []
                for s in df['skills'].dropna():
                    if isinstance(s, list):
                        skill_list.extend(s)
                    elif isinstance(s, str):
                        cleaned = s.strip('[]{}').replace("'", "").replace('"', "")
                        if cleaned:
                            parsed = [tag.strip() for tag in cleaned.split(',') if tag.strip()]
                            skill_list.extend(parsed)
                
                if skill_list:
                    skill_counts = pd.Series(skill_list).value_counts().reset_index()
                    skill_counts.columns = ['Skill', 'Count']
                    skill_counts = skill_counts.head(10)
                    fig2 = px.bar(skill_counts, x='Skill', y='Count', color='Count', color_continuous_scale='Greens')
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info("No skills data to display.")
            except Exception as e:
                st.warning(f"Could not parse skills for analytics: {e}")
        else:
            st.info("No 'skills' column found in the data.")
        
    st.subheader("Recent Job Postings")
    st.dataframe(df.sort_values(by="date_posted", ascending=False).head(20))
