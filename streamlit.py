import streamlit as st
import pandas as pd
import plotly.express as px
from decimal import Decimal

# --- PAGE CONFIG ---
st.set_page_config(page_title="🎓 Edulytics Dashboard", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global Styles */
    .stApp {
        background: linear-gradient(135deg, #0a0a23 0%, #1a1a3e 50%, #2d1b69 100%);
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Main title styling */
    h1 {
        color: white !important;
        text-align: center;
        font-weight: 600;
        font-size: 2.5rem !important;
        text-shadow: 0 0 20px rgba(0, 255, 255, 0.3);
        margin-bottom: 2rem;
    }
    
    /* Sidebar styling */
    .css-1d391kg, .css-1cypcdb {
        background: rgba(20, 20, 60, 0.8) !important;
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(0, 255, 255, 0.2);
        border-radius: 0 16px 16px 0;
    }
    
    /* Sidebar title */
    .css-1d391kg h1, .css-1cypcdb h1 {
        color: white !important;
        text-align: center !important;
        font-weight: 600 !important;
        text-shadow: 0 0 10px rgba(0, 255, 255, 0.5) !important;
        margin-bottom: 2rem !important;
        font-size: 1.5rem !important;
    }
    
    /* Section headers */
    h3 {
        color: white !important;
        font-weight: 600 !important;
        text-shadow: 0 0 10px rgba(0, 255, 255, 0.3) !important;
        margin-bottom: 1rem !important;
        font-size: 1.25rem !important;
    }
    
    /* Metric cards styling */
    [data-testid="metric-container"] {
        background: rgba(30, 30, 80, 0.8);
        backdrop-filter: blur(10px);
        border: 2px solid transparent;
        border-image: linear-gradient(90deg, #00ffff, #ff00ff, #ffff00, #00ffff) 1;
        border-image-slice: 1;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        text-align: center;
        margin-bottom: 1rem;
    }
    
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: white !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
        text-shadow: 0 0 10px rgba(0, 255, 255, 0.3);
    }
    
    [data-testid="metric-container"] [data-testid="metric-label"] {
        color: #a0a0ff !important;
        font-weight: 500 !important;
        font-size: 1rem !important;
    }
    
    /* Status metric specific styling */
    [data-testid="metric-container"]:has(.status-active) [data-testid="metric-value"],
    [data-testid="metric-container"]:has(.status-inactive) [data-testid="metric-value"] {
        font-size: 1.5rem !important;
    }
    
    /* Button styling */
    .stButton > button {
        background: transparent !important;
        border: 2px solid transparent !important;
        border-image: linear-gradient(135deg, #00ffff 0%, #ff00ff 100%) 1 !important;
        border-image-slice: 1 !important;
        border-radius: 8px !important;
        color: white !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 15px rgba(0, 255, 255, 0.2) !important;
        padding: 0.75rem 1.5rem !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(0, 255, 255, 0.4) !important;
        background: rgba(50, 50, 100, 0.5) !important;
    }
    
    /* Radio button styling */
    .stRadio > div {
        background: transparent;
        border-radius: 12px;
        padding: 1rem;
    }
    
    .stRadio > div > label {
        color: white !important;
        font-weight: 500 !important;
    }
    
    .stRadio > div > div > div > label {
        background: transparent !important;
        border: 2px solid transparent !important;
        border-image: linear-gradient(135deg, #00ffff 0%, #ff00ff 100%) 1 !important;
        border-image-slice: 1 !important;
        border-radius: 8px !important;
        padding: 0.75rem 1rem !important;
        margin: 0.5rem 0 !important;
        color: white !important;
        transition: all 0.3s ease !important;
        text-align: center;
    }
    
    .stRadio > div > div > div > label:hover {
        background: rgba(50, 50, 100, 0.5) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 15px rgba(0, 255, 255, 0.2) !important;
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        background-color: rgba(30, 30, 80, 0.8) !important;
        border: 2px solid transparent !important;
        border-image: linear-gradient(135deg, #00ffff 0%, #ff00ff 100%) 1 !important;
        border-image-slice: 1 !important;
        border-radius: 8px !important;
        color: white !important;
    }
    
    .stSelectbox > div > div > div {
        color: white !important;
    }
    
    /* Dataframe styling */
    .stDataFrame {
        background: rgba(20, 20, 60, 0.8) !important;
        backdrop-filter: blur(10px) !important;
        border: 2px solid transparent !important;
        border-image: linear-gradient(90deg, #00ffff, #ff00ff, #ffff00, #00ffff) 1 !important;
        border-image-slice: 1 !important;
        border-radius: 16px !important;
        padding: 1rem !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3) !important;
    }
    
    /* Plotly chart background */
    .js-plotly-plot .plotly .modebar {
        background: rgba(20, 20, 60, 0.8) !important;
    }
    
    /* Chart container */
    .element-container:has(.js-plotly-plot) {
        background: rgba(20, 20, 60, 0.8);
        backdrop-filter: blur(10px);
        border: 2px solid transparent;
        border-image: linear-gradient(90deg, #00ffff, #ff00ff, #ffff00, #00ffff) 1;
        border-image-slice: 1;
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    }
    
    /* Column styling */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
    }
    
    /* Text styling */
    .stMarkdown {
        color: white;
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        padding: 2rem;
        margin-top: 3rem;
        border-top: 1px solid rgba(0, 255, 255, 0.2);
        color: #a0a0ff;
    }
    
    /* Status styling */
    .status-active {
        color: #00ff88 !important;
        font-weight: 600 !important;
        text-shadow: 0 0 8px rgba(0, 255, 136, 0.5) !important;
    }
    
    .status-inactive {
        color: #ff6b6b !important;
        font-weight: 600 !important;
        text-shadow: 0 0 8px rgba(255, 107, 107, 0.5) !important;
    }
    
    /* Email and Location row styling */
    .profile-details {
        display: flex;
        justify-content: flex-start;
        gap: 2rem;
        margin-bottom: 1.5rem;
    }
    
    .profile-details p {
        margin: 0;
        color: white;
        font-size: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .profile-details p span {
        color: #a0a0ff;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR TITLE ---
st.sidebar.markdown("<h1>Edulytics Dashboard</h1>", unsafe_allow_html=True)

# --- MAIN TITLE ---
st.title("Learning Dashboard")

# --- REFRESH CONTROL ---
refresh = st.sidebar.button("Refresh")

# --- USER MODE TOGGLE ---
mode = st.sidebar.radio("View Mode", ["Student", "Instructor", "Leaderboard"])

# --- GET CONNECTION ---
conn = st.connection("snowflake")

# --- STUDENT MODE ---
if mode == "Student":
    @st.cache_data(show_spinner=False)
    def get_students():
        query = "SELECT STUDENT_ID, NAME FROM DIM_STUDENT WHERE STUDENT_IS_CURRENT = TRUE ORDER BY NAME"
        return conn.query(query)

    @st.cache_data(show_spinner=False)
    def get_student_profile(student_id):
        query = f"SELECT * FROM TABLE(GET_STUDENT_PROFILE({student_id}))"
        return conn.query(query)

    students_df = get_students() if not refresh else get_students.clear() or get_students()
    selected_name = st.sidebar.selectbox("👤 Select Student", students_df["NAME"])
    selected_id = students_df[students_df["NAME"] == selected_name]["STUDENT_ID"].iloc[0]
    profile_df = get_student_profile(selected_id) if not refresh else get_student_profile.clear() or get_student_profile(selected_id)

    st.markdown(f"### 👤 Student Profile: {selected_name}")
    # Email and Location row
    st.markdown(
        f"""
        <div class="profile-details">
            <p>📧 <span>{profile_df["EMAIL"].iloc[0]}</span></p>
            <p>🌍 <span>{profile_df["LOCATION"].iloc[0]}</span></p>
        </div>
        """,
        unsafe_allow_html=True
    )

    total_time = int(profile_df["TOTAL_TIME_SPENT"].fillna(0).sum())
    total_activities = int(profile_df["ACTIVITY_COUNT"].fillna(0).sum())
    avg_score = round(float(profile_df["ENGAGEMENT_SCORE"].fillna(0).mean()), 2)
    status = "Inactive" if (profile_df["STATUS"] == "Inactive").all() else "Active"

    col4, col5, col6, col7 = st.columns(4)
    col4.metric("Total Time", f"{total_time} min")
    col5.metric("Activities", total_activities)
    col6.metric("Engagement Score", avg_score)
    col7.markdown(
        f'<div><div data-testid="metric-label">Status</div><div data-testid="metric-value" class="status-{"active" if status == "Active" else "inactive"}">{status}</div></div>',
        unsafe_allow_html=True
    )

    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.markdown("### Time Spent per Course")
        time_chart = px.bar(
            profile_df,
            x="COURSE_NAME",
            y="TOTAL_TIME_SPENT",
            color="CATEGORY",
            title="Time Spent by Course",
            labels={"TOTAL_TIME_SPENT": "Minutes"},
            height=400,
            color_discrete_sequence=["#00ffff", "#ff00ff", "#ffff00", "#00ff88", "#ff6b6b"]
        )
        time_chart.update_layout(
            title_x=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            title_font_color="white",
            xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
        st.plotly_chart(time_chart, use_container_width=True)

    with col_right:
        st.markdown("### Engagement Score by Category")
        pie_data = profile_df.groupby("CATEGORY")["ENGAGEMENT_SCORE"].mean().reset_index()
        pie_chart = px.pie(
            pie_data,
            values="ENGAGEMENT_SCORE",
            names="CATEGORY",
            title="Avg. Engagement Score by Category",
            hole=0.4,
            color_discrete_sequence=["#ff6b6b", "#4ecdc4", "#45b7d1"]
        )
        pie_chart.update_layout(
            title_x=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            title_font_color="white",
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
        st.plotly_chart(pie_chart, use_container_width=True)

    st.markdown("### 📘 Enrolled Courses Detail")
    st.dataframe(
        profile_df[[
            "COURSE_NAME", "CATEGORY", "INSTRUCTOR", "COURSE_DURATION",
            "TOTAL_TIME_SPENT", "ENGAGEMENT_SCORE", "DAYS_INACTIVE", "STATUS"
        ]],
        use_container_width=True,
        height=420,
        column_config={
            "COURSE_NAME": "Course",
            "CATEGORY": "Category",
            "INSTRUCTOR": "Instructor",
            "COURSE_DURATION": "Duration",
            "TOTAL_TIME_SPENT": "Time Spent",
            "ENGAGEMENT_SCORE": "Engagement Score",
            "DAYS_INACTIVE": "Days Inactive",
            "STATUS": "Status"
        }
    )

# --- INSTRUCTOR MODE ---
elif mode == "Instructor":
    @st.cache_data(show_spinner=False)
    def get_instructors():
        query = "SELECT DISTINCT INSTRUCTOR FROM DIM_COURSE WHERE COURSE_IS_CURRENT = TRUE ORDER BY INSTRUCTOR"
        return conn.query(query)

    @st.cache_data(show_spinner=False)
    def get_instructor_profile(name):
        query = f"SELECT * FROM TABLE(GET_INSTRUCTOR_PROFILE('{name}'))"
        return conn.query(query)

    instructors_df = get_instructors() if not refresh else get_instructors.clear() or get_instructors()
    selected_instructor = st.sidebar.selectbox("👨‍🏫 Select Instructor", instructors_df["INSTRUCTOR"])
    profile_df = get_instructor_profile(selected_instructor) if not refresh else get_instructor_profile.clear() or get_instructor_profile(selected_instructor)

    st.markdown(f"### 👨‍🏫 Instructor Profile: {selected_instructor}")
    col1, col2, col3 = st.columns(3)
    col1.metric("📘 Total Courses", profile_df["COURSE_NAME"].nunique())
    col2.metric("👨‍🎓 Total Students", int(profile_df["TOTAL_STUDENTS"].sum()))
    col3.metric("⭐ Avg Engagement", round(float(profile_df["AVG_ENGAGEMENT_SCORE"].astype(float).mean()), 2))

    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.markdown("### Time Spent per Course")
        bar_chart = px.bar(
            profile_df,
            x="COURSE_NAME",
            y="TOTAL_TIME_SPENT",
            color="CATEGORY",
            title="Total Time Spent by Students (per Course)",
            height=400,
            color_discrete_sequence=["#00ffff", "#ff00ff", "#ffff00", "#00ff88"]
        )
        bar_chart.update_layout(
            title_x=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            title_font_color="white",
            xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
        st.plotly_chart(bar_chart, use_container_width=True)

    with col_right:
        st.markdown("### Engagement Score by Category")
        pie_data = profile_df.groupby("CATEGORY")["AVG_ENGAGEMENT_SCORE"].mean().reset_index()
        pie_chart = px.pie(
            pie_data,
            values="AVG_ENGAGEMENT_SCORE",
            names="CATEGORY",
            title="Avg. Engagement Score by Category",
            hole=0.4,
            color_discrete_sequence=["#ff6b6b", "#4ecdc4", "#45b7d1"]
        )
        pie_chart.update_layout(
            title_x=0.5,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            title_font_color="white",
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
        st.plotly_chart(pie_chart, use_container_width=True)

    st.markdown("### 📚 Course Engagement Summary")
    st.dataframe(
        profile_df[[
            "COURSE_NAME", "CATEGORY", "TOTAL_STUDENTS", 
            "AVG_ENGAGEMENT_SCORE", "TOTAL_TIME_SPENT", "ACTIVITY_COUNT"
        ]],
        use_container_width=True,
        height=420,
        column_config={
            "COURSE_NAME": "Course",
            "CATEGORY": "Category",
            "TOTAL_STUDENTS": "Total Students",
            "AVG_ENGAGEMENT_SCORE": "Avg Engagement Score",
            "TOTAL_TIME_SPENT": "Total Time Spent",
            "ACTIVITY_COUNT": "Activity Count"
        }
    )

# --- LEADERBOARD MODE ---
elif mode == "Leaderboard":
    @st.cache_data(show_spinner=False)
    def get_leaderboard_students():
        query = """
        SELECT S.NAME, S.LOCATION, 
               SUM(F.ENGAGEMENT_SCORE) AS TOTAL_SCORE,
               SUM(F.TOTAL_TIME_SPENT) AS TOTAL_TIME,
               SUM(F.ACTIVITY_COUNT) AS ACTIVITIES
        FROM FACT_ENGAGEMENT_SUMMARY F
        JOIN DIM_STUDENT S ON S.STUDENT_ID = F.STUDENT_ID
        WHERE S.STUDENT_IS_CURRENT = TRUE
        GROUP BY S.NAME, S.LOCATION
        ORDER BY TOTAL_SCORE DESC
        LIMIT 10
        """
        return conn.query(query)

    @st.cache_data(show_spinner=False)
    def get_leaderboard_instructors():
        query = "SELECT * FROM TABLE(GET_INSTRUCTOR_LEADERBOARD())"
        return conn.query(query)

    st.markdown("## 🏆 Leaderboard - Top Performers")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 👨‍🎓 Top Students")
        students_leaderboard = get_leaderboard_students() if not refresh else get_leaderboard_students.clear() or get_leaderboard_students()
        st.dataframe(
            students_leaderboard,
            use_container_width=True,
            column_config={
                "NAME": "Name",
                "LOCATION": "Location",
                "TOTAL_SCORE": "Total Score",
                "TOTAL_TIME": "Total Time",
                "ACTIVITIES": "Activities"
            }
        )

    with col2:
        st.markdown("### 👨‍🏫 Top Instructors")
        instructors_leaderboard = get_leaderboard_instructors() if not refresh else get_leaderboard_instructors.clear() or get_leaderboard_instructors()
        st.dataframe(
            instructors_leaderboard,
            use_container_width=True
        )

# --- FOOTER ---
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown(
    "<div class='footer'>© 2025 Edulytics. All rights reserved.</div>",
    unsafe_allow_html=True
)