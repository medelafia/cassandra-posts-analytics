import streamlit as st 
from threading import Thread 
from core.db_utils import init_database
import matplotlib.pyplot as plt

init_database(insert_data=False)

from core.db_utils import * 
from core.event_consuming import start_consuming 
from core.event_producer import start_producing

if "thread_started" not in st.session_state : 
    consuming_thread = Thread(target=start_consuming , daemon=True) 
    producing_thread = Thread(target=start_producing , daemon=True) 

    consuming_thread.start() 
    producing_thread.start()

    st.session_state.thread_started = True 

st.set_page_config(
    layout="wide"  # Use "wide" to reduce side margins
)

st.markdown(
    """
    <h1 style="text-align:center;">📊 Cassandra Real-Time Likes Dashboard</h1>
    <p style="text-align:center;color:gray;">
        Demonstrating high-write throughput & real-time analytics using Apache Cassandra
    </p>
    """,
    unsafe_allow_html=True
)

st.divider()


colors = ["#4e79a7" ,  '#f28e2b' , '#59a14f', '#e15759' , '#b07aa1']
kpi1, kpi2, kpi3 = st.columns(3)

with kpi1:
    st.metric("🔥 Total Likes", get_total_likes())

with kpi2:
    st.metric("🧑 Active Users", get_active_users())

with kpi3:
    st.metric("📝 Total Posts", get_total_posts())

st.divider()


col1 , col2  = st.columns(2)

with col1 : 
    container1 = st.container(border=True)
    with container1:
        most_liked_post = get_most_liked_post()
        print(most_liked_post)
        st.header("👍 Most Liked Posts")
        # Plot
        fig, ax = plt.subplots()
        ax.bar(most_liked_post["id"], most_liked_post["value"], color=colors)
        ax.set_xlabel("Post ID")
        ax.set_ylabel("Likes Count")
        ax.set_title("Bar Chart with Custom Colors")
        plt.xticks(rotation=45, ha="right")

        st.pyplot(fig)
    
    container2 = st.container(border=True)
    with container2 :
        st.header("📅📊 Top Posts Per Day")  
        day = st.date_input("select day : ")
        if day :
            most_liked_post_per_day = get_most_liked_post_per_day(day)

            fig, ax = plt.subplots()
            ax.bar(most_liked_post_per_day["id"], most_liked_post_per_day["value"], color=colors)
            ax.set_xlabel("Post ID")
            ax.set_ylabel("Likes count")
            ax.set_title("Bar Chart with Custom Colors")
            plt.xticks(rotation=45, ha="right")

            st.pyplot(fig)



with col2 :
    container3 = st.container(border=True)
    with container3 : 
        most_active_users = get_most_active_users() 
        st.header("👤📈 Most Active Users")

        fig, ax = plt.subplots()
        ax.bar(most_active_users["id"], most_active_users["value"], color=colors)
        ax.set_xlabel("User ID")
        ax.set_ylabel("Likes count")
        ax.set_title("Bar Chart with Custom Colors")
        plt.xticks(rotation=45, ha="right")

        st.pyplot(fig)       