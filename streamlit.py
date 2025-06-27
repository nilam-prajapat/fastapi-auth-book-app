import streamlit as st
import requests

API_URL = "http://127.0.0.1:8000"  # FastAPI base URL

st.set_page_config(page_title="User Auth App", page_icon="🔐")

# Initialize login state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user_email" not in st.session_state:
    st.session_state.user_email = ""

st.title("📚 User Authentication")

# ---------- If Logged In, Show Welcome Page ----------
if st.session_state.logged_in:
    st.success(f"✅ Welcome to AI Learning Institute By Gourav, {st.session_state.user_email}!")
    if st.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.user_email = ""
        st.rerun()

# ---------- Else, Show Login/Register Tabs ----------
else:
    tab = st.sidebar.radio("Navigate", ["Register", "Login"])

    # ---------- Register Tab ----------
    if tab == "Register":
        st.subheader("🔐 Register")

        name = st.text_input("Name")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        address = st.text_area("Address")
        aadhar = st.text_input("Aadhar Number")

        if st.button("Register"):
            payload = {
                "email": email,
                "password": password,
                "name": name,
                "address": address,
                "aadhar": aadhar
            }

            try:
                res = requests.post(f"{API_URL}/register", json=payload)
                if res.status_code == 200:
                    st.success("✅ Registered successfully! You can now log in.")
                else:
                    st.error(f"❌ {res.json()['detail']}")
            except Exception as e:
                st.error(f"Server Error: {e}")

    # ---------- Login Tab ----------
    elif tab == "Login":
        st.subheader("🔓 Login")

        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            payload = {
                "email": email,
                "password": password
            }

            try:
                res = requests.post(f"{API_URL}/login", json=payload)
                if res.status_code == 200:
                    st.session_state.logged_in = True
                    st.session_state.user_email = email
                    st.rerun()
                else:
                    st.error(f"❌ {res.json()['detail']}")
            except Exception as e:
                st.error(f"Server Error: {e}")
