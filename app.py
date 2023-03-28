import base64
import matplotlib.pyplot as plt
import scipy.io as sc
import streamlit as st
from PIL import Image
import config
import local_functions
import model_functions
import snowflake_functions

im = Image.open("./images/heart.png")
st.set_page_config(
    page_title="Arrhythmia study diagnosis",
    page_icon=im,
    layout='wide'
)

# Initialization
if 'signed_in' not in st.session_state:
    st.session_state['signed_in'] = False

if 'user_id' not in st.session_state:
    st.session_state['user_id'] = None

if 'file' not in st.session_state:
    st.session_state['file'] = None

if 'change' not in st.session_state:
    st.session_state['change'] = True

if 'button_disabled' not in st.session_state:
    st.session_state['button_disabled'] = False

if 'session' not in st.session_state:
    st.session_state['session'] = snowflake_functions.get_session(initialize=False)

if 'patient_id' not in st.session_state:
    st.session_state['patient_id'] = None

if 'current_tab' not in st.session_state:
    st.session_state['current_tab'] = 'Home'


def show_gif(gif):
    st.markdown(
        f'<img src="data:data:image/gif;base64,{gif}" alt="mushroom gif" width="50%">',
        unsafe_allow_html=True,
    )


def get_gif(location):
    mushroom_ = open(location, "rb")
    contents = mushroom_.read()
    gif = base64.b64encode(contents).decode("utf-8")
    mushroom_.close()
    return gif


def show_plot():
    # Change height and width of plot
    f = plt.figure()
    f.set_figwidth(15)
    f.set_figheight(5)
    plt.plot(lead)
    st.pyplot(plt)


def load_patient_ecg():
    st.session_state['patient_id'] = snowflake_functions.get_next_patient_id(st.session_state['session'])
    st.session_state['session'].sql(
        f'INSERT INTO {config.fact_patient} '
        f'VALUES (\'{st.session_state["patient_id"]}\', 1, \'X\')').collect()
    df = local_functions.convert_mat_to_df(st.session_state['file'], st.session_state['patient_id'])
    st.session_state['session'].write_pandas(df=df, table_name=config.dim_lead, overwrite=False)
    st.session_state["change"] = False
    snowflake_functions.add_patient_to_user(st.session_state['patient_id'], st.session_state['user_id'],
                                            st.session_state['session'])
    st.experimental_rerun()


def login_user(username, password):
    id = snowflake_functions.login(username, password, st.session_state['session'])
    if not (id is None or id == 0):
        st.session_state['signed_in'] = True
        st.session_state['user_id'] = id


def register_user(firstname, lastname, username, password):
    snowflake_functions.register(firstname, lastname, username, password, st.session_state['session'])
    login_user(username, password)


# Adding a sidebar
with st.sidebar:
    if st.session_state['signed_in']:
        Home = st.button("Homepage", use_container_width=True)
        if Home:
            if not st.session_state['button_disabled']:
                st.session_state['current_tab'] = 'Home'
        upload = st.button("Upload .mat file", use_container_width=True)
        if upload:
            if not st.session_state['button_disabled']:
                st.session_state['change'] = True
                st.session_state['file'] = None
                st.session_state['current_tab'] = 'Upload'
        patients = st.button("My patients", use_container_width=True)
        if patients:
            if not st.session_state['button_disabled']:
                st.session_state['change'] = True
                st.session_state['current_tab'] = 'Patients'

# Introduction on the Streamlit page.
st.title("""🫀 ECGenius 🫀""")

# When a file is uploaded we will calculate everything.

if not st.session_state['signed_in']:
    st.write("This is a web application made by three students of the KdG college, Antwerp, Belgium."
             "In this application you can upload a 12-lead ecg-scan of a person to get a prediction of the patient's diagnose.")

    col1, col2 = st.columns(2)

    with col1:
        firstname = st.text_input("Firstname:", placeholder="Firstname")
        lastname = st.text_input("Lastname:", placeholder="Lastname")
        username1 = st.text_input("Username:", placeholder="Username")
        password1 = st.text_input("Password:", type='password', placeholder="Password")
        register = st.button(label='Register')
        if register:
            with st.spinner('Registrating...'):
                register_user(firstname, lastname, username1, password1)
                if not st.session_state['signed_in']:
                    st.error('Username already exists')
                st.experimental_rerun()

    with col2:
        username2 = st.text_input("Username: ", placeholder="Username")
        password2 = st.text_input("Password: ", type='password', placeholder="Password")
        login = st.button(label='Login')
        if login:
            with st.spinner('Logging in...'):
                login_user(username2, password2)
                if not st.session_state['signed_in']:
                    st.error('Can not sign in')
                st.experimental_rerun()

else:
    if st.session_state['current_tab'] == 'Home':
        st.write("""

        **This is a project for "The Lab" at KdG College in Antwerp Belgium.**

        ## Info

        In this project we are going to use data about arrhythmia study. 
        The purpose of this project is to predict the diagnosis of the patient.

        The application is made in Python and the data is stored on a Snowflake data warehouse. The application will be running with Streamlit.

        """)

        gif = get_gif('images/ecg_scan_transparent.gif')
        show_gif(gif)

    if st.session_state['current_tab'] == 'Patients':
        patient_ids = snowflake_functions.get_all_patients(st.session_state['user_id'], st.session_state['session'])
        selected_id = st.selectbox(label="Select a patient to inspect:", options=patient_ids)
        if st.button("Inspect"):
            with st.spinner("loading patient..."):
                st.session_state['patient_id'] = selected_id
                st.session_state['file'] = snowflake_functions.get_tensions(st.session_state['patient_id'],
                                                                            st.session_state['session'])
                st.session_state['current_tab'] = 'Upload'
                st.session_state['button_disabled'] = True
                st.experimental_rerun()

    if st.session_state['current_tab'] == 'Upload':
        if st.session_state['file'] is None:
            file = st.file_uploader(accept_multiple_files=False,
                                    label="Upload your file to get a diagnosis.",
                                    type=['mat'])

            if file is not None:
                mat = sc.loadmat(file)
                st.session_state['file'] = mat['val']
                st.session_state['button_disabled'] = True
                st.experimental_rerun()

        else:
            if st.session_state['change']:
                with st.spinner("Loading .mat file..."):
                    load_patient_ecg()

            tab1, tab2, tab3 = st.tabs(["Patient", "Analyse", "Diagnose"])

            with tab1:
                st.session_state['button_disabled'] = False
                st.subheader("Please fill in some extra information about your patient: ")
                df = snowflake_functions.get_age_and_gender(st.session_state['patient_id'], st.session_state['session'])
                patient_age = df['AGE'][0]
                patient_gender = df['GENDER'][0]
                age = st.number_input(label="Age: ", min_value=1, max_value=120, value=patient_age)
                options = ['M', 'V', 'X']
                gender = st.selectbox(label='Gender: ', options=options, index=options.index(patient_gender))

                if st.button('Save'):
                    snowflake_functions.update_patient(age, gender, st.session_state["patient_id"],
                                                       st.session_state["session"])

            with tab2:
                st.subheader("Here are the plots of your patient.")
                # Making the plot
                time_series = st.session_state['file']

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    for lead in time_series[0:3]:
                        show_plot()
                with col2:
                    for lead in time_series[3:6]:
                        show_plot()
                with col3:
                    for lead in time_series[6:9]:
                        show_plot()
                with col4:
                    for lead in time_series[9:12]:
                        show_plot()

            with tab3:
                with st.spinner('Calculating diagnosis...'):
                    diagnoses = model_functions.predict_with_patient_id(st.session_state['session'],
                                                                        st.session_state['patient_id'])
                    if diagnoses.empty:
                        st.write('We have not found any diseases for this patient.')
                    else:
                        st.write('Following our calculations, your patient could have these diseases:')
                        st.dataframe(data=diagnoses, use_container_width=True)
                        st.session_state['button_disabled'] = False
