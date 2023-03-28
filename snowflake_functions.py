from datetime import datetime
from snowflake.snowpark.session import Session
import config

# URL to locate package concerning all the files around our project
# This package has to be on the same level as the project directory
BASE_PATH = '.\\..\\a-large-scale-12-lead-electrocardiogram-database-for-arrhythmia-study-1.0.0\\'


def get_session(initialize):
    connection_parameters = {
        'account': config.account,
        'user': config.user,
        'password': config.password,
        'role': config.role,
        'warehouse': config.warehouse,
        'schema': config.schema
    }

    s = Session.builder.configs(connection_parameters).create()

    if initialize:
        initialize_session(s)
    else:
        use_session(s)

    return s


def initialize_session(session):
    # Create warehouses
    session.sql(f'CREATE OR REPLACE WAREHOUSE {config.warehouse_optimized} WITH '
                f'WAREHOUSE_TYPE = \'SNOWPARK-OPTIMIZED\' WAREHOUSE_SIZE = MEDIUM MAX_CONCURRENCY_LEVEL = 1').collect()

    session.sql(f'CREATE OR REPLACE WAREHOUSE {config.warehouse} WITH WAREHOUSE_SIZE = XSMALL').collect()

    # Create database
    session.sql(f'CREATE OR REPLACE DATABASE {config.database}').collect()

    # Create schema
    session.sql(f'CREATE OR REPLACE SCHEMA {config.schema}').collect()

    # Create stages
    create_stage(session, config.models_stage)
    create_stage(session, config.functions_stage)

    # Create stages with a custom file csv format
    create_stage_with_format_type(session, config.csv_format, 'CSV SKIP_HEADER = 1 COMPRESSION = GZIP',
                                  config.csv_stage)
    create_stage_with_format_type(session, config.csv_format, 'CSV SKIP_HEADER = 1 COMPRESSION = GZIP',
                                  config.csv_stage_lead)


def create_stage(session, stage_name):
    session.sql(f'CREATE OR REPLACE STAGE {stage_name}').collect()


def create_stage_with_format_type(session, format_name, format_type, stage_name):
    session.sql(f'CREATE OR REPLACE FILE FORMAT {format_name} '
                f'TYPE = {format_type}').collect()

    session.sql(f'CREATE OR REPLACE STAGE {stage_name} '
                f'FILE_FORMAT = {format_name}').collect()


def use_session(session):
    session.sql(f'USE WAREHOUSE {config.warehouse}').collect()

    session.sql(f'USE DATABASE {config.database}').collect()

    session.sql(f'USE SCHEMA {config.schema}').collect()


def use_warehouse(session, warehouse_name):
    session.sql(f'USE WAREHOUSE {warehouse_name}').collect()


def process_csv_files(upload, create_tables, copy, session):
    if upload:
        # Upload the csv-files to stages on Snowflake
        upload_all_files(session)

    if create_tables:
        # Create the tables on Snowflake
        create_all_tables(session)

    if copy:
        # Copy csv-files into tables using SnowSQL-commands
        copy_all_files_into_tables(session)


def upload_py_files(upload, session):
    if upload:
        upload_import_file(session, 'model_functions.py', config.functions_stage)
        upload_import_file(session, 'config.py', config.functions_stage)


def create_all_tables(session):
    create_fact_patient_table(session)
    create_dim_disease_info_table(session)
    create_dim_disease(session)
    create_dim_lead_table(session)
    create_table_user(session)
    create_table_patient_user(session)


def create_fact_patient_table(session):
    session.sql(f'CREATE OR REPLACE TABLE {config.fact_patient} ('
                f'patient_id VARCHAR(7) PRIMARY KEY, '
                f'age NUMBER(3, 0), '
                f'gender VARCHAR(1))').collect()


def create_dim_disease(session):
    session.sql(
        f'CREATE OR REPLACE TABLE {config.dim_disease} ('
        f'patient_id VARCHAR(7) REFERENCES {config.fact_patient} (patient_id), '
        f'disease_info_id NUMBER(9, 0) REFERENCES {config.dim_disease_info} (disease_info_id))').collect()


def create_dim_disease_info_table(session):
    session.sql(
        f'CREATE OR REPLACE TABLE {config.dim_disease_info} ('
        f'acronym VARCHAR(7), '
        f'full_name VARCHAR(50), '
        f'disease_info_id NUMBER(9, 0) PRIMARY KEY)').collect()


def create_dim_lead_table(session):
    session.sql(
        f'CREATE OR REPLACE TABLE {config.dim_lead} ('
        f'patient_id VARCHAR(7) REFERENCES {config.fact_patient} (patient_id), '
        f'timestamp NUMBER(5, 0), '
        f'tension NUMBER(5, 0))').collect()


def create_table_user(session):
    session.sql(
        f'CREATE OR REPLACE TABLE {config.table_user} ('
        f'user_id NUMBER NOT NULL AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY, '
        f'firstname VARCHAR, '
        f'lastname VARCHAR, '
        f'username VARCHAR UNIQUE, '
        f'password VARCHAR)').collect()


def create_table_patient_user(session):
    session.sql(
        f'CREATE OR REPLACE TABLE {config.table_patient_user} ('
        f'patient_id VARCHAR(7) REFERENCES {config.fact_patient} (patient_id), '
        f'user_id NUMBER REFERENCES {config.table_user} (user_id))'
    ).collect()


def upload_all_files(session):
    sql_statement = (
        f'PUT file://C:{BASE_PATH}ConditionNames_SNOMED-CT.csv '
        f'@{config.csv_stage} '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded ConditionNames_SNOMED-CT.csv")

    sql_statement = (
        f'PUT file://C:csv_files\\fact_patient.csv '
        f'@{config.csv_stage} '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded fact_patient.csv")

    sql_statement = (
        f'PUT file://C:csv_files\\dim_disease.csv '
        f'@{config.csv_stage} '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded dim_disease.csv")

    sql_statement = (
        f'PUT file://C:csv_files\\dim_lead\\dim_lead_0*.csv '
        f'@{config.csv_stage_lead} '
        f'PARALLEL = 50 '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded dim_lead_0*.csv")

    sql_statement = (
        f'PUT file://C:csv_files\\dim_lead\\dim_lead_1*.csv '
        f'@{config.csv_stage_lead} '
        f'PARALLEL = 50 '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded dim_lead_1*.csv")

    sql_statement = (
        f'PUT file://C:csv_files\\dim_lead\\dim_lead_2*.csv '
        f'@{config.csv_stage_lead} '
        f'PARALLEL = 56 '
        f'AUTO_COMPRESS = TRUE '
        f'SOURCE_COMPRESSION = NONE')

    execute_sql_statement_with_message(session, sql_statement, "Uploaded dim_lead_2*.csv")


def execute_sql_statement_with_message(session, sql_statement, message):
    start_time = datetime.now()
    result = session.sql(sql_statement).collect()
    end_time = datetime.now()
    time_delta = end_time - start_time
    print(f'{message} in {time_delta}')
    return result


def copy_all_files_into_tables(session):
    sql_statement = (
        f'COPY INTO {config.dim_disease_info} '
        f'FROM @{config.csv_stage}/ConditionNames_SNOMED-CT.csv.gz '
        f'FILE_FORMAT = (FORMAT_NAME = {config.csv_format})')

    execute_sql_statement_with_message(session, sql_statement, "Copied ConditionNames_SNOMED-CT.csv.gz in table")

    sql_statement = (
        f'COPY INTO {config.fact_patient} FROM ('
        f'SELECT $1, $2, $3 FROM @{config.csv_stage}/fact_patient.csv.gz) '
        f'FILE_FORMAT = (FORMAT_NAME = \'{config.csv_format}\')')

    execute_sql_statement_with_message(session, sql_statement, "Copied patient.csv.gz in table")

    sql_statement = (
        f'COPY INTO {config.dim_disease} FROM ('
        f'SELECT $1, $2 FROM @{config.csv_stage}/dim_disease.csv.gz) '
        f'FILE_FORMAT = (FORMAT_NAME = \'{config.csv_format}\')')

    execute_sql_statement_with_message(session, sql_statement, "Copied dim_disease.csv.gz in table")

    sql_statement = (
        f'COPY INTO {config.dim_lead} FROM ('
        f'SELECT $1, $2, $3 FROM @{config.csv_stage_lead}/) '
        f'FILE_FORMAT = (FORMAT_NAME = \'{config.csv_format}\')')

    execute_sql_statement_with_message(session, sql_statement, "Copied dim_lead_*.csv.gz in table")

    sql_statement = (f'DELETE FROM {config.dim_disease} '
                     f'WHERE disease_info_id NOT IN ('
                     f'SELECT disease_info_id FROM {config.dim_disease_info})')

    session.sql(sql_statement).collect()


def upload_import_file(session, file_path, stage):
    sql_statement = (
        f'PUT file://C:{file_path} '
        f'@{stage}/ '
        f'AUTO_COMPRESS = FALSE '
        f'SOURCE_COMPRESSION = NONE '
        f'OVERWRITE = TRUE')

    execute_sql_statement_with_message(session, sql_statement, f'Uploaded {file_path}')


def create_stored_procedure_train_model(session, module, function, return_type, parameter_names, parameter_types):
    sql_statement = f'CREATE OR REPLACE PROCEDURE {function}('

    for i in range(len(parameter_names)):
        if i != 0:
            sql_statement += ', '
        sql_statement += f'{parameter_names[i]} {parameter_types[i]}'

    sql_statement += f') ' \
                     f'RETURNS {return_type} ' \
                     f'LANGUAGE PYTHON ' \
                     f'RUNTIME_VERSION = \'3.8\' ' \
                     f'PACKAGES = (\'snowflake-snowpark-python\', \'pandas\', \'scikit-learn\', \'tensorflow\') ' \
                     f'imports = (\'@{config.functions_stage}/model_functions.py\', ' \
                     f'\'@{config.functions_stage}/config.py\') ' \
                     f'HANDLER = \'{module}.{function}\''

    execute_sql_statement_with_message(session, sql_statement, f'Created stored procedure {function}')


def create_stored_procedure_predict(session, module, function, return_type, parameter_names, parameter_types):
    sql_statement = f'CREATE OR REPLACE PROCEDURE {function}('

    for i in range(len(parameter_names)):
        if i != 0:
            sql_statement += ', '
        sql_statement += f'{parameter_names[i]} {parameter_types[i]}'

    sql_statement += f') ' \
                     f'RETURNS {return_type} ' \
                     f'LANGUAGE PYTHON ' \
                     f'RUNTIME_VERSION = \'3.8\' ' \
                     f'PACKAGES = (\'snowflake-snowpark-python\', \'pandas\', \'scikit-learn\', \'tensorflow\') ' \
                     f'imports = (\'@{config.functions_stage}/model_functions.py\', ' \
                     f'\'@{config.functions_stage}/config.py\', ' \
                     f'\'@{config.models_stage}/my_model.h5\') ' \
                     f'HANDLER = \'{module}.{function}\''

    execute_sql_statement_with_message(session, sql_statement, f'Created stored procedure {function}')


def call_procedure(session, function_name):
    sql_statement = (
        f'CALL '
        f'{function_name}()')

    return execute_sql_statement_with_message(session, sql_statement, f'Called stored procedure {function_name}')


def call_procedure_with_parameters(session, function_name, parameters):
    sql_statement = (
        f'CALL '
        f'{function_name}({parameters})')

    return execute_sql_statement_with_message(session, sql_statement, f'Called stored procedure {function_name}')


def get_next_patient_id(session):
    query = session.sql(
        f'SELECT MAX(patient_id) AS max FROM {config.fact_patient}'
    )
    df = query.to_pandas()
    id = df["MAX"].iloc[0]
    if id is None:
        id = 'JS1'
    id = str((int(id[2:]) + 1))
    return f'JS{id.zfill(5)}'


def login(username, password, session):
    query = session.sql(
        f'SELECT user_id AS id '
        f'FROM {config.table_user} WHERE '
        f'username = \'{username}\' '
        f'AND password = \'{password}\''
    )
    df = query.to_pandas()
    if len(df["ID"]) == 0:
        id = 0
    else:
        id = df["ID"].iloc[0]
    return id


def register(firstname, lastname, username, password, session):
    session.sql(
        f'INSERT INTO {config.table_user}(firstname, lastname, username, password) '
        f'VALUES(\'{firstname}\', \'{lastname}\', \'{username}\', \'{password}\')'
    ).collect()


def add_patient_to_user(patient_id, user_id, session):
    session.sql(f'INSERT INTO {config.table_patient_user} '
                f'VALUES (\'{patient_id}\', {user_id})').collect()


def get_all_patients(user_id, session):
    query = session.sql(f'SELECT patient_id AS id '
                        f'FROM {config.table_patient_user} '
                        f'WHERE user_id = {user_id}')

    data = []
    df = query.to_pandas()
    for i in range(len(df["ID"])):
        data.append(df["ID"].iloc[i])

    return data


def update_patient(age, gender, patient_id, session):
    session.sql(
        f'UPDATE {config.fact_patient} '
        f'SET age = {age}, gender = \'{gender}\' '
        f'WHERE patient_id = \'{patient_id}\'').collect()


def get_tensions(patient_id, session):
    query = session.sql(
        f'SELECT l.tension FROM {config.dim_lead} AS l WHERE patient_id = \'{patient_id}\' ORDER BY l.timestamp')
    df = query.to_pandas()
    data = []
    for i in range(12):
        lead = []
        for j in range(5000):
            lead.append(df["TENSION"].iloc[i * 5000 + j])
        data.append(lead)

    return data


def get_age_and_gender(patient_id, session):
    query = session.sql(f'SELECT age, gender FROM {config.fact_patient} WHERE patient_id = \'{patient_id}\'')
    return query.to_pandas()
