import config
import local_functions
import model_functions
import snowflake_functions

if __name__ == "__main__":
    # Convert all data to local csv-files
    local_functions.convert_files_locally(mat_files=False, hea_files=False)

    # Connect to snowflake
    session = snowflake_functions.get_session(initialize=False)

    # Upload, create tables and copy into all csv-files
    snowflake_functions.process_csv_files(upload=False, create_tables=False, copy=False, session=session)

    # Upload py-files to import in stored procedures
    snowflake_functions.upload_py_files(upload=False, session=session)

    # # Create stored procedure: train_model
    # snowflake_functions.create_stored_procedure_train_model(session, 'model_functions',
    #                                                         model_functions.train_model.__name__,
    #                                                         'STRING', [], [])
    #
    # # Change current warehouse to a Snowflake-optimized warehouse to train the model
    # snowflake_functions.use_warehouse(session, config.warehouse_optimized)
    #
    # # Call stored procedure train_model
    # print(snowflake_functions.call_procedure(session, model_functions.train_model.__name__)[0][0])
    #
    # # Change back to the normal warehouse
    # snowflake_functions.use_warehouse(session, config.warehouse)
    #
    # # Create stored procedure: predict
    # snowflake_functions.create_stored_procedure_predict(session, 'model_functions',
    #                                                     model_functions.predict.__name__,
    #                                                     'STRING', ['tensions'], ['STRING'])

    print(model_functions.predict_with_patient_id(session, 'JS00013'))
