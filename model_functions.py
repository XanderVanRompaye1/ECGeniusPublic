import os
import sys

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MultiLabelBinarizer
from tensorflow.python.keras.layers import *
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.models import load_model
from tensorflow.python.keras.utils.all_utils import normalize

import config


def train_model(session):
    # Create table tensions_diseases
    session.sql(f'CREATE OR REPLACE TABLE {config.table_tensions_diseases} ('
                f'tensions STRING, '
                f'diseases STRING)').collect()

    # Insert training data into table tensions_diseases
    session.sql(f'INSERT INTO {config.table_tensions_diseases} ('
                f'SELECT t1.tensions, t2.diseases '
                f'FROM ('
                f'SELECT l.patient_id, '
                f'LISTAGG(l.tension, \',\') WITHIN GROUP (ORDER BY l.timestamp) AS tensions '
                f'FROM {config.dim_lead} l GROUP BY l.patient_id) t1 '
                f'JOIN ('
                f'SELECT d.patient_id, '
                f'LISTAGG(d.disease_info_id, \',\') WITHIN GROUP (ORDER BY d.disease_info_id) AS diseases '
                f'FROM {config.dim_disease} d GROUP BY d.patient_id) t2 '
                f'ON t1.patient_id = t2.patient_id '
                f'ORDER BY t1.patient_id)').collect()

    # Get table tensions_diseases into a pandas dataframe
    train_data = session.table(config.table_tensions_diseases).to_pandas()

    # Data preprocessing
    tensions = normalize(train_data['TENSIONS'].str.split(',', expand=True).astype(int), axis=1)
    diseases = train_data['DISEASES'].str.split(',', expand=True).astype(float).fillna(0)

    # Extracting diseases as labels
    mlb = MultiLabelBinarizer()
    diseases = mlb.fit_transform(diseases.values)

    # Create table tensions_diseases
    session.sql(f'CREATE OR REPLACE TABLE {config.table_model_labels} ('
                f'label_id NUMBER(2, 0), '
                f'disease_info_id NUMBER(9, 0))').collect()

    label_id = 0
    for label in mlb.classes_:
        session.sql(
            f'INSERT INTO {config.table_model_labels} '
            f'(label_id, disease_info_id) '
            f'VALUES ({label_id}, {label})').collect()
        label_id += 1

    # Split the data into a train and test set for the tensions and diseases
    x_train, x_test, y_train, y_test = train_test_split(tensions, diseases, test_size=0.2, train_size=0.8, shuffle=True)

    # Create the model
    model = Sequential()
    model.add(InputLayer(input_shape=x_train.shape[1]))
    model.add(Dense(128, kernel_initializer='he_uniform', activation='relu'))
    model.add(Dense(64))
    model.add(Dense(y_train.shape[1], activation='sigmoid'))
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

    # Train the model
    history = model.fit(x_train, y_train, epochs=20, validation_data=(x_test, y_test))

    loss = history.history['loss'][-1]
    accuracy = history.history['accuracy'][-1]
    val_loss = history.history['val_loss'][-1]
    val_accuracy = history.history['val_accuracy'][-1]

    # Save the model to a Snowflake stage
    model_file = os.path.join('/tmp', 'my_model.h5')
    model.save(model_file)
    session.file.put(model_file, f'@{config.models_stage}', auto_compress=False, overwrite=True)

    return f'\tTraining results:\n' \
           f'\t\tLoss: {loss}\n' \
           f'\t\tAccuracy: {accuracy}\n' \
           f'\t\tVal loss: {val_loss}\n' \
           f'\t\tVal accuracy: {val_accuracy}'


def predict(session, tensions):
    # Convert the tensions string into a single row pandas dataframe
    tensions = list(map(int, tensions.split(',')))
    tensions = pd.DataFrame([tensions], columns=[i for i in range(0, 60000)])

    # Get the path of the model from the Snowflake stage
    import_dir = sys._xoptions['snowflake_import_directory']
    model_file = 'my_model.h5'
    model_path = import_dir + model_file

    # Load the model from the Snowflake stage
    model = load_model(model_path)
    predictions = (model.predict(tensions.iloc[:1])).astype('float64')[0]

    # Convert predictions to disease full names
    df = pd.DataFrame(columns=['DISEASE_INFO_ID', 'ACRONYM', 'FULL_NAME'])
    for i in range(1, predictions.size):
        if predictions[i] > 0.20:
            query = session.sql(f'SELECT di.disease_info_id, di.acronym, di.full_name '
                                f'FROM {config.table_model_labels} ml '
                                f'JOIN {config.dim_disease_info} di '
                                f'ON ml.disease_info_id = di.disease_info_id '
                                f'WHERE label_id = {i}')
            data = query.to_pandas()
            # data['PERCENTAGE'] = str(round((predictions[i] * 100).astype('float64'), 2)) + '%'
            row = [df, data]
            df = pd.concat(row, ignore_index=True)

    return df.to_json()


def predict_with_patient_id(session, patient_id):
    # Get the tensions string from dim_lead for the patient_id
    tensions = session.sql(f'SELECT LISTAGG(l.tension, \',\') WITHIN GROUP (ORDER BY l.timestamp) AS tensions '
                           f'FROM {config.dim_lead} l '
                           f'WHERE l.patient_id = \'{patient_id}\'').collect()[0][0]

    # Call the predict stored procedure
    predictions = session.sql(f'CALL predict(\'{tensions}\')').collect()[0][0]

    json = eval(predictions)

    df = pd.DataFrame(columns=['SNOMED CT', 'Acronym', 'Full name'])
    for disease_info_id in json['DISEASE_INFO_ID']:
        df.loc[len(df.index)] = [json['DISEASE_INFO_ID'][disease_info_id], json['ACRONYM'][disease_info_id],
                                 json['FULL_NAME'][disease_info_id]]

    return df
