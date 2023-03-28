import csv
import os
from pathlib import Path

import scipy.io as sc
import pandas as pd
from tqdm import tqdm
import contextlib

# URL to locate package concerning all the files around our project
# This package has to be on the same level as the project directory
BASE_PATH = '.\\..\\a-large-scale-12-lead-electrocardiogram-database-for-arrhythmia-study-1.0.0\\'


def convert_files_locally(mat_files, hea_files):
    all_file_paths = get_all_file_paths()
    if hea_files:
        convert_all_hea_files_to_csv(all_file_paths)
    if mat_files:
        convert_all_mat_files_to_csv(all_file_paths)


# Reading the record file and getting all the different filenames (without extension)
def get_all_file_paths():
    paths = []
    with open(os.path.join(BASE_PATH, 'RECORDS'), 'r') as folder_record_paths:
        for folder_path in folder_record_paths:
            with open(os.path.join(BASE_PATH, folder_path.strip(), 'RECORDS')) as file_record_paths:
                for file_record_path in file_record_paths:
                    paths.append(os.path.join(BASE_PATH, folder_path.strip(), file_record_path.strip()))
    return paths


def convert_all_hea_files_to_csv(all_file_paths):
    # Create directory to save the csv-files
    os.makedirs('csv_files/', exist_ok=True)

    # Create fact_patient.csv and dim_disease.csv files
    with contextlib.ExitStack() as stack:
        csv_files = [stack.enter_context(open(f'csv_files/{filename}.csv', 'w', newline=''))
                     for filename in ['fact_patient', 'dim_disease']]
        csv_writers = [csv.writer(csv_file) for csv_file in csv_files]
        csv_writers[0].writerow(['PATIENT_ID', 'AGE', 'GENDER'])
        csv_writers[1].writerow(['PATIENT_ID', 'DISEASE_INFO_ID'])

        # Loop over all file paths
        for i in tqdm(range(len(all_file_paths)), desc='Converting .hea files to csv...'):
            write_hea_file_to_csv(all_file_paths[i], csv_writers[0], csv_writers[1])

    # csv files are automatically closed when the with-block is exited


def write_hea_file_to_csv(file_path, patient_writer, disease_writer):
    # Load .hea file
    with open(file_path + '.hea', 'r') as hea_file:
        # Declaration
        age, sex, gender, dxs = None, None, None, []

        # Looping all the lines
        for line in hea_file:
            # Collecting the age
            if line.startswith('#Age:'):
                age = line.split(' ', 1)[1].rstrip()
                if age == 'NaN':
                    age = None
            # Collecting the gender
            elif line.startswith('#Sex:'):
                sex = line.split(' ', 1)[1].rstrip()
                gender = 'M' if sex == 'Male' else 'F' if sex == 'Female' else 'X'
            # Collecting the diseases
            elif line.startswith('#Dx:'):
                dxs = [int(x) for x in line.split(' ', 1)[1].rstrip().split(',') if 9999999 < int(x) < 1000000000]

    # Collecting the patient_id
    patient_id = os.path.basename(file_path)

    # Writing each disease to the csv-file dim_disease.csv
    disease_writer.writerows([[patient_id, disease] for disease in dxs])

    # Writing the patient info to the csv-file fact_patient.csv
    patient_writer.writerow([patient_id, age, gender])


def convert_all_mat_files_to_csv(all_file_paths):
    # Create directory to save the csv-files
    Path('csv_files/dim_lead/').mkdir(parents=True, exist_ok=True)

    # Create 256 dim_lead_*.csv files and csv writers to those files
    num_files = 256
    csv_writers = []
    csv_files = []
    for i in range(num_files):
        file_path = f'csv_files/dim_lead/dim_lead_{i + 1:03d}.csv'
        csv_file = open(file_path, 'w', newline='')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['PATIENT_ID', 'TIMESTAMP', 'TENSION'])
        csv_files.append(csv_file)
        csv_writers.append(csv_writer)

    # Loop over all file paths and write all tensions to a csv-file dim_lead_*.csv
    for i in tqdm(range(len(all_file_paths)), f'Converting .mat files to csv...'):
        file_path = all_file_paths[i]
        writer_index = i % num_files
        write_mat_file_to_csv(file_path, csv_writers[writer_index])

    # Close all csv files
    for csv_file in csv_files:
        csv_file.close()


def write_mat_file_to_csv(file_path, writer):
    # Load .mat file and convert to a dictionary
    data_dict = sc.loadmat(f'{file_path}.mat')
    file_name = os.path.basename(file_path)
    time_series = data_dict['val']

    # Writing each tension to a csv-file dim_lead_*.csv
    for i in range(12):
        row_data = [(file_name, i * 5000 + j + 1, time_series[i][j]) for j in range(5000)]
        writer.writerows(row_data)


def convert_mat_to_df(uploaded_file, patient_id):
    # Extract the data from time_series using numpy

    data = []

    for i in range(12):
        for j in range(5000):
            data.append(uploaded_file[i][j])

    # Create the DataFrame
    df = pd.DataFrame({'PATIENT_ID': patient_id, 'TIMESTAMP': range(60000), 'TENSION': data})

    return df
