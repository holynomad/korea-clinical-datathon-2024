# aggregate patients info for 5 medical centers (w/ gpt-4o)
# Importing the pandas library
import pandas as pd

# Defining the paths to the CSV files
file_paths = [
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/434/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/435/PATIENTS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/433/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/434/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/435/PATIENTS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/433/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/434/PATIENTS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/433/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/434/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/435/PATIENTS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/433/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/434/PATIENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/435/PATIENTS.csv'
]

# Define the chunk size
chunk_size = 100

# Initialize an empty list to hold the chunks
all_chunks = []

# Loop through each file path
for file_path in file_paths:
    # Extract the hospital ID from the file path
    hospital_id = file_path.split('/')[6]  
    
    # Read the CSV file in chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Reset index and select specific columns
        chunk = chunk.reset_index(drop=True)
        chunk = chunk[['SUBJECT_ID', 'SEX', 'ANCHOR_AGE', 'ANCHOR_YEAR', 'ANCHOR_YEAR_GROUP', 'DOD']]
        
        # Add the hospital_id column
        chunk['HOSPITAL_ID'] = hospital_id
        
        # Append the processed chunk to the list
        all_chunks.append(chunk)

# Concatenate all chunks into a single DataFrame
pats = pd.concat(all_chunks, ignore_index=True)

# Display the first few rows of the combined DataFrame
print(pats.head())

# Count the number of patients
pats_count = len(pats)
print(f"Count of patients: {pats_count}")

############ 

    SUBJECT_ID SEX ANCHOR_AGE  ANCHOR_YEAR ANCHOR_YEAR_GROUP  DOD HOSPITAL_ID
0  10900028075   F   73 years         2355         2022-2024  NaN         001
1  10900023253   M   61 years         2458         2021-2023  NaN         001
2  10900007563   M   64 years         2432         2022-2024  NaN         001
3  10900011021   M   54 years         2613         2021-2023  NaN         001
4  10900039519   M   83 years         2091         2021-2023  NaN         001
Count of patients: 3223
