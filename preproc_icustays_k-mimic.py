# Importing the pandas library
import pandas as pd

# Defining the path to the CSV file
file_paths = [
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/434/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/435/ICUSTAYS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/433/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/434/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/435/ICUSTAYS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/433/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/434/ICUSTAYS.csv',
    #'/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/435/ADMISSIONS.csv', -- nothing
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/433/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/434/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/435/ICUSTAYS.csv',
    
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/433/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/434/ICUSTAYS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/435/ICUSTAYS.csv'
]

# Define the chunk size
chunk_size = 100

# Initialize an empty list to hold the chunks
all_chunks = []

# Define the datetime format (example: 'YYYY-MM-DD HH:MM:SS')
datetime_format = '%Y-%m-%d %H:%M:%S'

# Loop through each file path
for file_path in file_paths:
    # Extract the hospital ID from the file path
    hospital_id = file_path.split('/')[6]  
    
    # Read the CSV file in chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        
        # Reset index and select specific columns
        chunk = chunk.reset_index(drop=True)
        
        # Read the CSV file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Reset index and select specific columns
            chunk = chunk.reset_index(drop=True)
            
            chunk = chunk[['SUBJECT_ID', 'HADM_ID', 'STAY_ID', 'FIRST_CAREUNIT', 'LAST_CAREUNIT',
           'INTIME', 'OUTTIME', 'LOS', 'OP_FLAG']]
            
            # Convert date columns to datetime format
            chunk['INTIME']  = pd.to_datetime(chunk['INTIME'],  errors='coerce')
            chunk['OUTTIME'] = pd.to_datetime(chunk['OUTTIME'], errors='coerce')
                    
            # Filter rows where column is not null
            chunk = chunk[chunk['INTIME'].notnull()]
            chunk = chunk[chunk['OUTTIME'].notnull()]
    
            # Add the hospital_id column
            chunk['HOSPITAL_ID'] = hospital_id
    
    
            chunk = chunk[['HOSPITAL_ID', 'SUBJECT_ID', 'HADM_ID', 'STAY_ID', 'FIRST_CAREUNIT', 'LAST_CAREUNIT',
           'INTIME', 'OUTTIME', 'LOS', 'OP_FLAG']]
    
            
            # Append the processed chunk to the list
            all_chunks.append(chunk)

# Check if there are any chunks to concatenate
if all_chunks:
    # Concatenate all chunks into a single DataFrame
    icustayPats = pd.concat(all_chunks, ignore_index=True)
    
    # Display the first few rows of the combined DataFrame
    print(icustayPats.head())
    
    # Count the number of admissions
    admissions_count = len(icustayPats)
    print(f"Count of icu-stayed pats: {admissions_count}")
else:
    print("No data to concatenate")

####### samples #########

  HOSPITAL_ID   SUBJECT_ID                         HADM_ID   STAY_ID  \
0         001  ******15661  0011090001566186F03EA1584D6B6E  165817.0   
1         001  ******26180  00110900026180490AB95D2DE67366  166269.0   
2         001  ******27802  00110900027802441B7A3BDF3CC6E9  166332.0   
3         001  ******50963  001109000509633F4CCE317E7E256E  166311.0   
4         001  ******50426  00110900050426645106B006EBB604  166173.0   

  FIRST_CAREUNIT LAST_CAREUNIT INTIME             OUTTIME   LOS  OP_FLAG  
0            CCU           NaN    NaT 2026-10-07 14:05:00  0.04        0  
1          SICU2           052    NaT 2138-09-21 15:20:00  0.90        0  
2            CCU           NaN    NaT 2174-09-15 13:00:00  0.26        0  
3            CCU           NaN    NaT 2067-10-10 16:30:00  0.06        0  
4            CCU           092    NaT 2026-10-15 13:55:00  1.07        0  

Count of icu-stayed pats: 4724
