# Importing the pandas library
import pandas as pd

# Defining the path to the CSV file
file_path = '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/ADMISSIONS.csv'

# Define the chunk size
chunk_size = 100

# Read the first chunk to inspect the columns
first_chunk = pd.read_csv(file_path, nrows=5)
print(first_chunk.columns)

# Now you should see the actual column names in the output
# Let's assume the actual column names are 'SUBJECT_ID', 'HADM_ID', 'ADMITTIME', 'DISCHTIME', 'DEATHTIME', 'ETHNICITY'

# Initialize an empty list to hold the chunks
chunks = []

# Read the CSV file in chunks
for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Reset index and select specific columns
    chunk = chunk.reset_index()
    chunk = chunk[['HOSPITAL_ID', 'SUBJECT_ID', 'HADM_ID', 'ADMITTIME', 'DISCHTIME', 'DEATHTIME'
       #'ADMISSION_TYPE', 'ADMISSION_LOCATION', 'DISCHARGE_LOCATION',
       #'INSURANCE', 'LANGUAGE', 'MARITAL_STATUS', 'ETHNICITY', 'EDREGTIME',
       #'EDOUTTIME', 'HOSPITAL_EXPIRE_FLAG', 'ICU_EXPIRE_FLAG', 'NATIONALITY',
       ]]
    
    # Convert date columns to datetime format
    #chunk['ADMITTIME'] = pd.to_datetime(chunk['ADMITTIME'])
    #chunk['DISCHTIME'] = pd.to_datetime(chunk['DISCHTIME'])
    #chunk['DEATHTIME'] = pd.to_datetime(chunk['DEATHTIME'])

    # Filter rows where DEATHTIME is not null
    chunk = chunk[chunk['DEATHTIME'].notnull()]
    
    # Append the processed chunk to the list
    chunks.append(chunk)

# Concatenate all chunks into a single DataFrame
admits = pd.concat(chunks, ignore_index=True)

    

# Display the first few rows of the combined DataFrame
print(admits.head())

# Count the number of admissions
admissions_count = len(admits)
print(f"Count of admissions: {admissions_count}")

###################

Index(['SUBJECT_ID', 'HADM_ID', 'ADMITTIME', 'DISCHTIME', 'DEATHTIME',
       'ADMISSION_TYPE', 'ADMISSION_LOCATION', 'DISCHARGE_LOCATION',
       'INSURANCE', 'LANGUAGE', 'MARITAL_STATUS', 'ETHNICITY', 'EDREGTIME',
       'EDOUTTIME', 'HOSPITAL_EXPIRE_FLAG', 'ICU_EXPIRE_FLAG', 'NATIONALITY',
       'HOSPITAL_ID'],
      dtype='object')

   HOSPITAL_ID   SUBJECT_ID                         HADM_ID  \
0            1  ******87730  00110900087730929067646F0B6970   
1            1  ******45254  0011090004525405B16CCBEA059CA7   
2            1  ******84997  001109000849975B4FDBB5E8DC6C99   
3            1  ******70234  00110900070234D9CACCEFB4945C86   
4            1  ******83970  001109000839708338CC3AE5B3CAAF   

             ADMITTIME            DISCHTIME            DEATHTIME  
0  2309-07-07T16:06:52  2309-08-11T09:46:00  2309-08-11T09:37:00  
1  2957-02-20T15:36:39  2957-03-08T15:23:00  2957-03-08T14:31:00  
2  2531-06-21T17:13:15  2531-06-25T08:53:00  2531-06-25T02:34:00  
3  2597-06-07T00:01:51  2597-06-08T15:05:00  2597-06-08T10:08:00  
4  2289-08-22T12:59:35  2289-08-22T16:44:00  2289-08-22T16:47:00  
Count of admissions: 47
