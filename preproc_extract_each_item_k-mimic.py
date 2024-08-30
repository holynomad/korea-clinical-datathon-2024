import pandas as pd

# Defining the paths to the CHARTEVENTS CSV files
file_paths = [
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/434/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/435/CHARTEVENTS.csv',

    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/433/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/434/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/435/CHARTEVENTS.csv',

    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/433/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/434/CHARTEVENTS.csv',

    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/433/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/434/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/435/CHARTEVENTS.csv',

    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/433/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/434/CHARTEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/435/CHARTEVENTS.csv'
]

# Define the chunk size
chunk_size = 100  # Number of rows per chunk
max_rows = 800000  # Maximum number of rows to read

# Initialize an empty list to store the processed chunks
all_chunks = []

# Iterate over each file path
for file_path in file_paths:
    # Extract the hospital ID from the file path
    hospital_id = file_path.split('/')[6] 
    
    # Initialize a counter to keep track of the total number of rows read per file
    rows_read = 0

    # Initialize an empty list to store the chunks for the current file
    chunks = []

    # Iterate over the CSV file in chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):

        # Add the hospital_id column
        chunk['HOSPITAL_ID'] = hospital_id
        
        # Check if the total number of rows read has reached the maximum limit
        if rows_read + len(chunk) > max_rows:
            # Calculate the number of rows needed to reach the maximum limit
            remaining_rows = max_rows - rows_read
            chunk = chunk.iloc[:remaining_rows]
            chunks.append(chunk)
            rows_read += len(chunk)
            break
        else:
            # Process the chunk
            chunks.append(chunk)
            rows_read += len(chunk)

        # Delete the chunk to free up memory
        del chunk

    # Combine the chunks of the current file into a single DataFrame
    df = pd.concat(chunks, ignore_index=True)

    # Append the DataFrame to the list of all chunks
    all_chunks.append(df)

# Combine all DataFrames into a single DataFrame
final_df = pd.concat(all_chunks, ignore_index=True)

# Print the columns to verify the column names
print(final_df.columns)

# Ensure the column 'ITEMID' is in the DataFrame
if 'ITEMID' in final_df.columns:
    
    # Filter rows where 'ITEMID' column contains specific values
    search_itemids = ['001C_1187_20765', '001C_1187_20770', '001C_1187_20775']
    sce = final_df[final_df['ITEMID'].astype(str).str.contains('|'.join(search_itemids), case=False, na=False)].copy()

    print(sce.head())
else:
    print("Column 'ITEMID' not found in the DataFrame")

# Free up memory by deleting the original DataFrame
del final_df

#############

Index(['CHARTEVENT_ID', 'SUBJECT_ID', 'HADM_ID', 'STAY_ID', 'CHARTTIME',
       'STORETIME', 'ITEMID', 'VALUE', 'VALUENUM', 'VALUEUOM', 'WARNING',
       'HOSPITAL_ID'],
      dtype='object')

       CHARTEVENT_ID   SUBJECT_ID                         HADM_ID   STAY_ID  \
1733  11001244676470  ******38639  001109000386394405A09C6048648A  164439.0   
1734  11001244676472  ******38639  001109000386394405A09C6048648A  164439.0   
1735  11001244676471  ******38639  001109000386394405A09C6048648A  164439.0   
1736  11001244681645  ******38639  001109000386394405A09C6048648A  164439.0   
1737  11001244681647  ******38639  001109000386394405A09C6048648A  164439.0   

                CHARTTIME            STORETIME           ITEMID VALUE  \
1733  2411-05-23T00:00:00  2411-05-23T00:14:50  001C_1187_20765     4   
1734  2411-05-23T00:00:00  2411-05-23T00:14:50  001C_1187_20770     5   
1735  2411-05-23T00:00:00  2411-05-23T00:14:50  001C_1187_20775     E   
1736  2411-05-23T01:00:00  2411-05-23T00:51:35  001C_1187_20765     4   
1737  2411-05-23T01:00:00  2411-05-23T00:51:35  001C_1187_20770     5   

      VALUENUM VALUEUOM  WARNING HOSPITAL_ID  
1733       4.0      NaN        0         001  
1734       5.0      NaN        0         001  
1735       0.0      NaN        0         001  
1736       4.0      NaN        0         001  
1737       5.0      NaN        0         001  
