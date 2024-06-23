# Importing the pandas library
import pandas as pd

#item_path = '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/D_ITEMS.csv'


# Defining the path to the CSV file
file_paths = ['/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/CHARTEVENTS.csv',
              '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/434/CHARTEVENTS.csv']

# Define the chunk size
chunk_size = 100

# Initialize an empty list to hold the chunks
all_chunks = []

# Loop through each file path
for file_path in file_paths:
    # Read the CSV file in chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Reset index and select specific columns
        chunk = chunk.reset_index(drop=True)
        
        chunk = chunk[['CHARTEVENT_ID', 'SUBJECT_ID', 'HADM_ID', 'STAY_ID', 'CHARTTIME',
                       'STORETIME', 'ITEMID', 'VALUE', 'VALUENUM', 'VALUEUOM', 'WARNING']]
            
        # Merge with D_ITEMS on ITEMID
        merged_chunk = pd.merge(chunk, d_items[['ITEMID', 'LABEL']], on='ITEMID', how='left')
        
        # Filter rows where LABEL contains specific keywords
        merged_chunk = merged_chunk[merged_chunk['LABEL'].str.contains(
                        'RR|RESP|HR|HEART|SBP|DBP|BT|TEMP|SPO2|SATU|GCS|EYE|VERBAL|MOTOR', case=False, na=False)]
        
        # Append the processed chunk to the list
        all_chunks.append(merged_chunk)

# Concatenate all chunks into a single DataFrame
items = pd.concat(all_chunks, ignore_index=True)

print(items.head(50))
