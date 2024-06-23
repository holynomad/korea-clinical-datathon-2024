import pandas as pd

# Defining the paths to the CHARTEVENTS CSV files
file_paths = [
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/434/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/435/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/433/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/434/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/002/435/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/433/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/006/434/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/433/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/434/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/008/435/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/433/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/434/LABEVENTS.csv',
    '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/010/435/LABEVENTS.csv'
]

# Set pandas display options to avoid truncation
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

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

# Define the list of codes as a multi-line string in the local codes
raw_items = """
010L9307
010L3007
010L1102
010L8000A
008L30511
001L3092
00201L3095
00201L8186
"""

# Convert the multi-line string to a list, stripping any whitespace
formatted_items = raw_items.strip().split()

# Filter rows where 'ITEMID' column contains specific values
search_itemids = formatted_items

# Ensure the column 'ITEMID' is in the DataFrame
if 'ITEMID' in final_df.columns:
    
    # Make sure ITEMID is treated as a string
    final_df['ITEMID'] = final_df['ITEMID'].astype(str)
    
    # Subset only rows where 'ITEMID' exactly matches one of the search_itemids
    sce = final_df[final_df['ITEMID'].isin(search_itemids)].copy()
    
    # Group by ITEMID and count the occurrences
    itemid_counts = sce['ITEMID'].value_counts()

    # Print the counts for each ITEMID
    print(itemid_counts)
else:
    print("Column 'ITEMID' not found in the DataFrame")

# Free up memory by deleting the original DataFrame
del final_df
