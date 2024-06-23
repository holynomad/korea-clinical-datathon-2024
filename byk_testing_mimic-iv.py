## load libraries
import pandas as pd

## Define file path
file_path = '/mnt/dataset/dataset-2064568781941768192/MIMIC4/2.2/icu/chartevents.csv.gz'

# Read the .csv.gz file
cevents = pd.read_csv(file_path, nrows = 10000)

print(cevents.head(10))

#####
subset_cevents = cevents[cevents['warning'] == 1.0]
print(subset_cevents.head(10))

#####
## Define file path
file_path = '/mnt/dataset/dataset-2064568781941768192/MIMIC4/2.2/icu/d_items.csv.gz'

#####
# Read the .csv.gz file
itemID = pd.read_csv(file_path, nrows = 1000)
itemID

#####
import csv
import gzip

# Specify the path to your .gz CSV file
file_path = '/mnt/dataset/dataset-2064568781941768192/MIMIC4/2.2/icu/d_items.csv.gz'

# Initialize row and column counters
row_count = 0
column_count = 0

# Open the .gz CSV file and count the rows and columns
with gzip.open(file_path, 'rt') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if row_count == 0:
            # Count the number of columns in the header row
            column_count = len(row)
        row_count += 1

# Print the dimensions
print(f"Dimensions of the CSV file: {row_count} rows, {column_count} columns")

#####
## Define file path
file_path = '/mnt/dataset/dataset-2064568781941768192/MIMIC4/2.2/icu/d_items.csv.gz'

# Read the .csv.gz file
itemID = pd.read_csv(file_path)

#####
itemID_search = itemID[itemID['label'].str.contains('heart rate', case=False, na=False)]
itemID_search

#####
subset_cevents = cevents[cevents['itemid'] == 220045]
subset_cevents.head(10)

#####
import pandas as pd
import matplotlib.pyplot as plt

df = cevents[cevents['itemid'] == 220045].copy()

# Convert the measure time to datetime format if it's not already
df['time'] = pd.to_datetime(df['charttime'])

df.head(10)

# Plot heart rate change over time for each patient
plt.figure(figsize=(14, 7))

# Group by patient ID and plot each patient's data
for hadm_id, group in df.groupby('hadm_id'):
    plt.plot(group['charttime'], group['value'], label=f'Patient {hadm_id}')

# Add labels and title
plt.xlabel('Measure Time')
plt.ylabel('Heart Rate (bpm)')
plt.title('Heart Rate Change Over Time Per Patient')
plt.legend(title='Patient ID')
plt.xticks(rotation=45)
plt.tight_layout()

# Show the plot
plt.show()

#####
df2 = df.copy()
df2.head()

#####
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Function to reset the date to 2000-01-01 but keep the time
#def reset_date_to_2000(dt):
#    return dt.replace(year=2000, month=1, day=1)

# Apply the function to the 'measure time' column
#df2['time'] = df2['time'].apply(reset_date_to_2000)


# Function to reset the date starting from 2000-01-01
def reset_dates(group):
    group = group.sort_values('time')  # Ensure the group is sorted by the original datetime
    start_date = pd.Timestamp('2000-01-01')
    group['new_date'] = start_date + pd.to_timedelta(group['time'].dt.date - group['time'].dt.date.min(), unit='D')
    group['new_measure_time'] = pd.to_datetime(group['new_date'].dt.date.astype(str) + ' ' + group['time'].dt.time.astype(str))
    return group

# Apply the function to each patient group
df2 = df2.groupby('hadm_id').apply(reset_dates)
df2.head(100)

#####
df2 = df2.rename(columns={'hadm_id': 'hadmID'})

# Plot heart rate change over time for each patient
plt.figure(figsize=(14, 7))

# Group by patient ID and plot each patient's data
for hadm_id, group in df2.groupby('hadm_id'):
    plt.plot(group['new_measure_time'], group['valuenum'], label=f'Patient {hadm_id}')

# Add labels and title
plt.xlabel('Measure Time')
plt.ylabel('Heart Rate (bpm)')
plt.title('Heart Rate Change Over Time Per Patient')
plt.legend(title='Patient ID')
plt.xticks(rotation=45)
plt.tight_layout()

# Show the plot
plt.show()

#####
df3 = df2[df2['warning'] == 1.0].copy()

# Plot heart rate change over time for each patient
plt.figure(figsize=(14, 7))

# Group by patient ID and plot each patient's data
for hadm_id, group in df3.groupby('hadm_id'):
    plt.plot(group['new_measure_time'], group['valuenum'], label=f'Patient {hadm_id}')

# Add labels and title
plt.xlabel('Measure Time')
plt.ylabel('Heart Rate (bpm)')
plt.title('Heart Rate Change Over Time Per Patient')
plt.legend(title='Patient ID')
plt.xticks(rotation=45)
plt.tight_layout()

# Show the plot
plt.show()

#####

