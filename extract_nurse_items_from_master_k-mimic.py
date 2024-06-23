##Optional
import pandas as pd

##Load file
## Define file path
#file_path = '/mnt/dataset/dataset-2064568781941768192/MIMIC4/2.2/icu/d_items.csv.gz'
file_path = '/mnt/dataset/dataset-2064568781941768192/K-MIMIC/EMR/001/433/D_ITEMS.csv'

# Output path for the result CSV file
#output_path = 'd_items_kmimic.csv'

# Read the .csv.gz file
itemID = pd.read_csv(file_path)

##Load relevant libraries
import pandas as pd
#!pip install matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

##Search for heart rate item
itemID_search = itemID[
    (itemID['LABEL'].str.contains(
        'RR|RESP|HR|HEART|SBP|DBP|BT|TEMP|SPO2|SATU|GCS|EYE|VERBAL|MOTOR', 
        case=False, na=False
    )) & 
    (itemID['ABBREVIATION'].notna())
    & (itemID['CATEGORY'].notna())
    & (~itemID['CATEGORY'].isin(['사지혈압', '순환기계']))
]

# Save the result to a CSV file
#itemID_search.to_csv(output_path, index=False)
itemID_search
