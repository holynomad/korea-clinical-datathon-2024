## MIMIC-IV and K-MIMIC (AST) short-term(time-shifted) histogram analysis v0.9.7 (w/ gpt-4o)

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

# Function to process and resample dataset
def process_data(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.lower()  # Convert all column names to lowercase
    df['charttime'] = pd.to_datetime(df['charttime'], errors='coerce')  # Coerce errors to NaT
    df.set_index('charttime', inplace=True)
    resampled_data = df[['valuenum']].resample('24H').mean().dropna()
    return resampled_data

# Load and process the MIMIC-IV dataset
file_path_mimic = 'AST.csv'
resampled_data_mimic = process_data(file_path_mimic)

# Load and process the K-MIMIC dataset
file_path_kmimic = 'K_AST.csv'
resampled_data_kmimic = process_data(file_path_kmimic)

# Define a common time range that spans both datasets
common_start = max(resampled_data_mimic.index.min(), resampled_data_kmimic.index.min())
common_end = min(resampled_data_mimic.index.max(), resampled_data_kmimic.index.max())
common_time_range = pd.date_range(start=common_start, end=common_end, freq='24H')

# Interpolate both datasets to the common time range
resampled_data_mimic = resampled_data_mimic.reindex(common_time_range).interpolate(method='linear').dropna()
resampled_data_kmimic = resampled_data_kmimic.reindex(common_time_range).interpolate(method='linear').dropna()

# Ensure there are no NaN or infinite values
resampled_data_mimic = resampled_data_mimic[np.isfinite(resampled_data_mimic['valuenum'])]
resampled_data_kmimic = resampled_data_kmimic[np.isfinite(resampled_data_kmimic['valuenum'])]

# Generate smooth curve data for MIMIC-IV
x_mimic = np.arange(len(resampled_data_mimic))
y_mimic = resampled_data_mimic['valuenum'].values
x_smooth_mimic = np.linspace(x_mimic.min(), x_mimic.max(), 300)
spl_mimic = make_interp_spline(x_mimic, y_mimic, k=3)
y_smooth_mimic = spl_mimic(x_smooth_mimic)
x_smooth_datetime_mimic = np.interp(x_smooth_mimic, x_mimic, resampled_data_mimic.index.astype(np.int64))

# Generate smooth curve data for K-MIMIC
x_kmimic = np.arange(len(resampled_data_kmimic))
y_kmimic = resampled_data_kmimic['valuenum'].values
x_smooth_kmimic = np.linspace(x_kmimic.min(), x_kmimic.max(), 300)
spl_kmimic = make_interp_spline(x_kmimic, y_kmimic, k=3)
y_smooth_kmimic = spl_kmimic(x_smooth_kmimic)
x_smooth_datetime_kmimic = np.interp(x_smooth_kmimic, x_kmimic, resampled_data_kmimic.index.astype(np.int64))

# Plot smooth curves with area fill for both datasets
plt.figure(figsize=(12, 8))

# Plot for MIMIC-IV
plt.plot(resampled_data_mimic.index, y_mimic, 'o', linestyle='dotted', color='gray', alpha=0.5, markersize=5, label='MIMIC-IV Original Data')
plt.plot(pd.to_datetime(x_smooth_datetime_mimic), y_smooth_mimic, label='MIMIC-IV Smooth Curve', color='cornflowerblue')
plt.fill_between(pd.to_datetime(x_smooth_datetime_mimic), y_smooth_mimic, alpha=0.3, color='cornflowerblue')

# Plot for K-MIMIC
plt.plot(resampled_data_kmimic.index, y_kmimic, 'o', linestyle='dotted', color='lightgray', alpha=0.5, markersize=5, label='K-MIMIC Original Data')
plt.plot(pd.to_datetime(x_smooth_datetime_kmimic), y_smooth_kmimic, label='K-MIMIC Smooth Curve', color='lightcoral')
plt.fill_between(pd.to_datetime(x_smooth_datetime_kmimic), y_smooth_kmimic, alpha=0.3, color='lightcoral')

plt.title('24-Hour Aggregated (time-shifted) AST Area Chart Comparison between MIMIC-IV and K-MIMIC')
plt.xlabel('Date')
plt.ylabel('Average Lab Value')
plt.grid(True)
plt.legend()
plt.show()

