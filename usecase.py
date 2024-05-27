#usecase

import pandas as pd
from data_cleaning_pipeline import DataCleaningPipeline

# Example DataFrame
df = pd.DataFrame({
    'A': ['1', '2', '3', 'four'],
    'B': ['5', '6', 'seven', '8'],
    'C': [1, 2, 3, 4],
    'D': ['2020-01-01', '2020-02-01', 'invalid_date', '2020-04-01'],
    'E': ['a', 'b', 'c', 'd']
})

#Checking the dataset 

df.head()

df.info()

# Create an instance of DataCleaningPipeline
pipeline = DataCleaningPipeline()

# Define the sequence of functions to apply
sequence = [
    'clean_columns',
    'data_imputer',
    'to_date',
    'convert_to_dummy_columns',
    'to_numeric'
]

# Define parameters for each function
params = {
    'clean_columns': {'columns': ['E']},
    'data_imputer': {'columns': ['A', 'B']},
    'to_date': {'columns': ['D']},
    'convert_to_dummy_columns': {'column_names': []},  # No dummy columns to convert
    'to_numeric': {'columns': ['A', 'B']}
}

# Use the pipeline to clean the data
cleaned_df = pipeline.clean_data(df, sequence, params)

print(cleaned_df)




cleaned_df



cleaned_df.info()