#data_cleaning_pipeline.py

import pandas as pd
import numpy as np
import re

class DataCleaningPipeline:
    def __init__(self):
        pass

    def data_imputer(self, df, columns):
        for column in columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce')
                mean_value = df[column].mean()
                df[column].fillna(mean_value, inplace=True)
            else:
                print(f"Column '{column}' does not exist in the DataFrame.")
        return df

    def clean_columns(self, df, columns):
        def remove_special_characters(value):
            if isinstance(value, str):
                return re.sub(r'[^A-Za-z0-9\s]', '', value)
            return value
        
        for column in columns:
            if column in df.columns:
                df[column] = df[column].apply(remove_special_characters)
            else:
                print(f"Column '{column}' does not exist in the DataFrame.")
        return df

    def to_date(self, df, columns):
        for column in columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            else:
                print(f"Column '{column}' does not exist in the DataFrame.")
        return df

    def convert_to_dummy_columns(self, df, column_names):
        if column_names:
            for col in column_names:
                if col not in df.columns:
                    raise ValueError(f"Column '{col}' not found in the DataFrame")
            df = pd.get_dummies(df, columns=column_names, drop_first=True)
        return df

    def to_numeric(self, df, columns):
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                print(f"Column '{col}' does not exist in the DataFrame.")
        return df

    def clean_data(self, df, sequence, params):
        """
        Clean the data by applying the functions in the specified sequence.

        Parameters:
        df (pd.DataFrame): The input DataFrame.
        sequence (list of str): List of function names in the order to be applied.
        params (dict): Dictionary of parameters for each function.

        Returns:
        pd.DataFrame: The cleaned DataFrame.
        """
        for func_name in sequence:
            if hasattr(self, func_name):
                df = getattr(self, func_name)(df, **params.get(func_name, {}))
            else:
                print(f"Function '{func_name}' not found in the pipeline.")
        return df