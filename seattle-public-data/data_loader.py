from types import NoneType
import pandas as pd
import requests
from typing import Dict
import json
from panda_extenders import *

class DataLoader:
    """
    Object-oriented data loader for Seattle public data.
    """

    def __init__(self, config_path: str, limit: int = 50000, accessor_name: str = None):
        self.config_path = config_path
        self.url = None
        self.column_config = None
        self.limit = limit
        self.df = None
        self.accessor_name = accessor_name
        self._load_config()

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            self.url = config['api_url']
            self.column_config = config['columns']

        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at {self.config_path}")

    def load_data(self):
        """Load data from the API."""
        try:
            # Make a single request with the specified limit
            params = {'$limit': self.limit}
            response = requests.get(self.url, params=params)
            response.raise_for_status()

            data = response.json()

            if not data:
                print("No data found at the specified URL")
                self.df = pd.DataFrame()
                return self.df

            self.df = pd.DataFrame(data)
            print(f"Successfully loaded {len(self.df)} rows from {self.url}")

        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from API: {e}")
        except ValueError as e:
            raise ValueError(f"Failed to parse JSON response: {e}")

    def clean_data(self):
        """
        Cleans data using the panda extender data_clean function
        This was an intentional decision where cleaning data is available via loader logic,
        but the way a dataset is cleaned is defined in that datasets accessor class.
        """

        if self.df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        if self.accessor_name is not None:
            self.df = getattr(self.df, self.accessor_name).clean_data()


    def define_columns(self):
        """Rename columns and apply data types based on configuration."""
        if self.df is None:
            raise ValueError("No data loaded. Call load_data() first.")

        # Create column mapping and type dictionaries
        column_mapping = {}
        column_types = {}

        # CLAUDE GENERATED CODE NOT UNDERSTOOD---
        # Comments added by Claude: filter column_mapping to only include columns that exist in the DataFrame
        # Example: if column_mapping = {'occured_date_time': 'occurred_date_time', 'precinct': 'precinct', 'missing_col': 'renamed_col'}
        # and self.df.columns = ['occured_date_time', 'precinct', 'officer_id']
        # then existing_mapping = {'occured_date_time': 'occurred_date_time', 'precinct': 'precinct'}
        # This prevents errors from trying to rename columns that don't exist in the actual data
        for original_name, col_config in self.column_config.items():
            new_name = col_config.get('new_name', original_name)
            dtype = col_config['dtype']

            column_mapping[original_name] = new_name
            column_types[new_name] = dtype


        existing_mapping = {k: v for k, v in column_mapping.items()
                           if k in self.df.columns}

        if existing_mapping:
            self.df = self.df.rename(columns=existing_mapping)
            print(f"Renamed {len(existing_mapping)} columns")

        # ---END CLAUDE GENERATED CODE

        # Apply data types to renamed columns
        for col, dtype in column_types.items():
            if col in self.df.columns:
                if 'datetime' in dtype:
                    self.df[col] = pd.to_datetime(self.df[col])
                else:
                    self.df[col] = self.df[col].astype(dtype)

        print(f"Column types applied to {len(self.df.columns)} columns")

    def load_and_process(self) -> pd.DataFrame:
        """
        Load data, define columns, and clean in one step, outputting the dataframe.
        The intention is to use this once you've understood the data you're working with and have defined
        cleaning in the accessor class to allow cleaner code in the analysis file(s)
        """

        self.load_data()
        self.clean_data()
        self.define_columns()

        return self.df
