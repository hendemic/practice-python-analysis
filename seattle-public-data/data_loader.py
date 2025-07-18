import pandas as pd
import requests
from typing import Dict


@pd.api.extensions.register_dataframe_accessor("seattle")
class SeattleAccessor:
    """
    Custom DataFrame accessor for Seattle data operations.
    Usage: df.seattle.define_columns(column_types)
    """
    
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
    
    def define_columns(self, column_types: Dict[str, str]):
        """Define column data types. Returns modified DataFrame."""
        df = self._obj.copy()
        # Convert datetime columns first
        for col, dtype in column_types.items():
            if col in df.columns:
                if 'datetime' in dtype:
                    df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(dtype)
        print(f"Column types applied to {len(df.columns)} columns")
        return df
    
    def clean_data(self):
        """Basic data cleaning. Returns modified DataFrame."""
        df = self._obj.copy()
        original_shape = df.shape
        
        # Remove rows with ANY missing values
        df = df.dropna()
        
        # Convert all values to strings and strip whitespace for comparison
        # Remove rows containing 'X', '0', '-', 'x' in ANY column
        bad_values = ['X', '0', '-', 'x', '']
        mask = ~df.astype(str).apply(lambda x: x.str.strip()).isin(bad_values).any(axis=1)
        df = df[mask]
        
        print(f"Data cleaned. Shape: {original_shape} -> {df.shape}")
        return df

class DataLoader:
    """
    Object-oriented data loader for Seattle public data.
    """
    
    def __init__(self, url: str, limit: int = 50000):
        self.url = url
        self.limit = limit
        self.df = None
        self.column_types = {}
    
    def load_data(self) -> pd.DataFrame:
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
            return self.df
        
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch data from API: {e}")
        except ValueError as e:
            raise ValueError(f"Failed to parse JSON response: {e}")
    
    
    def load_and_process(self, column_types: Dict[str, str]) -> pd.DataFrame:
        """Load data, define columns, and clean in one step."""
        self.load_data()
        self.df = self.df.seattle.define_columns(column_types)
        self.df = self.df.seattle.clean_data()
        return self.df