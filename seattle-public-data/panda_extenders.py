import pandas as pd


@pd.api.extensions.register_dataframe_accessor("seattle")
class SeattleAccessor:
    """
    Custom DataFrame accessor for Seattle API data operations.
    """

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def clean_data(self):
        """Basic data cleaning. Returns modified DataFrame."""
        df = self._obj.copy()
        original_shape = df.shape

        # Remove rows with missing values
        df = df.dropna()

        # CLAUDE GENERATED -- NEEDS REVIEW AND BETTER PANDAS KNOWLEDGE
        # Remove rows containing 'X', '0', '-', 'x' in ANY column
        bad_values = ['X', '0', '-', 'x', '']
        mask = ~df.astype(str).apply(lambda x: x.str.strip()).isin(bad_values).any(axis=1)
        df = df[mask]

        # --- END CLAUDE GENERATED

        print(f"Data cleaned. Shape: {original_shape} -> {df.shape}")
        return df
