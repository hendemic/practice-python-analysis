from data_loader import DataLoader
import seaborn as sns
import seaborn.objects as so
import matplotlib.pyplot as plt
import json

def main():
    """
    Main function for Seattle public data analysis.
    """
    global df, fig, ax, column_def
    
    # Load column definitions from JSON file
    with open('column_definitions.json', 'r') as f:
        column_def = json.load(f)
    
    # Create DataLoader
    url = "https://data.seattle.gov/resource/ppi5-g2bj.json"
    loader = DataLoader(url, 20000)
    
    # Load data to df
    df = loader.load_data()
    
    # Use seattle accessor to define columns
    df = df.seattle.clean_data()
    df = df.seattle.define_columns(column_def)

    # Show top 5 officers with most entries
    print("Top 5 officers with most entries:")
    print(df[df['occured_date_time'].dt.year == 2024]['officer_id']
        .value_counts()
        .head(5))

    # fig, ax = plt.subplots(figsize=(10, 6))
    #sns.set_style("whitegrid")

    # Create seaborn objects plot

    plot = (so.Plot(
        df[df['occured_date_time'].dt.year == 2024],
        x='precinct')
        .add(so.Bar(), so.Count()))
    
    plot.show()

    print(df.head())



if __name__ == "__main__":
    main()

