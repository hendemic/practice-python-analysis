from data_loader import DataLoader
import seaborn as sns
import seaborn.objects as so
import matplotlib.pyplot as plt
import json


def main():
    """
    Main function for Seattle public data analysis.
    """

    #Define global variables to allow access in ipython
    global df, fig, ax, column_def

    #--- IMPORT & CLEAN DATA ---
    # Use DataLoader class to:
    # - import
    # - clean (per seattle panda extender)
    # - rename and type columns
    # - And finally save to dataframe.
    dataset = 'datasets/police_uof.json'
    loader = DataLoader(dataset,20000,'seattle')
    df = loader.load_and_process()


    #--- ANALYSIS ---
    # Show top 5 officers with most entries
    print("Top 5 officers with most entries:")
    print(df[df['occurred_date_time'].dt.year == 2024]['officer_id']
        .value_counts()
        .head(5))



    # Test out seaborn objects plot
    plot = (so.Plot(
        df[df['occurred_date_time'].dt.year == 2024],
        x='precinct')
        .add(so.Bar(), so.Count()))

    plot.show()

if __name__ == "__main__":
    main()
