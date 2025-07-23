from data_loader import DataLoader
import seaborn as sns
import seaborn.objects as so
import matplotlib.pyplot as plt
import json


def main():
    """
    Main function for Seattle public data analysis.
    Data is imported using DataLoader class, with json denoting appropriate API for data and column types + new names
    """

    #Define global variables to allow access in ipython
    global df, fig, ax, column_def

    #--- IMPORT & CLEAN DATA ---
    dataset = 'datasets/police_uof.json'
    loader = DataLoader(dataset,20000,'seattle')
    df = loader.load_and_process()


    # %% Show top 5 officers with most entries
    print("Top 5 officers with most entries:")
    print(df[df['occurred_date_time'].dt.year == 2024]['officer_id']
        .value_counts()
        .head(5))

    df.info()
    df.incident_type.cat.categories

    sns.set_palette("pastel")
    sns.countplot(
        y="subject_race",
        data=df.query("incident_type == 'Level 3 - OIS' and occurred_date_time.dt.year == 2018"),
        color="b")
    plt.show()

    # %% Create a line plot showing Level 3 - OIS incidents by year and subject race
    ois_data = df.query("incident_type == 'Level 3 - OIS'")
    ois_yearly = ois_data.groupby([ois_data['occurred_date_time'].dt.year, 'subject_race']).size().reset_index(name='count')
    ois_yearly = ois_yearly.rename(columns={'occurred_date_time': 'year'})

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=ois_yearly, x='year', y='count', hue='subject_race', marker='o')
    plt.title('Level 3 - OIS Incidents by Year and Subject Race')
    plt.xlabel('Year')
    plt.ylabel('Number of Incidents')
    plt.legend(title='Subject Race')
    plt.show()


if __name__ == "__main__":
    main()
