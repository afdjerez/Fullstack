import cx_Oracle
import pandas as pd
import plotly.express as px
import os

def connect_to_oracle():
    DB_USER = "SYSTEM"
    DB_PASSWORD = "12345"
    DB_DSN = "localhost:1521/xe"
    return cx_Oracle.connect(DB_USER, DB_PASSWORD, DB_DSN)

def load_csv(file_path):
    return pd.read_csv(file_path, sep=";")

def migrate_data(df, connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM CANDIDATES WHERE 1 = 1")
    connection.commit()

    insert_sql = """
    INSERT INTO CANDIDATES (FIRST_NAME, LAST_NAME, EMAIL, APPLICATION_DATE, COUNTRY, YOE, SENIORITY, TECHNOLOGY, CODE_CHALLENGE_SCORE, TECHNICAL_INTERVIEW_SCORE)
    VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5, :6, :7, :8, :9, :10)
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_sql, (
            row["First Name"], row["Last Name"], row["Email"], row["Application Date"],
            row["Country"], row["YOE"], row["Seniority"], row["Technology"],
            row["Code Challenge Score"], row["Technical Interview Score"]
        ))
    
    connection.commit()
    cursor.close()

def fetch_data(connection):
    query = """
    SELECT TECHNOLOGY, EXTRACT(YEAR FROM APPLICATION_DATE) AS YEAR, SENIORITY, COUNTRY
    FROM CANDIDATES
    WHERE CODE_CHALLENGE_SCORE >= 7 AND TECHNICAL_INTERVIEW_SCORE >= 7
    """
    return pd.read_sql(query, con=connection)

def plot_hires_by_technology(df):
    tech_counts = df["TECHNOLOGY"].value_counts().reset_index()
    tech_counts.columns = ["TECHNOLOGY", "COUNT"]
    fig = px.pie(tech_counts, names="TECHNOLOGY", values="COUNT", title="HIRES BY TECHNOLOGY", hole=0.3)
    fig.show()

def plot_hires_by_year(df):
    year_counts = df["YEAR"].value_counts().sort_index()
    fig = px.bar(
        x=year_counts.values,
        y=year_counts.index,
        orientation='h',
        labels={"x": "NUMBER OF HIRES", "y": "YEAR"},
        title="HIRES BY YEAR",
        text=year_counts.values
    )
    fig.update_traces(marker_color='royalblue', textposition='outside')
    fig.show()

def plot_hires_by_seniority(df):
    seniority_counts = df["SENIORITY"].value_counts().reset_index()
    seniority_counts.columns = ["SENIORITY", "COUNT"]
    fig = px.bar(seniority_counts, x="SENIORITY", y="COUNT", title="HIRES BY SENIORITY", text="COUNT")
    fig.update_traces(marker_color='lightblue', textposition='outside')
    fig.show()

def plot_hires_by_country(df):
    country_df = df[df["COUNTRY"].isin(["USA", "Brazil", "Colombia", "Ecuador"])]
    country_grouped = country_df.groupby(["YEAR", "COUNTRY"]).size().reset_index(name="HIRES")
    fig = px.line(country_grouped, x="YEAR", y="HIRES", color="COUNTRY", markers=True, title="HIRES BY COUNTRY OVER THE YEARS")
    fig.show()

def main():
    
    
    csv_file_path = os.path.abspath("input/candidates.csv")
    print('load data csv')
    df_csv = load_csv(csv_file_path)
    
    print('Creating conection to Oracle')
    connection = connect_to_oracle()
    
    print('Creating migration csv to Database oracle')
    migrate_data(df_csv, connection)
    df = fetch_data(connection)
    connection.close()
    
    print('Creating charts')
    plot_hires_by_technology(df)
    plot_hires_by_year(df)
    plot_hires_by_seniority(df)
    plot_hires_by_country(df)
    
    print("MIGRACIÓN Y VISUALIZACIONES COMPLETADAS EXITOSAMENTE.")

    

if __name__ == "__main__":
    main()
