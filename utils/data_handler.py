import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_all_tables(conn):

    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    table_names = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return table_names

def fetch_data_to_dataframe(conn, query_dict):

    df_list= []
    for query in query_dict:
        df = pd.read_sql_query(query_dict[query], conn)

        #handle duplicated columns
        first_two_columns = list(df.columns[:2])
        first_row_values = df.iloc[0, :2].astype(str).tolist()
        if first_two_columns == first_row_values:
            # Drop the first row
            df = df.iloc[1:].reset_index(drop=True)

        df["source_table"] = query
        df["searchable_text"] = df.apply(lambda row: " ".join(map(str, row)), axis=1)
        df_list.append(df)


    return df_list

def clean_data(df_list):

    # Combine all DataFrames
    df = pd.concat(df_list, ignore_index=True)

    # List of date columns to process
    date_columns= ["datecreated","dateupdated", "datedebrepa","datefinrepa","datedeb","dateinterv"]
    image_cols = ["avant","droite","gauche", "arriere"]
    date_cols = ["datecrea","dateupda"]
    
    for col in df.columns:
        #rename some columns   
        if col in image_cols:
            new_name = f"image_{col}"
            df.rename(columns={col: new_name}, inplace=True)
        elif (col in date_cols) and col == "datecrea":
            df.rename(columns={col:"datecreated"}, inplace=True)
        elif (col in date_cols) and col == "dateupda":
            df.rename(columns={col:"dateupdated"}, inplace=True)

    for col in date_columns:
        if col in df.columns:
            # Split the date and time
            datetime_parsed = pd.to_datetime(df[col], errors='coerce')
            time_section = datetime_parsed.dt.strftime('%H:%M:%S')
            df[col] = datetime_parsed.dt.date.astype(str) 
            time_col = col.replace("date", "time") if col != "dateupdated" else "timeupdated"
            df[time_col] = time_section

    # Save the updated CSV
    # output_file_path = "updated_data.csv"
    # df.to_csv(output_file_path, index=False)
    return df


def main(db_user, db_password, db_host, db_port, db_name):
    conn = psycopg2.connect(
        database= db_name,
        user= db_user,
        password= db_password,
        host= db_host,
        port= db_port
    )

    query_dict = {}
    for table_name in get_all_tables(conn):
        query = f"SELECT * FROM {table_name}"
        query_dict[table_name] = query

    df_list = fetch_data_to_dataframe(conn, query_dict)
    conn.close()

    cleaned_df = clean_data(df_list[::-1])
    # print(cleaned_df.head(1))
    return cleaned_df



