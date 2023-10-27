import pandas as pd
import requests
import psycopg2
from psycopg2 import OperationalError

def extract_data(name, email, address, country, item):
    extracted_data = {
        "name":name,
        "email":email,
        "address":address,
        "country":country,
        "item":item
    }
    return extracted_data

def transform_data(data):
    transformed_data = pd.DataFrame.from_dict([data])

    return transformed_data

def load_data(data):
    
    try:
        connection = psycopg2.connect(
            host="127.0.0.1",
            database="postgres",
            user="postgres",
            password="12345",
            port = "5432"
        )
        cursor = connection.cursor()
        connection.commit()
    except OperationalError as e:
        print("Error:", e)

    table_name = 'user_info'
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"

    cursor.execute(query)
    
    # Extract column names from the tuples and print them    
    data_tuples = list(zip(data['name'], data['email'], data['address'], data['country'], data['item']))    
    for data_tuple in data_tuples:
        cursor.execute("INSERT INTO user_info (name, email, address, country, item) VALUES (%s, %s, %s, %s, %s)", data_tuple)
        print("Data Tuples:", data_tuples) 
    
    #cursor.execute("INSERT INTO user_info (name, email, address, country, item) VALUES (%s, %s, %s, %s, %s)", data_tuples[0])
    connection.commit()

    cursor.close()
    connection.close()
    print("Data loaded successfully!")
    


def run_pipeline(name, email, address, country, item):

    extracted_data = extract_data(name, email, address, country, item)    
    transformed_data = transform_data(extracted_data)    
    load_data(transformed_data)

    return "ETL process completed successfully"
