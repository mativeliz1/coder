import requests
import psycopg2
import pandas as pd
import json

# Credenciales de Redshift desde el archivo config JSON
with open("C:/redshift_config.json", 'r') as config_r:
    config = json.load(config_r)

# Clave de API desde el archivo config JSON
with open("C:/api_config.json", 'r') as config_a:
    ap_config = json.load(config_a)

api_key = ap_config["api_key"]

# Lista de símbolos de acciones
symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA","IBM", "NVDA", "NFLX", "AMD", "CRM"]

# Crear un DataFrame vacío para almacenar los datos
data_df = pd.DataFrame()

# Solicitudes para obtener datos de cada símbolo de acciones
for symbol in symbols:
    response = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}")
    data = response.json()
    time_series = data["Time Series (Daily)"]
    
    # Convertir los datos en un DataFrame
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'date', '1. open': 'open_value', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'}, inplace=True)
    df['symbol'] = symbol
    
    # Agregar los datos al DataFrame principal
    data_df = pd.concat([data_df, df], ignore_index=True)

# Conexión a Amazon Redshift
try:
    conn = psycopg2.connect(
        host=config["host"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        port=config["port"]
    )
    print("Conectado a Amazon Redshift exitosamente!")

except Exception as e:
    print("No se pudo conectar a Amazon Redshift.")
    print(e)

# Creación de la tabla en Amazon Redshift
create_table = """
CREATE TABLE IF NOT EXISTS data_historica (
    symbol VARCHAR(10),
    date DATE,
    open_value FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT,
    PRIMARY KEY (symbol, date)
);
"""

with conn.cursor() as cursor:
    cursor.execute(create_table)

# Insertar los datos en la tabla de Amazon Redshift desde el DataFrame
try:
    with conn.cursor() as cursor:  # Abre un nuevo cursor
        for index, row in data_df.iterrows():
            insert_query = """
            INSERT INTO data_historica (symbol, date, open_value, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (row['symbol'], row['date'], row['open_value'], row['high'], row['low'], row['close'], row['volume']))
    
    print("Datos insertados en Amazon Redshift.")

except Exception as e:
    print("Error al insertar los datos en Amazon Redshift.")
    print(e)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
conn.close()
