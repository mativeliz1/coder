import requests
import psycopg2
import json

#Leer credenciales de Redshift desde el config JSON
with open("C:/redshift_config.json", 'r') as config_r:
    config = json.load(config_r)

# Leer la clave de API desde el config JSON
with open("C:/api_config.json", 'r') as config_a:
    ap_config = json.load(config_a)

api_key = ap_config["api_key"]

# Lista de símbolos de acciones
symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "FB", "NVDA", "NFLX", "AMD", "CRM"]

# Solicitudes para obtener datos de cada símbolo de acciones
for symbol in symbols:
    response = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}")
    data = response.json()
    time_series = data["Time Series (Daily)"]
 

# Conexión a Amazon Redshift (utiliza las credenciales desde el archivo de configuración)
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

#Creación de la tabla en Amazon Redshift
create_table = """
CREATE TABLE IF NOT EXISTS data_historica (
    id int,
    symbol varchar(10),
    date date,
    open_value float,
    high float,
    low float,
    close float,
    volume int
);
"""

with conn.cursor() as cursor:
    cursor.execute(create_table)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
conn.close()
