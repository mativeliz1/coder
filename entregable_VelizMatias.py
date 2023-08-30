import requests
import psycopg2

# Configuración de la API Alpha Vantage
with open("C:\\api_key.txt",'r') as f1:
     api_key= f1.read()
symbol = "AAPL"
response = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}")
data = response.json()
time_series = data["Time Series (Daily)"]

# Conexión a Amazon Redshift
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="matiasveliz14_coderhouse"
with open("C:\pwd_coder.txt",'r') as f2:
     pwd= f2.read()

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Conectado a Amazon Redshift exitosamente!")
    
except Exception as e:
    print("No se pudo conectar a Amazon Redshift.")
    print(e)

#Creación de la tabla en Amazon Redshift
create_table = """
CREATE TABLE data_historica (
    id int,
    symbol varchar(10),
    date date,
    open_value float,
    high float,
    low float,
    close float,
    volume int );
"""

with conn.cursor() as cursor:
    cursor.execute(create_table)

# Confirmar los cambios en la base de datos
conn.commit()

# Cerrar la conexión
conn.close()