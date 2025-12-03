import psycopg2
import redis as r
from psycopg2.extras import RealDictCursor
import traceback
import sys

# --- Configuración PostgreSQL ---
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "negocio"
PG_USER = "user"
PG_PASSWORD = "user123"

# Tabla destino
PG_TABLE = "RegistroSensor"


def main():
    print("Ejercicio 22: Importar datos desde Redis → PostgreSQL\n")

    try:
        conexionRedis = r.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
        baseDatos = r.Redis(connection_pool=conexionRedis)
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor()

        claves = baseDatos.keys("registroSQL:*")

        print(f"Claves encontradas en Redis para importar: {len(claves)}\n")

        insertados = 0

        for clave in claves:
            data = baseDatos.hgetall(clave)

            if not data:
                continue

            id_sensor = data.get("id_sensor")
            fecha = data.get("fecha")
            valor = data.get("valor")
            unidad = data.get("unidad")

            cur.execute(f"SELECT id FROM {PG_TABLE} WHERE id = %s;", (data["id"],))
            existe = cur.fetchone()

            if existe:
                print(f"Registro con id={data['id']} ya existe. Saltado.")
                continue

            sql_insert = f"""
                INSERT INTO {PG_TABLE} (id, id_sensor, fecha, valor, unidad)
                VALUES (%s, %s, %s, %s, %s);
            """

            cur.execute(
                sql_insert,
                (data["id"], id_sensor, fecha, valor, unidad)
            )

            print(f"Insertado en PostgreSQL → ID={data['id']} | Sensor={id_sensor}")

            insertados += 1

        conn.commit()

        print(f"Total insertados en PostgreSQL: {insertados}")

    except Exception as e:
        print("ERROR al ejecutar el ejercicio 22.")
        print(e)
    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
