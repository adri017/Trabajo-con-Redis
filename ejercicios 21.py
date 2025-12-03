import psycopg2
import redis as r
from psycopg2.extras import RealDictCursor
import traceback
import sys

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "negocio"
PG_USER = "user"
PG_PASSWORD = "user123"

REDIS_HOST = "redis"
REDIS_PORT = 6379

PG_TABLE = "RegistroSensor"

def main():
    print("Ejercicio 21")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)

        conexionRedis = r.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
        baseDatos = r.Redis(connection_pool=conexionRedis)
        sql = f"""
        SELECT id, id_sensor, fecha, valor, unidad
        FROM {PG_TABLE};
        """

        cur.execute(sql)
        rows = cur.fetchall()

        print(f"Total de filas recuperadas: {len(rows)}\n")

        for row in rows:
            registro_id = row["id"]

            redis_key = f"registroSQL:{registro_id}"

            mapping = {
                "id": str(row["id"]),
                "id_sensor": str(row["id_sensor"]),
                "fecha": str(row["fecha"]),
                "valor": str(row["valor"]),
                "unidad": row["unidad"]
            }

            baseDatos.hset(redis_key, mapping=mapping)

            baseDatos.sadd(f"indexSQL:sensor:{row['id_sensor']}", registro_id)
            baseDatos.sadd(f"indexSQL:unidad:{row['unidad']}", registro_id)

            print(f"Insertado en Redis: {redis_key} → {mapping}")

        print(f"Total de registros insertados en Redis: {len(rows)}")

    except Exception:
        print("ERROR: No se pudo ejecutar.")
        traceback.print_exc()
        sys.exit(1)

    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()