import redis as r
import time
conexionRedis = r.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
baseDatos = r.Redis(connection_pool=conexionRedis)



def creacionClaveValor():
    print("Tarea 1: Creación de Registros de Sensor (HSET)")

    # Simulamos timestamps para las claves (únicas)
    ts_1 = int(time.time())
    ts_2 = ts_1 + 60
    ts_3 = ts_1 + 120
    ts_4 = ts_1 + 180
    
    registro_1 = {
        "id_sensor": "1",
        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_1)),
        "valor": "75.2",
        "unidad": "dB"
    }
    key_1 = f"registroSensor:1:{ts_1}"
    baseDatos.hset(key_1, mapping=registro_1)

    registro_2 = {
        "id_sensor": "2",
        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_2)),
        "valor": "26.5",
        "unidad": "C"
    }
    key_2 = f"registroSensor:2:{ts_2}"
    baseDatos.hset(key_2, mapping=registro_2)
    
    registro_3 = {
        "id_sensor": "1",
        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_3)),
        "valor": "80.9",
        "unidad": "dB"
    }
    key_3 = f"registroSensor:1:{ts_3}"
    baseDatos.hset(key_3, mapping=registro_3)

    registro_4 = {
        "id_sensor": "2",
        "fecha": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts_4)),
        "valor": "27.1", 
        "unidad": "C"
    }
    key_4 = f"registroSensor:2:{ts_4}"
    baseDatos.hset(key_4, mapping=registro_4)

    print("Tarea 1 completada. Se crearon 4 registros Hash.")
    return key_2, key_4, key_3

def numeroDeClaves():
    print("Tarea 2: Número de Claves Registradas (DBSIZE)")
    
    num_keys = baseDatos.dbsize()
    
    print(f"Resultado: Número total de claves registradas en Redis: {num_keys}")
    return num_keys

def obtenerRegistroPorClave(clave):
    registro = baseDatos.hgetall(clave)
    print(registro)

def actualizarRegistro(clave, fila, nuevoValor):
    print("Valor viejo: ", baseDatos.hgetall(clave))
    baseDatos.hset(clave, fila, nuevoValor)
    print("Valor nuevo: ", baseDatos.hgetall(clave))



claveParaVer, claveParaActualizar, key_to_delete = creacionClaveValor()
numeroDeClaves() 
obtenerRegistroPorClave(claveParaVer)
actualizarRegistro(claveParaActualizar, "valor", 23.5)

baseDatos.close()