import redis as r
import time
conexionRedis = r.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)
baseDatos = r.Redis(connection_pool=conexionRedis)


# 1- Crear registros clave-valor(0.25 puntos)
def creacionClaveValor():
    print("Tarea 1: Creación de Registros de Sensor (HSET)")

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

    print(f"Tarea 1 completada. Se crearon {baseDatos.dbsize} registros Hash.")
    return key_2, key_4, key_3

# 2 - Obtener y mostrar el número de claves registradas (0.25 puntos)
def numeroDeClaves():
    num_keys = baseDatos.dbsize()
    print(f"Resultado: Número total de claves registradas en Redis: {num_keys}")
    return num_keys

# 3 - Obtener y mostrar un registro en base a una clave (0.25 puntos)
def obtenerRegistroPorClave(clave):
    registro = baseDatos.hgetall(clave)
    print(registro)

# 4 - Actualizar el valor de una clave y mostrar el nuevo valor(0.25 puntos)
def actualizarRegistro(clave, fila, nuevoValor):
    print("Valor viejo: ", baseDatos.hgetall(clave))
    baseDatos.hset(clave, fila, nuevoValor)
    print("Valor nuevo: ", baseDatos.hgetall(clave))

# 5 - Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.25 puntos)
def eliminarRegistro(clave):
    print("Valor eliminado: ", baseDatos.hgetall(clave))
    baseDatos.delete(clave)

# 6 - Obtener y mostrar todas las claves guardadas (0.25 puntos)
def obtenerYMostrarClaves():
    print("Las claves registradas son: ", baseDatos.keys())

# 7 - Obtener y mostrar todos los valores guardados(0.25 puntos)
def obtenerYMostrarValores():
    valores = [baseDatos.hvals(clave) for clave in baseDatos.keys()]
    print("Los valores registrados son: ", valores)

# 8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)
def obtenerValoresConPatronComun8():
    claves = baseDatos.keys("registroSensor:2:17*")
    print("Valores con patron comun: ", [baseDatos.hgetall(clave) for clave in claves])

# 9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)
def obtenerRegistrosConPatronComun9():
    claves = baseDatos.keys("registroSensor:[1]:*")
    print("Valores con patron comun: ", [baseDatos.hgetall(clave) for clave in claves])

# 10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)
def obtenerRegistrosConPatronComun10():
    # Las ? lo que indican el numero de digitos de ese numero
    claves = baseDatos.keys("registroSensor:?:*")
    print("Valores con patron comun: ", [baseDatos.hgetall(clave) for clave in claves])

# 11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)
def obtenerRegistrosFiltrados():
    print("Valores filtrados por el sensor 2: ", [baseDatos.hgetall(clave) for clave in baseDatos.keys() if baseDatos.hgetall(clave)['id_sensor'] == "2"])

# 12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)
def actualizarRegistrosPorFiltro():
    claves = [clave for clave in baseDatos.keys() if baseDatos.hgetall(clave)["unidad"] == "dB"]
    print("Valores sin actualizar: ", [baseDatos.hgetall(clave) for clave in claves])
    for clave in claves:
        baseDatos.hset(clave,"valor",float(baseDatos.hgetall(clave)["valor"])+1)
    print("Valores actualizados: ", [baseDatos.hgetall(clave) for clave in claves])

# 13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)
def eliminarPorFiltro():
    claves = 



def main():
    print("1- Crear registros clave-valor(0.25 puntos)")
    claveParaVer, claveParaActualizar, key_to_delete = creacionClaveValor()
    print("2 - Obtener y mostrar el número de claves registradas (0.25 puntos)")
    numeroDeClaves()
    print("3 - Obtener y mostrar un registro en base a una clave (0.25 puntos)")
    obtenerRegistroPorClave(claveParaVer)
    print("4 - Actualizar el valor de una clave y mostrar el nuevo valor(0.25 puntos)")
    actualizarRegistro(claveParaActualizar, "valor", 23.5)
    print("5 - Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.25 puntos)")
    eliminarRegistro(key_to_delete)
    print("6 - Obtener y mostrar todas las claves guardadas (0.25 puntos)")
    obtenerYMostrarClaves()
    print("7 - Obtener y mostrar todos los valores guardados(0.25 puntos)")
    obtenerYMostrarValores()
    print("8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)")
    obtenerValoresConPatronComun8()
    print("9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)")
    obtenerRegistrosConPatronComun9()
    print("10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)")
    obtenerRegistrosConPatronComun10()
    print("11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)")
    obtenerRegistrosFiltrados()
    print("12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)")
    actualizarRegistrosPorFiltro()
    print("13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)")



baseDatos.close()