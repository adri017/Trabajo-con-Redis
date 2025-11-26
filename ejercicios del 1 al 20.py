import redis as r
import time
import json
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
    claves = [clave for clave in baseDatos.keys() if baseDatos.hgetall(clave)["unidad"] == "C"]
    print("Valores sin eliminar: ", [baseDatos.hgetall(clave) for clave in claves])
    for clave in claves:
        baseDatos.delete(clave)
    print("Todos los valores menos los eliminados: ", [baseDatos.hvals(clave) for clave in baseDatos.keys()])

# 14 - Crear estructura JSON (array) de los datos a almacenar (0.5 puntos)
def crearEstructuraJsonArray():
    registros = [
        {"id_sensor": "1", "fecha": "2025-11-01 10:00:00", "valor": 75.2, "unidad": "dB"},
        {"id_sensor": "2", "fecha": "2025-11-01 10:01:00", "valor": 26.5, "unidad": "C"},
        {"id_sensor": "1", "fecha": "2025-11-01 10:02:00", "valor": 80.9, "unidad": "dB"},
        {"id_sensor": "3", "fecha": "2025-11-01 10:03:00", "valor": 55.0, "unidad": "dB"},
    ]
    json_str = json.dumps(registros)
    baseDatos.set("sensores:json", json_str)

    print("Json creado: ",registros)

# 15 - Filtrar por cada atributo de la estructura JSON anterior (0.5 puntos)
def filtrarJsonPorAtributo(atributo, valor):
    json_str = baseDatos.get("sensores:json")
    if not json_str:
        print("No existe 'sensores:json'.")
        return []
    registros = json.loads(json_str)
    resultado = []
    if atributo == "valor" and isinstance(valor, str) and ':' in valor:
        minv, maxv = valor.split(":")
        minv = float(minv); maxv = float(maxv)
        resultado = [r for r in registros if minv <= float(r.get("valor", 0)) <= maxv]
    else:
        for r in registros:
            if atributo not in r:
                continue
            try:
                if isinstance(r[atributo], (int, float)) or isinstance(valor, (int, float)):
                    if float(r[atributo]) == float(valor):
                        resultado.append(r)
                else:
                    if str(r[atributo]) == str(valor):
                        resultado.append(r)
            except Exception:
                if str(r[atributo]) == str(valor):
                    resultado.append(r)

    print(f"Filtrado por {atributo} = {valor}: {resultado}")

# 16 - Crear una lista en Redis (0.25 puntos)
def crearListaRedis():
    alertas = [
        {"alerta_id": "a1", "mensaje": "Nivel de ruido alto", "nivel": 80},
        {"alerta_id": "a2", "mensaje": "Temperatura alta", "nivel": 27},
        {"alerta_id": "a3", "mensaje": "Batería baja", "nivel": 15}
    ]
    baseDatos.delete("lista_alertas")
    for item in alertas:
        baseDatos.rpush("lista_alertas", json.dumps(item))
    print("Lista de redis: ", alertas)

# 17 - Obtener elementos de una lista con un filtro en concreto (0.5 puntos)
def obtenerElementosListaFiltrados(substring=None, key="lista_alertas"):
    longitud = baseDatos.llen(key)
    elementos = baseDatos.lrange(key, 0, longitud - 1)
    objetos = [json.loads(e) for e in elementos]
    if substring is None:
        resultado = objetos
    else:
        resultado = [o for o in objetos if substring.lower() in json.dumps(o).lower()]
    print(f"Elementos en '{key}' filtrados por '{substring}': {resultado}")

# 18 - Crear datos con índices (esquema con al menos 3 campos) (0.5 puntos)
def crearRegistrosIndexados():
    registros = [
        {"key": "registro_idx:1", "id_sensor": "1", "fecha": "2025-11-01 10:00", "valor": 75.2, "unidad": "dB"},
        {"key": "registro_idx:2", "id_sensor": "2", "fecha": "2025-11-01 10:01", "valor": 26.5, "unidad": "C"},
        {"key": "registro_idx:3", "id_sensor": "1", "fecha": "2025-11-01 10:02", "valor": 80.9, "unidad": "dB"},
        {"key": "registro_idx:4", "id_sensor": "3", "fecha": "2025-11-01 10:03", "valor": 55.0, "unidad": "dB"},
    ]
    baseDatos.delete("index:all")
    for r in registros:
        key = r["key"]
        baseDatos.hset(key, mapping={
            "id_sensor": r["id_sensor"],
            "fecha": r["fecha"],
            "valor": r["valor"],
            "unidad": r["unidad"]
        })
        baseDatos.sadd("index:all", key)
        baseDatos.sadd(f"index:id_sensor:{r['id_sensor']}", key)
        baseDatos.sadd(f"index:unidad:{r['unidad']}", key)
        try:
            baseDatos.zadd("zindex:valor", {key: float(r["valor"])})
        except Exception:
            baseDatos.zadd("zindex:valor", {key: float(r["valor"])})
    print("Registros indexados: ", [r["key"] for r in registros])

# 19 - Búsqueda con índices en base a un campo (0.5 puntos)
def buscarPorIndice(campo, valor):
    resultados = []
    if campo == "id_sensor":
        miembros = baseDatos.smembers(f"index:id_sensor:{valor}")
        resultados = [baseDatos.hgetall(m) for m in miembros]
    elif campo == "unidad":
        miembros = baseDatos.smembers(f"index:unidad:{valor}")
        resultados = [baseDatos.hgetall(m) for m in miembros]
    elif campo == "valor":
        if isinstance(valor, str) and ":" in valor:
            minv, maxv = valor.split(":")
            members = baseDatos.zrangebyscore("zindex:valor", float(minv), float(maxv))
            resultados = [baseDatos.hgetall(m) for m in members]
        else:
            score = float(valor)
            members = baseDatos.zrangebyscore("zindex:valor", score, score)
            resultados = [baseDatos.hgetall(m) for m in members]
    else:
        print("No se puede filtrar por este campo")
    print(f"Resultados búsqueda por índice {campo} = {valor}: {resultados}")

# 20 - Realiza un group by usando los índices (0.5 puntos)
def groupByIndice(campo):
    miembros = baseDatos.smembers("index:all")
    agrupacion = {}
    for m in miembros:
        h = baseDatos.hgetall(m)
        if not h:
            continue
        valor = h.get(campo, None)
        agrupacion[valor] = agrupacion.get(valor, 0) + 1
    print(f"Group by '{campo}': {agrupacion}")
    return agrupacion

# ------------------------

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
    eliminarPorFiltro()
    print("14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)")
    crearEstructuraJsonArray()
    print("15 - Realizar un filtro por cada atributo de la estructura JSON anterior (0.5 puntos)")
    filtrarJsonPorAtributo("unidad", "dB")
    print("16 - Crear una lista en Redis (0.25 puntos)")
    crearListaRedis()
    print("17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)")
    obtenerElementosListaFiltrados("Temperatura")
    print("18 - Crea datos con índices, definiendo un esquema de al menos tres campos (0.5 puntos)")
    crearRegistrosIndexados()
    print("19 - Realiza una búsqueda con índices en base a un campo(0.5 puntos")
    buscarPorIndice("unidad", "dB")
    print("20 - Realiza un group  by usando los índices(0.5 puntos)")
    groupByIndice("id_sensor")

if __name__ == '__main__':
    main()
    baseDatos.close()