#!/usr/bin/env python3
import csv
import json
import os

import pydgraph
#  SCHEMA
def set_schema(client):
    schema = """
    type Alumno {
        alumno.nombre
        alumno.genero
        alumno.edad
        alumno.ubicacion
        inscrito_en
        entrega
        presenta
        obtiene_certificado
    }

    type Maestro {
        maestro.nombre
        maestro.genero
        maestro.especialidad
        imparte
        califica_tarea
        califica_examen
    }

    type Curso {
        curso.nombre
        curso.categoria
        tiene_examenes
        tiene_tareas
    }

    type Tarea {
        tarea.id
        tarea.fecha_entrega
        tarea.calificacion
    }

    type Examen {
        examen.id
        examen.preguntas
        examen.calificacion
    }

    type Certificado {
        certificado.id
        certificado.fecha
        certificado.categoria
    }

    # Indices de texto
    alumno.nombre:    string  @index(exact) .
    alumno.genero:    string  .
    alumno.edad:      int     .
    alumno.ubicacion: geo     .

    maestro.nombre:      string @index(exact) .
    maestro.genero:      string .
    maestro.especialidad: string .

    curso.nombre:    string @index(exact) .
    curso.categoria: string .

    tarea.id:            string @index(exact) .
    tarea.fecha_entrega: datetime .
    tarea.calificacion:  float    @index(float) .

    examen.id:           string @index(exact) .
    examen.preguntas:    int    @index(int) .
    examen.calificacion: float  @index(float) .

    certificado.id:       string @index(exact) .
    certificado.fecha:    datetime .
    certificado.categoria: string .

    # Relaciones
    inscrito_en:         [uid] @reverse .
    imparte:             [uid] .
    entrega:             [uid] .
    presenta:            [uid] @reverse .
    tiene_examenes:      [uid] .
    tiene_tareas:        [uid] .
    obtiene_certificado: [uid] .
    califica_tarea:      [uid] .
    califica_examen:     [uid] .
    """
    return client.alter(pydgraph.Operation(schema=schema))

#CARGA DE DATOS DESDE CSV
def load_data_from_csv(client):
    base = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(base, "data")

    txn = client.txn()
    try:
        uid_map = {}   # nombre_clave -> uid temporal

        #Certificados
        certs = []
        with open(os.path.join(csv_dir, "certificados.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                certs.append({
                    "uid": uid_key,
                    "dgraph.type": "Certificado",
                    "certificado.id": row["id"],
                    "certificado.fecha": row["fecha"],
                    "certificado.categoria": row["categoria"],
                })
                uid_map[row["id"]] = uid_key

        #Tareas
        tareas = []
        with open(os.path.join(csv_dir, "tareas.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                tareas.append({
                    "uid": uid_key,
                    "dgraph.type": "Tarea",
                    "tarea.id": row["id"],
                    "tarea.fecha_entrega": row["fecha_entrega"],
                    "tarea.calificacion": float(row["calificacion"]),
                })
                uid_map[row["id"]] = uid_key

        #Examenes
        examenes = []
        with open(os.path.join(csv_dir, "examenes.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                examenes.append({
                    "uid": uid_key,
                    "dgraph.type": "Examen",
                    "examen.id": row["id"],
                    "examen.preguntas": int(row["preguntas"]),
                    "examen.calificacion": float(row["calificacion"]),
                })
                uid_map[row["id"]] = uid_key

        #Curso
        cursos_map = {} # nombre_curso -> uid_clave
        cursos = []
        with open(os.path.join(csv_dir, "cursos.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                examenes_ids = [e.strip() for e in row["examenes_ids"].split("|") if e.strip()]
                tareas_ids   = [t.strip() for t in row["tareas_ids"].split("|")   if t.strip()]
                curso_obj = {
                    "uid": uid_key,
                    "dgraph.type": "Curso",
                    "curso.nombre": row["nombre"],
                    "curso.categoria": row["categoria"],
                    "tiene_examenes": [{"uid": uid_map[e]} for e in examenes_ids if e in uid_map],
                    "tiene_tareas":   [{"uid": uid_map[t]} for t in tareas_ids   if t in uid_map],
                }
                cursos.append(curso_obj)
                uid_map[row["id"]] = uid_key
                cursos_map[row["nombre"]] = uid_key

        #Maestros
        maestros = []
        with open(os.path.join(csv_dir, "maestros.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                cursos_ids    = [c.strip() for c in row["cursos_ids"].split("|")    if c.strip()]
                cal_tareas    = [t.strip() for t in row["califica_tareas_ids"].split("|")  if t.strip()]
                cal_examenes  = [e.strip() for e in row["califica_examenes_ids"].split("|") if e.strip()]
                maestros.append({
                    "uid": uid_key,
                    "dgraph.type": "Maestro",
                    "maestro.nombre": row["nombre"],
                    "maestro.genero": row["genero"],
                    "maestro.especialidad": row["especialidad"],
                    "imparte":         [{"uid": uid_map[c]} for c in cursos_ids   if c in uid_map],
                    "califica_tarea":  [{"uid": uid_map[t]} for t in cal_tareas   if t in uid_map],
                    "califica_examen": [{"uid": uid_map[e]} for e in cal_examenes if e in uid_map],
                })
                uid_map[row["id"]] = uid_key

        #Alumnos
        alumnos = []
        with open(os.path.join(csv_dir, "alumnos.csv"), newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                uid_key = f"_:{row['id']}"
                cursos_ids   = [c.strip() for c in row["cursos_ids"].split("|")   if c.strip()]
                tareas_ids   = [t.strip() for t in row["tareas_ids"].split("|")   if t.strip()]
                examenes_ids = [e.strip() for e in row["examenes_ids"].split("|") if e.strip()]
                certs_ids    = [c.strip() for c in row["certs_ids"].split("|")    if c.strip()]

                alumno_obj = {
                    "uid": uid_key,
                    "dgraph.type": "Alumno",
                    "alumno.nombre": row["nombre"],
                    "alumno.genero": row["genero"],
                    "alumno.edad": int(row["edad"]),
                    "alumno.ubicacion": {
                        "type": "Point",
                        "coordinates": [float(row["lon"]), float(row["lat"])],
                    },
                    "inscrito_en":         [{"uid": uid_map[c]} for c in cursos_ids   if c in uid_map],
                    "entrega":             [{"uid": uid_map[t]} for t in tareas_ids   if t in uid_map],
                    "presenta":            [{"uid": uid_map[e]} for e in examenes_ids if e in uid_map],
                    "obtiene_certificado": [{"uid": uid_map[c]} for c in certs_ids   if c in uid_map],
                }
                alumnos.append(alumno_obj)

        all_objects = certs + tareas + examenes + cursos + maestros + alumnos
        response = txn.mutate(set_obj=all_objects)
        txn.commit()
        print(f"Datos cargados exitosamente. UIDs creados: {len(response.uids)}")
    finally:
        txn.discard()

#  QUERIES
def search_alumno(client, nombre):
    """Query 1: Buscar alumno por nombre (indice texto exacto)."""
    query = """query search($a: string) {
        alumno(func: eq(alumno.nombre, $a)) {
            uid
            alumno.nombre
            alumno.genero
            alumno.edad
            alumno.ubicacion
            inscrito_en {
                curso.nombre
                curso.categoria
            }
            presenta {
                examen.id
                examen.calificacion
            }
            obtiene_certificado {
                certificado.id
                certificado.categoria
            }
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$a": nombre})
    data = json.loads(res.json)
    resultados = data.get("alumno", [])
    print(f"\nAlumnos encontrados con nombre '{nombre}': {len(resultados)}")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def search_examenes_por_calificacion(client, min_cal):
    """Query 2: Buscar examenes con calificacion >= valor (indice numerico)."""
    query = """query search($cal: float) {
        examenes(func: ge(examen.calificacion, $cal)) {
            examen.id
            examen.preguntas
            examen.calificacion
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$cal": str(min_cal)})
    data = json.loads(res.json)
    resultados = data.get("examenes", [])
    print(f"\nExamenes con calificacion >= {min_cal}: {len(resultados)}")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def alumnos_por_curso_inverso(client, nombre_curso):
    """Query 3: Relacion inversa inscrito_en — ver alumnos desde un curso."""
    query = """query search($c: string) {
        curso(func: eq(curso.nombre, $c)) {
            curso.nombre
            curso.categoria
            ~inscrito_en {
                alumno.nombre
                alumno.genero
                alumno.edad
            }
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$c": nombre_curso})
    data = json.loads(res.json)
    print(f"\nAlumnos inscritos en '{nombre_curso}' (relacion inversa):")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def alumnos_con_certificado(client):
    """Query 4: Alumnos que obtuvieron certificado (2 tipos de nodo)."""
    query = """{
        alumnos(func: has(obtiene_certificado)) {
            alumno.nombre
            alumno.edad
            obtiene_certificado {
                certificado.id
                certificado.categoria
                certificado.fecha
            }
            inscrito_en {
                curso.nombre
            }
        }
    }"""
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)
    resultados = data.get("alumnos", [])
    print(f"\nAlumnos con certificado: {len(resultados)}")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def ranking_alumnos_paginado(client, pagina, tam_pagina):
    """Query 5: Ranking de alumnos por calificacion de examen (desc), con paginacion."""
    offset = (pagina - 1) * tam_pagina
    query = f"""{{
        ranking(func: has(presenta), first: {tam_pagina}, offset: {offset}) {{
            alumno.nombre
            alumno.edad
            presenta(orderdesc: examen.calificacion) {{
                examen.id
                examen.calificacion
            }}
        }}
    }}"""
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)

    # Ordenar en Python por la calificacion mas alta de cada alumno
    alumnos = data.get("ranking", [])
    alumnos_ordenados = sorted(
        alumnos,
        key=lambda a: max(
            (e.get("examen.calificacion", 0) for e in a.get("presenta", [])),
            default=0
        ),
        reverse=True  # orderdesc: el mejor primero
    )

    print(f"\nPagina {pagina} (mostrando {tam_pagina} resultados, offset {offset}):")
    print(f"Alumnos en esta pagina: {len(alumnos_ordenados)}")
    for i, alumno in enumerate(alumnos_ordenados, start=1):
        mejor_cal = max(
            (e.get("examen.calificacion", 0) for e in alumno.get("presenta", [])),
            default=0
        )
        print(f"  {i}. {alumno.get('alumno.nombre')} — mejor examen: {mejor_cal}")
    print("\nDetalle completo:")
    print(json.dumps({"ranking": alumnos_ordenados}, indent=2, ensure_ascii=False))

def contar_alumnos_por_curso(client):
    """Query 6: Contar alumnos inscritos en cada curso."""
    query = """{
        cursos(func: type(Curso)) {
            curso.nombre
            curso.categoria
            total_alumnos: count(~inscrito_en)
        }
    }"""
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)
    print("\nConteo de alumnos por curso:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

#DELETE
def delete_alumno(client, nombre):
    """Eliminar alumno por nombre usando condicion."""
    query = """query search($a: string) {
        all(func: eq(alumno.nombre, $a)) {
            uid
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$a": nombre})
    data = json.loads(res.json)
    encontrados = data.get("all", [])

    if not encontrados:
        print(f"No se encontro ningun alumno con nombre '{nombre}'.")
        return

    txn = client.txn()
    try:
        for persona in encontrados:
            print(f"Eliminando UID: {persona['uid']}")
            txn.mutate(del_obj=persona)
        txn.commit()
        print(f"Alumno '{nombre}' eliminado correctamente.")
    finally:
        txn.discard()


def drop_all(client):
    """Eliminar todos los datos y schema."""
    return client.alter(pydgraph.Operation(drop_all=True))


def tareas_y_examenes_de_maestro(client, nombre):
    """Query 7: Ver tareas y examenes que califica un maestro especifico."""
    query = """query search($a: string) {
        maestro(func: eq(maestro.nombre, $a)) {
            maestro.nombre
            maestro.especialidad
            califica_tarea {
                tarea.id
                tarea.fecha_entrega
                tarea.calificacion
            }
            califica_examen {
                examen.id
                examen.preguntas
                examen.calificacion
            }
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$a": nombre})
    data = json.loads(res.json)
    print(f"\nTareas y examenes calificados por '{nombre}':")
    print(json.dumps(data, indent=2, ensure_ascii=False))


def tareas_de_curso(client, nombre):
    """Query 8: Ver tareas asignadas a un curso especifico."""
    query = """query search($a: string) {
        curso(func: eq(curso.nombre, $a)) {
            curso.nombre
            curso.categoria
            tiene_tareas {
                tarea.id
                tarea.fecha_entrega
                tarea.calificacion
            }
        }
    }"""
    res = client.txn(read_only=True).query(query, variables={"$a": nombre})
    data = json.loads(res.json)
    print(f"\nTareas asignadas al curso '{nombre}':")
    print(json.dumps(data, indent=2, ensure_ascii=False))