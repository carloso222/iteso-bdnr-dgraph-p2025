import os
import pydgraph
import model

DGRAPH_URI = 'localhost:9081'


def print_menu():
    options = {
        1:  "Cargar datos desde CSV",
        2:  "Buscar alumno por nombre",
        3:  "Buscar examenes por calificacion minima",
        4:  "Ver alumnos inscritos en un curso (relacion inversa)",
        5:  "Ver alumnos con certificado",
        6:  "Ranking de alumnos por calificacion de examen (orden + paginacion)",
        7:  "Contar alumnos por curso",
        8:  "Ver tareas y examenes que califica un maestro",
        9:  "Ver tareas asignadas a un curso",
        10: "Eliminar alumno por nombre",
        11: "Eliminar todos los datos",
        0:  "Salir",
    }
    print("\n" + "="*50)
    print("         SISTEMA ESCOLAR - DGRAPH")
    print("="*50)
    for key, value in options.items():
        print(f"  {key:2} -- {value}")
    print("="*50)


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)

    print("Conectando a Dgraph y creando schema...")
    model.set_schema(client)
    print("Schema creado exitosamente.")

    while True:
        print_menu()
        try:
            option = int(input("Selecciona una opcion: "))
        except ValueError:
            print("Opcion invalida. Intenta de nuevo.")
            continue

        if option == 1:
            model.load_data_from_csv(client)

        elif option == 2:
            nombre = input("Nombre del alumno: ")
            model.search_alumno(client, nombre)

        elif option == 3:
            try:
                cal = float(input("Calificacion minima: "))
                model.search_examenes_por_calificacion(client, cal)
            except ValueError:
                print("Ingresa un numero valido.")

        elif option == 4:
            curso = input("Nombre del curso: ")
            model.alumnos_por_curso_inverso(client, curso)

        elif option == 5:
            model.alumnos_con_certificado(client)

        elif option == 6:
            try:
                pagina = int(input("Numero de pagina (1, 2, ...): "))
                tam = int(input("Resultados por pagina: "))
                model.ranking_alumnos_paginado(client, pagina, tam)
            except ValueError:
                print("Ingresa numeros validos.")

        elif option == 7:
            model.contar_alumnos_por_curso(client)

        elif option == 8:
            nombre = input("Nombre del maestro: ")
            model.tareas_y_examenes_de_maestro(client, nombre)

        elif option == 9:
            nombre = input("Nombre del curso: ")
            model.tareas_de_curso(client, nombre)

        elif option == 10:
            nombre = input("Nombre del alumno a eliminar: ")
            model.delete_alumno(client, nombre)

        elif option == 11:
            confirm = input("Seguro que deseas eliminar TODOS los datos? (s/n): ")
            if confirm.lower() == 's':
                model.drop_all(client)
                print("Todos los datos eliminados.")

        elif option == 0:
            print("Cerrando conexion...")
            close_client_stub(client_stub)
            exit(0)

        else:
            print("Opcion no valida.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")