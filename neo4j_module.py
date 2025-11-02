
import os
import argparse
from typing import List, Dict
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO_URI  = os.getenv("NEO_URI",  "bolt://localhost:7687").strip()
NEO_USER = os.getenv("NEO_USER", "neo4j").strip()
NEO_PASS = os.getenv("NEO_PASS", "neo4jpassword").strip()
NEO_DB   = os.getenv("NEO_DB",   "neo4j").strip()

def run(tx, query, **params):
    return list(tx.run(query, **params))

def run_many(sess, statements):
    for q in statements:
        q = q.strip()
        if q:
            sess.execute_write(run, q)

def get_driver():
    return GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))


def bootstrap_model():
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        constraints = [
            "CREATE CONSTRAINT role_id  IF NOT EXISTS FOR (r:Role)    REQUIRE r.id IS UNIQUE",
            "CREATE CONSTRAINT user_id  IF NOT EXISTS FOR (u:User)    REQUIRE u.id IS UNIQUE",
            "CREATE CONSTRAINT group_id IF NOT EXISTS FOR (g:Group)   REQUIRE g.id IS UNIQUE",
            "CREATE CONSTRAINT proc_id  IF NOT EXISTS FOR (p:Process) REQUIRE p.id IS UNIQUE",
        ]
        run_many(sess, constraints)

def seed_minimal():
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        seeds_roles = [
            "MERGE (:Role {id:'usuario'})",
            "MERGE (:Role {id:'tecnico'})",
            "MERGE (:Role {id:'admin'})",
        ]
        run_many(sess, seeds_roles)

        seeds_procs = [
            "MERGE (:Process {id:'proc_temp_max_min', name:'Max/Min por ciudad'})",
            "MERGE (:Process {id:'proc_promedios',    name:'Promedios por ciudad'})",
            "MERGE (:Process {id:'proc_alertas',      name:'Alertas por umbrales'})",
        ]
        run_many(sess, seeds_procs)

        deps = [
            "MATCH (a:Process {id:'proc_promedios'}),(b:Process {id:'proc_temp_max_min'}) "
            "MERGE (a)-[:DEPENDS_ON]->(b)"
        ]
        run_many(sess, deps)


def grant_all_to_admin():
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, """
        MATCH (r:Role {id:'admin'}), (p:Process)
        MERGE (r)-[:CAN_RUN]->(p)
        """)

def create_user_with_role(userId: str, roleId: str):
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, """
        MERGE (u:User {id:$uid})
        WITH u
        MATCH (r:Role {id:$rid})
        MERGE (u)-[:HAS_ROLE]->(r)
        """, uid=userId, rid=roleId)

def define_group(groupId: str):
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, "MERGE (:Group {id:$gid})", gid=groupId)

def add_member(userId: str, groupId: str):
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, """
        MERGE (u:User {id:$uid})
        MERGE (g:Group {id:$gid})
        MERGE (u)-[:MEMBER_OF]->(g)
        """, uid=userId, gid=groupId)

def grant_role_can_run(roleId: str, processId: str):
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, """
        MATCH (r:Role {id:$rid}), (p:Process {id:$pid})
        MERGE (r)-[:CAN_RUN]->(p)
        """, rid=roleId, pid=processId)

def grant_group_can_run(groupId: str, processId: str):
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        sess.execute_write(run, """
        MATCH (g:Group {id:$gid}), (p:Process {id:$pid})
        MERGE (g)-[:CAN_RUN]->(p)
        """, gid=groupId, pid=processId)


Q_USER_PROCS = """
MATCH (u:User {id:$uid})
OPTIONAL MATCH (u)-[r]->(mid)-[:CAN_RUN]->(p:Process)
WHERE (type(r) = 'HAS_ROLE'  AND mid:Role)
   OR (type(r) = 'MEMBER_OF' AND mid:Group)
WITH DISTINCT p, type(r) AS via
RETURN p.id AS id, coalesce(p.name, p.id) AS nombre,
       CASE via WHEN 'HAS_ROLE' THEN 'Por rol'
                WHEN 'MEMBER_OF' THEN 'Por grupo'
                ELSE 'Otro' END AS acceso
ORDER BY acceso, id
"""

def processes_user_can_run(userId: str) -> List[Dict]:
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        result = sess.execute_read(run, Q_USER_PROCS, uid=userId)
        return [r.data() for r in result if r["id"]]

def list_roles() -> List[str]:
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        res = sess.execute_read(run, "MATCH (r:Role) RETURN r.id AS id ORDER BY id")
        return [x["id"] for x in res]

def list_groups() -> List[str]:
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        res = sess.execute_read(run, "MATCH (g:Group) RETURN g.id AS id ORDER BY id")
        return [x["id"] for x in res]

def list_processes() -> List[Dict]:
    with get_driver() as driver, driver.session(database=NEO_DB) as sess:
        res = sess.execute_read(run, "MATCH (p:Process) RETURN p.id AS id, coalesce(p.name,p.id) AS nombre ORDER BY id")
        return [x.data() for x in res]


def print_table(rows: List[Dict], title: str, cols: List[str], widths: List[int]):
    if rows is None:
        rows = []
    total_w = sum(widths) + (len(widths) - 1) * 1
    print(f"\n{title}")
    print("=" * total_w)
    header = " ".join(f"{c:<{w}}" for c, w in zip(cols, widths))
    print(header)
    print("-" * total_w)
    for r in rows:
        line = " ".join(f"{str(r.get(c,'')):<{w}}" for c, w in zip(cols, widths))
        print(line)
    print("=" * total_w)

def print_processes(rows):
    print_table(rows, "Procesos habilitados", ["id", "nombre", "acceso"], [24, 28, 10])


def build_parser():
    p = argparse.ArgumentParser(description="Neo4j CLI - Roles/Grupos/Permisos")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Mostrar conexión y DB")

    sub.add_parser("bootstrap", help="Crear constraints")

    sub.add_parser("seed", help="Crear roles/procesos demo")

    cu = sub.add_parser("create-user", help="Crear usuario y asignar rol")
    cu.add_argument("--id", required=True)
    cu.add_argument("--role", required=True)

    dg = sub.add_parser("define-group", help="Crear grupo")
    dg.add_argument("--id", required=True)

    am = sub.add_parser("add-member", help="Agregar usuario a grupo")
    am.add_argument("--user", required=True)
    am.add_argument("--group", required=True)

    ga = sub.add_parser("grant-admin", help="Dar acceso a todos los procesos al rol admin")

    gr = sub.add_parser("grant-role", help="Conceder proceso a rol")
    gr.add_argument("--role", required=True)
    gr.add_argument("--process", required=True)

    gg = sub.add_parser("grant-group", help="Conceder proceso a grupo")
    gg.add_argument("--group", required=True)
    gg.add_argument("--process", required=True)

    lp = sub.add_parser("list-procs", help="Listar procesos que puede ejecutar un usuario")
    lp.add_argument("--user", required=True)

    sub.add_parser("list-roles", help="Listar roles")
    sub.add_parser("list-groups", help="Listar grupos")
    sub.add_parser("list-processes", help="Listar catálogo de procesos")

    return p

def main():
    args = build_parser().parse_args()

    if args.cmd == "status":
        print(f"[neo4j] uri={NEO_URI} user={NEO_USER} db={NEO_DB} (pass=***)")
        return

    if args.cmd == "bootstrap":
        bootstrap_model()
        print("Constraints OK")
        return

    if args.cmd == "seed":
        seed_minimal()
        print("Seeds OK (roles y procesos)")
        return

    if args.cmd == "create-user":
        create_user_with_role(args.id, args.role)
        print(f"Usuario {args.id} creado/asignado al rol {args.role}")
        return

    if args.cmd == "define-group":
        define_group(args.id)
        print(f"Grupo {args.id} creado/asegurado")
        return

    if args.cmd == "add-member":
        add_member(args.user, args.group)
        print(f"{args.user} agregado a grupo {args.group}")
        return

    if args.cmd == "grant-admin":
        grant_all_to_admin()
        print("Rol admin puede ejecutar todos los procesos")
        return

    if args.cmd == "grant-role":
        grant_role_can_run(args.role, args.process)
        print(f"Rol {args.role} puede ejecutar {args.process}")
        return

    if args.cmd == "grant-group":
        grant_group_can_run(args.group, args.process)
        print(f"Grupo {args.group} puede ejecutar {args.process}")
        return

    if args.cmd == "list-procs":
        rows = processes_user_can_run(args.user)
        print_processes(rows)
        return

    if args.cmd == "list-roles":
        rows = [{"id": r} for r in list_roles()]
        print_table(rows, "Roles", ["id"], [20])
        return

    if args.cmd == "list-groups":
        rows = [{"id": g} for g in list_groups()]
        print_table(rows, "Grupos", ["id"], [20])
        return

    if args.cmd == "list-processes":
        rows = list_processes()
        print_table(rows, "Procesos", ["id", "nombre"], [24, 28])
        return

if __name__ == "__main__":
    main()
