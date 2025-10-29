import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO_URI  = os.getenv("NEO_URI",  "bolt://localhost:7687").strip()
NEO_USER = os.getenv("NEO_USER", "neo4j").strip()
NEO_PASS = os.getenv("NEO_PASS", "neo4jpassword").strip()
NEO_DB   = os.getenv("NEO_DB",   "neo4j").strip()

# --- Funciones básicas de ejecución ---
def run(tx, query, **params):
    return list(tx.run(query, **params))

def run_many(sess, statements):
    for q in statements:
        q = q.strip()
        if q:
            sess.execute_write(run, q)

# --- Bootstrap del modelo inicial ---
def bootstrap_model():
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS)) as driver:
        with driver.session(database=NEO_DB) as sess:
            constraints = [
                "CREATE CONSTRAINT role_id  IF NOT EXISTS FOR (r:Role)    REQUIRE r.id IS UNIQUE",
                "CREATE CONSTRAINT user_id  IF NOT EXISTS FOR (u:User)    REQUIRE u.id IS UNIQUE",
                "CREATE CONSTRAINT group_id IF NOT EXISTS FOR (g:Group)   REQUIRE g.id IS UNIQUE",
                "CREATE CONSTRAINT proc_id  IF NOT EXISTS FOR (p:Process) REQUIRE p.id IS UNIQUE",
            ]
            run_many(sess, constraints)

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
                "MATCH (a:Process {id:'proc_promedios'}),(b:Process {id:'proc_temp_max_min'}) MERGE (a)-[:DEPENDS_ON]->(b)"
            ]
            run_many(sess, deps)

# --- Asignaciones ---
def grant_all_to_admin():
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS)) as driver:
        with driver.session(database=NEO_DB) as sess:
            sess.execute_write(run, """
            MATCH (r:Role {id:'admin'}), (p:Process)
            MERGE (r)-[:CAN_RUN]->(p)
            """)

def create_user_with_role(userId: str, roleId: str):
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS)) as driver:
        with driver.session(database=NEO_DB) as sess:
            sess.execute_write(run, """
            MERGE (u:User {id:$uid})
            WITH u MATCH (r:Role {id:$rid})
            MERGE (u)-[:HAS_ROLE]->(r)
            """, uid=userId, rid=roleId)

# --- Procesos accesibles (versión más clara y sin warnings) ---
def processes_user_can_run(userId: str):
    with GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS)) as driver:
        with driver.session(database=NEO_DB) as sess:
            result = sess.execute_read(run, """
            MATCH (u:User {id:$uid})
            OPTIONAL MATCH (u)-[r]->(mid)-[:CAN_RUN]->(p:Process)
            WHERE (type(r) = 'HAS_ROLE' AND mid:Role)
               OR (type(r) = 'MEMBER_OF' AND mid:Group)
            WITH DISTINCT p, type(r) AS via
            RETURN p.id AS id, coalesce(p.name, p.id) AS nombre,
                   CASE via WHEN 'HAS_ROLE' THEN 'Por rol'
                            WHEN 'MEMBER_OF' THEN 'Por grupo'
                            ELSE 'Otro' END AS acceso
            ORDER BY acceso, id
            """, uid=userId)
            return [r.data() for r in result if r["id"]]

def print_processes(rows):
    if not rows:
        print("Procesos habilitados: (ninguno)")
        return
    ancho = 60
    print("\nProcesos habilitados")
    print("=" * ancho)
    print(f"{'ID':<24} {'Nombre':<26} {'Acceso':<10}")
    print("-" * ancho)
    for r in rows:
        print(f"{r['id']:<24} {r['nombre']:<26} {r['acceso']:<10}")
    print("=" * ancho)

# --- MAIN ---
if __name__ == "__main__":
    print(f"[neo4j] uri={NEO_URI} user={NEO_USER} db={NEO_DB} (pass=***)")
    bootstrap_model()
    grant_all_to_admin()
    create_user_with_role("usr_0001", "admin")
    procesos = processes_user_can_run("usr_0001")
    print_processes(procesos)
