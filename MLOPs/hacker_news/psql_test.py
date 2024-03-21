import psycopg2

conn_params = 'postgres://postgres:mysecretpassword@localhost:5432/postgres'
try:
    with psycopg2.connect(conn_params) as conn:
        with conn.cursor() as cur:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS questions (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            body TEXT,
            comments JSONB
            );
            """
            cur.execute(create_table_query)
            conn.commit()
except Exception as e:
    print(f"Error creating table: {e}")