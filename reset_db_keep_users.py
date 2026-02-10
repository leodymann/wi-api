import psycopg2

conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/moto_store")
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
TRUNCATE TABLE
  installments,
  promissories,
  sales,
  product_images,
  products,
  clients,
  finance
RESTART IDENTITY CASCADE;
""")

cur.close()
conn.close()
print("OK: limpo (mantive users) e IDs resetados")
