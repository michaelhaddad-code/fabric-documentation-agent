import subprocess, struct, pyodbc

AZ = r'C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd'
result = subprocess.run(
    [AZ, 'account', 'get-access-token', '--resource', 'https://database.windows.net/',
     '--query', 'accessToken', '-o', 'tsv'],
    capture_output=True, text=True, shell=True
)
token = result.stdout.strip()
token_bytes = token.encode('utf-16-le')
token_struct = struct.pack('=i', len(token_bytes)) + token_bytes

server = 'xeqchbfhpzxefhxm4nant3xd6m-cbzc5jxhdelendxtleyso5rnyi.datawarehouse.fabric.microsoft.com'
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE=gold_time_entries;Encrypt=yes;',
    attrs_before={1256: token_struct}
)
cur = conn.cursor()

print('=== BASE TABLES ===')
cur.execute('SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME')
rows = cur.fetchall()
print(f'  {len(rows)} table(s)')
for r in rows:
    cur.execute(f'SELECT COUNT(*) FROM [{r[0]}].[{r[1]}]')
    n = cur.fetchone()[0]
    print(f'  {r[0]}.{r[1]}  ({n} rows)')

print()
print('=== VIEWS ===')
cur.execute('SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS ORDER BY TABLE_NAME')
rows = cur.fetchall()
print(f'  {len(rows)} view(s)')
for r in rows:
    print(f'  {r[0]}.{r[1]}')

conn.close()
