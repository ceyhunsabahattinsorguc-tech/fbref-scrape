# -*- coding: utf-8 -*-
import pyodbc
import sys
sys.stdout.reconfigure(encoding='utf-8')

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=195.201.146.224,1433;DATABASE=FBREF;UID=sa;PWD=FbRef2024Str0ng;')
cursor = conn.cursor()

# Sutunlari getir
cursor.execute("""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'TANIM' AND TABLE_NAME = 'LIG'
ORDER BY ORDINAL_POSITION
""")
columns = [row[0] for row in cursor.fetchall()]
print("Sutunlar:", columns)

# Verileri getir
cursor.execute("SELECT * FROM TANIM.LIG ORDER BY LIG_ID")
rows = cursor.fetchall()

print("\nTANIM.LIG Tablosu:")
print("-" * 100)
for row in rows:
    print(row)

conn.close()
