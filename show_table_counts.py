# -*- coding: utf-8 -*-
import pyodbc
import sys
sys.stdout.reconfigure(encoding='utf-8')

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=195.201.146.224,1433;DATABASE=FBREF;UID=sa;PWD=FbRef2024Str0ng;')
cursor = conn.cursor()

tables = [
    ('FIKSTUR', 'SEZON'),
    ('TANIM', 'LIG'),
    ('TANIM', 'TAKIM'),
    ('TANIM', 'OYUNCU'),
    ('FIKSTUR', 'FIKSTUR'),
    ('FIKSTUR', 'DETAY'),
    ('FIKSTUR', 'PERFORMANS'),
    ('FIKSTUR', 'KALECI_PERFORMANS'),
]

print("TABLO DURUMU:")
print("=" * 50)
print(f"{'TABLO':<35} {'KAYIT SAYISI':>12}")
print("-" * 50)

for schema, table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cursor.fetchone()[0]
        print(f"{schema}.{table:<27} {count:>12}")
    except Exception as e:
        print(f"{schema}.{table:<27} {'HATA':>12}")

print("=" * 50)
conn.close()
