# -*- coding: utf-8 -*-
"""LIG tablosu yapısını kontrol et"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

import pyodbc

CONNECTION_STRING = (
    "DRIVER={SQL Server};"
    "SERVER=195.201.146.224,1433;"
    "DATABASE=FBREF;"
    "UID=sa;"
    "PWD=FbRef2024Str0ng;"
)

conn = pyodbc.connect(CONNECTION_STRING)
cursor = conn.cursor()

# LIG tablosu yapisi
cursor.execute("""
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'TANIM' AND TABLE_NAME = 'LIG'
ORDER BY ORDINAL_POSITION
""")

print('TANIM.LIG Tablo Yapisi:')
print('-' * 50)
for row in cursor.fetchall():
    print(f'{row[0]:<20} {row[1]:<15} {row[2] if row[2] else ""}')

# Mevcut ligler
print()
print('Mevcut Ligler:')
print('-' * 80)
cursor.execute('SELECT * FROM TANIM.LIG ORDER BY LIG_ID')
cols = [desc[0] for desc in cursor.description]
print(cols)
for row in cursor.fetchall():
    print(list(row))

# Avrupa kupalari kontrol
print()
print('Avrupa Kupalari (19, 20, 21):')
print('-' * 80)
cursor.execute("""
SELECT LIG_ID, LIG_ADI FROM TANIM.LIG
WHERE LIG_ID IN (19, 20, 21)
""")
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

# Avrupa kupalarindaki mac sayisi
print()
print('Avrupa Kupalarindaki Mac Sayisi:')
cursor.execute("""
SELECT l.LIG_ADI, COUNT(*) as MAC_SAYISI
FROM FIKSTUR.FIKSTUR f
JOIN TANIM.LIG l ON f.LIG_ID = l.LIG_ID
WHERE f.LIG_ID IN (19, 20, 21)
GROUP BY l.LIG_ADI
""")
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]} mac')

conn.close()
