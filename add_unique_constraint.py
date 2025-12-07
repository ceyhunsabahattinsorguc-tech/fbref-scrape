# -*- coding: utf-8 -*-
import pyodbc
import sys
sys.stdout.reconfigure(encoding='utf-8')

conn = pyodbc.connect('DRIVER={SQL Server};SERVER=195.201.146.224,1433;DATABASE=FBREF;UID=sa;PWD=FbRef2024Str0ng;')
cursor = conn.cursor()

# Oncelikle mevcut constraint olup olmadigini kontrol et
cursor.execute("""
SELECT name
FROM sys.indexes
WHERE object_id = OBJECT_ID('TANIM.OYUNCU') AND is_unique = 1 AND name LIKE '%URL%'
""")
existing = cursor.fetchone()

if existing:
    print(f"UNIQUE constraint zaten mevcut: {existing[0]}")
else:
    # UNIQUE constraint ekle
    try:
        cursor.execute("""
        ALTER TABLE TANIM.OYUNCU
        ADD CONSTRAINT UQ_OYUNCU_URL UNIQUE (URL)
        """)
        conn.commit()
        print("UNIQUE constraint basariyla eklendi: UQ_OYUNCU_URL")
    except Exception as e:
        print(f"Hata: {e}")

conn.close()
