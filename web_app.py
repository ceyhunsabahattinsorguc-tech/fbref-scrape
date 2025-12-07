# -*- coding: utf-8 -*-
"""
FBRef Scraper Web Arayuzu
Streamlit ile lig secimi ve scrape kontrolu
Cloud-ready version with st.secrets support
"""

import streamlit as st
import pyodbc
from datetime import datetime
import subprocess
import sys
import threading
import time
import pandas as pd

# Sayfa ayarlari
st.set_page_config(
    page_title="FBRef Scraper",
    page_icon="⚽",
    layout="wide"
)

def get_connection_string():
    """Get connection string from secrets or fallback to default"""
    try:
        # Try Streamlit secrets first (for cloud deployment)
        db = st.secrets["database"]
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db['server']};"
            f"DATABASE={db['database']};"
            f"UID={db['username']};"
            f"PWD={db['password']};"
        )
    except:
        # Fallback for local development
        return (
            "DRIVER={SQL Server};"
            "SERVER=195.201.146.224,1433;"
            "DATABASE=FBREF;"
            "UID=sa;"
            "PWD=FbRef2024Str0ng;"
        )

# Tüm ligler
FULL_STATS_LEAGUES = [
    {"lig_id": 6, "name": "Premier League", "country": "İngiltere", "type": "full"},
    {"lig_id": 7, "name": "La Liga", "country": "İspanya", "type": "full"},
    {"lig_id": 8, "name": "Serie A", "country": "İtalya", "type": "full"},
    {"lig_id": 9, "name": "Ligue 1", "country": "Fransa", "type": "full"},
    {"lig_id": 10, "name": "Bundesliga", "country": "Almanya", "type": "full"},
    {"lig_id": 11, "name": "Eredivisie", "country": "Hollanda", "type": "full"},
    {"lig_id": 12, "name": "Primeira Liga", "country": "Portekiz", "type": "full"},
    {"lig_id": 14, "name": "Brazilian Serie A", "country": "Brezilya", "type": "full"},
    {"lig_id": 15, "name": "Championship", "country": "İngiltere", "type": "full"},
    {"lig_id": 17, "name": "First Division A", "country": "Belçika", "type": "full"},
]

SUMMARY_LEAGUES = [
    {"lig_id": 4, "name": "Süper Lig", "country": "Türkiye", "type": "summary"},
    {"lig_id": 13, "name": "Scottish Premiership", "country": "İskoçya", "type": "summary"},
    {"lig_id": 16, "name": "Austrian Bundesliga", "country": "Avusturya", "type": "summary"},
    {"lig_id": 18, "name": "Superliga", "country": "Danimarka", "type": "summary"},
    {"lig_id": 19, "name": "Champions League", "country": "Avrupa", "type": "summary"},
    {"lig_id": 20, "name": "Europa League", "country": "Avrupa", "type": "summary"},
    {"lig_id": 21, "name": "Europa Conference League", "country": "Avrupa", "type": "summary"},
    {"lig_id": 22, "name": "Serbian SuperLiga", "country": "Sırbistan", "type": "summary"},
    {"lig_id": 23, "name": "Swiss Super League", "country": "İsviçre", "type": "summary"},
    {"lig_id": 24, "name": "Ekstraklasa", "country": "Polonya", "type": "summary"},
    {"lig_id": 25, "name": "Super League Greece", "country": "Yunanistan", "type": "summary"},
    {"lig_id": 26, "name": "Czech First League", "country": "Çekya", "type": "summary"},
    {"lig_id": 28, "name": "Veikkausliiga", "country": "Finlandiya", "type": "summary"},
    {"lig_id": 29, "name": "Eliteserien", "country": "Norveç", "type": "summary"},
    {"lig_id": 30, "name": "Allsvenskan", "country": "İsveç", "type": "summary"},
]


def get_db_connection():
    return pyodbc.connect(get_connection_string())


def get_table_stats():
    """Tablo istatistiklerini getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        stats = {}
        tables = [
            ('TANIM', 'LIG'),
            ('TANIM', 'TAKIM'),
            ('TANIM', 'OYUNCU'),
            ('FIKSTUR', 'FIKSTUR'),
            ('FIKSTUR', 'PERFORMANS'),
            ('FIKSTUR', 'KALECI_PERFORMANS'),
        ]

        for schema, table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            stats[f"{schema}.{table}"] = cursor.fetchone()[0]

        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}


def get_league_match_counts():
    """Her lig için maç sayısını getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT l.LIG_ADI, COUNT(f.FIKSTURID) as MAC_SAYISI
            FROM TANIM.LIG l
            LEFT JOIN FIKSTUR.FIKSTUR f ON l.LIG_ID = f.LIG_ID
            WHERE l.SEZON = '2025-2026'
            GROUP BY l.LIG_ADI
            ORDER BY MAC_SAYISI DESC
        """)

        results = {}
        for row in cursor.fetchall():
            results[row[0]] = row[1]

        conn.close()
        return results
    except Exception as e:
        return {}


def main():
    st.title("⚽ FBRef Scraper")
    st.markdown("---")

    # Sidebar - Tablo İstatistikleri
    with st.sidebar:
        st.header("📊 Veritabanı Durumu")

        stats = get_table_stats()
        if "error" not in stats:
            for table, count in stats.items():
                st.metric(table, count)
        else:
            st.error(f"Bağlantı hatası: {stats['error']}")

        st.markdown("---")
        if st.button("🔄 Yenile"):
            st.rerun()

    # Ana içerik
    tab1, tab2, tab3 = st.tabs(["🎯 Lig Seçimi", "📈 Sonuçlar", "⚙️ Ayarlar"])

    with tab1:
        st.header("Lig Seçimi")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏆 Full Stats Ligler (6 Sekme)")
            st.caption("Summary, Passing, Pass Types, Defense, Possession, Misc")

            full_selected = []
            for league in FULL_STATS_LEAGUES:
                if st.checkbox(
                    f"{league['name']} ({league['country']})",
                    key=f"full_{league['lig_id']}"
                ):
                    full_selected.append(league)

        with col2:
            st.subheader("📋 Summary-Only Ligler")
            st.caption("Sadece Summary + Keeper Stats")

            summary_selected = []
            for league in SUMMARY_LEAGUES:
                if st.checkbox(
                    f"{league['name']} ({league['country']})",
                    key=f"summary_{league['lig_id']}"
                ):
                    summary_selected.append(league)

        st.markdown("---")

        # Test modu
        col1, col2, col3 = st.columns(3)
        with col1:
            test_mode = st.checkbox("🧪 Test Modu", value=True)
        with col2:
            if test_mode:
                test_limit = st.number_input("Maç limiti", min_value=1, max_value=10, value=1)
            else:
                test_limit = None

        # Scrape butonu
        st.markdown("---")

        total_selected = len(full_selected) + len(summary_selected)

        if total_selected > 0:
            st.info(f"Seçili lig sayısı: {total_selected}")

            if st.button("🚀 Scrape Başlat", type="primary"):
                st.warning("Scraping başlatılıyor... Bu işlem uzun sürebilir.")

                progress_bar = st.progress(0)
                status_text = st.empty()

                # Full stats ligler
                for i, league in enumerate(full_selected):
                    status_text.text(f"İşleniyor: {league['name']}...")
                    # Burada scraper çağrılacak
                    progress_bar.progress((i + 1) / total_selected)
                    time.sleep(0.5)  # Demo için

                # Summary ligler
                for i, league in enumerate(summary_selected):
                    status_text.text(f"İşleniyor: {league['name']}...")
                    progress_bar.progress((len(full_selected) + i + 1) / total_selected)
                    time.sleep(0.5)  # Demo için

                st.success("Scraping tamamlandı!")
                st.balloons()
        else:
            st.warning("Lütfen en az bir lig seçin.")

    with tab2:
        st.header("Sonuçlar")

        match_counts = get_league_match_counts()
        if match_counts:
            st.subheader("Lig Bazlı Maç Sayıları")

            for league, count in match_counts.items():
                st.metric(league, f"{count} maç")
        else:
            st.info("Henüz veri yok.")

    with tab3:
        st.header("Ayarlar")

        st.subheader("Veritabani Baglantisi")
        conn_str = get_connection_string()
        # Mask password in display
        masked = conn_str.replace("FbRef2024Str0ng", "*****")
        for word in conn_str.split(";"):
            if "PWD=" in word:
                pwd = word.split("=")[1] if "=" in word else ""
                masked = conn_str.replace(pwd, "*****")
                break
        st.code(masked)

        st.subheader("Sezon")
        st.text("2025-2026")

        st.markdown("---")

        st.subheader("Tehlikeli İşlemler")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🗑️ Tüm Verileri Temizle", type="secondary"):
                st.warning("Bu işlem tüm verileri silecek!")
                if st.button("Evet, Sil"):
                    # Truncate işlemi
                    pass

        with col2:
            if st.button("📋 Tabloları Oluştur"):
                st.info("Tablolar oluşturulacak...")


if __name__ == "__main__":
    main()
