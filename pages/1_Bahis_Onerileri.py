# -*- coding: utf-8 -*-
"""
Bahis Onerileri Sayfasi
Poisson tabanli mac tahminleri ve bahis onerileri
"""

import streamlit as st
import pandas as pd
import math
from datetime import datetime, timedelta

# Database connection
try:
    import pymssql
    USE_PYMSSQL = True
except ImportError:
    import pyodbc
    USE_PYMSSQL = False

st.set_page_config(
    page_title="Bahis Onerileri",
    page_icon="🎯",
    layout="wide"
)

def get_db_config():
    """Get database configuration from st.secrets"""
    try:
        db = st.secrets["database"]
        return {
            'server': db['server'].split(',')[0],
            'port': int(db['server'].split(',')[1]) if ',' in db['server'] else 1433,
            'database': db['database'],
            'user': db['username'],
            'password': db['password']
        }
    except Exception as e:
        st.error(f"Veritabani yapilandirmasi hatasi: {str(e)}")
        st.info("Lutfen Streamlit Cloud'da Secrets ekleyin.")
        return None


def get_db_connection():
    config = get_db_config()
    if config is None:
        return None
    if USE_PYMSSQL:
        return pymssql.connect(
            server=config['server'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
    else:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={config['server']},{config['port']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={config['password']}"
        )
        return pyodbc.connect(conn_str)


def poisson_pmf(k, lam):
    if lam <= 0:
        return 0 if k > 0 else 1
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def calculate_probs(ev_lambda, mis_lambda, max_goals=7):
    matrix = {}
    for ev in range(max_goals + 1):
        for mis in range(max_goals + 1):
            matrix[(ev, mis)] = poisson_pmf(ev, ev_lambda) * poisson_pmf(mis, mis_lambda)

    ev_kazanir = sum(p for (ev, mis), p in matrix.items() if ev > mis)
    berabere = sum(p for (ev, mis), p in matrix.items() if ev == mis)
    mis_kazanir = sum(p for (ev, mis), p in matrix.items() if ev < mis)
    ust_25 = sum(p for (ev, mis), p in matrix.items() if ev + mis > 2.5)
    kg_var = sum(p for (ev, mis), p in matrix.items() if ev > 0 and mis > 0)

    sorted_scores = sorted(matrix.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        '1': round(ev_kazanir * 100, 1),
        'X': round(berabere * 100, 1),
        '2': round(mis_kazanir * 100, 1),
        'ust_25': round(ust_25 * 100, 1),
        'alt_25': round((1 - ust_25) * 100, 1),
        'kg_var': round(kg_var * 100, 1),
        'kg_yok': round((1 - kg_var) * 100, 1),
        'en_olasi': [(f"{s[0][0]}-{s[0][1]}", round(s[1] * 100, 1)) for s in sorted_scores]
    }


def main():
    st.title("🎯 Bahis Onerileri")
    st.markdown("Poisson tabanli mac tahminleri ve bahis onerileri")

    # Sidebar filters
    with st.sidebar:
        st.header("Filtreler")

        days_ahead = st.slider("Kac gun ilerisine bak", 1, 14, 7)

        min_confidence = st.selectbox(
            "Minimum Guvenilirlik",
            ["Hepsi", "ORTA", "YUKSEK"],
            index=0
        )

        market_filter = st.multiselect(
            "Pazar",
            ["1X2", "Ust 2.5", "Alt 2.5", "KG Var", "KG Yok"],
            default=["1X2", "Ust 2.5"]
        )

    # Database connection and data
    try:
        conn = get_db_connection()
        if conn is None:
            st.error("Veritabani baglantisi yapilanamadi!")
            return
        cursor = conn.cursor()

        # Check if view exists
        try:
            cursor.execute("""
                SELECT TOP 1 1 FROM TAHMIN.v_Mac_Tahmin
            """)
            cursor.fetchone()
        except:
            st.error("v_Mac_Tahmin VIEW bulunamadi. Lutfen setup_prediction_system.py calistirin.")
            return

        # Get predictions
        query = f"""
            SELECT
                FIKSTURID, LIG_ADI, EVSAHIBI, MISAFIR, TARIH,
                EV_BEKLENEN_GOL, MIS_BEKLENEN_GOL, TOPLAM_BEKLENEN_GOL,
                GUVENILIRLIK, VERI_SKORU
            FROM TAHMIN.v_Mac_Tahmin
            WHERE TARIH >= GETDATE()
              AND TARIH <= DATEADD(day, {days_ahead}, GETDATE())
            ORDER BY TARIH
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            st.warning("Bu tarih araliginda tahmin edilebilir mac bulunamadi.")
            return

        # Process predictions
        predictions = []
        for row in rows:
            fid, lig, ev, mis, tarih, ev_l, mis_l, top, guven, veri = row

            if not ev_l or not mis_l:
                continue

            if min_confidence != "Hepsi" and guven != min_confidence:
                continue

            probs = calculate_probs(float(ev_l), float(mis_l))

            predictions.append({
                'fikstur_id': fid,
                'lig': lig,
                'ev_sahibi': ev,
                'misafir': mis,
                'tarih': tarih,
                'ev_lambda': float(ev_l),
                'mis_lambda': float(mis_l),
                'toplam': float(top) if top else float(ev_l) + float(mis_l),
                'guven': guven,
                'veri': veri,
                'probs': probs
            })

        conn.close()

        if not predictions:
            st.warning("Filtrelerinize uygun tahmin bulunamadi.")
            return

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Toplam Mac", len(predictions))
        with col2:
            yuksek = len([p for p in predictions if p['guven'] == 'YUKSEK'])
            st.metric("Yuksek Guven", yuksek)
        with col3:
            ust_25_count = len([p for p in predictions if p['probs']['ust_25'] >= 60])
            st.metric("Ust 2.5 Oneri", ust_25_count)
        with col4:
            today_count = len([p for p in predictions if p['tarih'] and p['tarih'].date() == datetime.now().date()])
            st.metric("Bugunki Mac", today_count)

        st.divider()

        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["📋 Tum Tahminler", "🔥 En Iyi Oneriler", "📊 Analiz"])

        with tab1:
            for p in predictions:
                with st.expander(f"[{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - {p['tarih'].strftime('%d.%m %H:%M') if p['tarih'] else '-'}"):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("**Beklenen Gol**")
                        st.write(f"Ev: {p['ev_lambda']:.2f} | Misafir: {p['mis_lambda']:.2f}")
                        st.write(f"Toplam: {p['toplam']:.2f}")

                        st.markdown("**1X2 Olasiliklari**")
                        st.progress(p['probs']['1'] / 100, f"1 (Ev): {p['probs']['1']}%")
                        st.progress(p['probs']['X'] / 100, f"X: {p['probs']['X']}%")
                        st.progress(p['probs']['2'] / 100, f"2 (Mis): {p['probs']['2']}%")

                    with col2:
                        st.markdown("**Gol Pazari**")
                        st.write(f"Ust 2.5: {p['probs']['ust_25']}% | Alt 2.5: {p['probs']['alt_25']}%")
                        st.write(f"KG Var: {p['probs']['kg_var']}% | KG Yok: {p['probs']['kg_yok']}%")

                        st.markdown("**En Olasi Skorlar**")
                        for skor, olas in p['probs']['en_olasi']:
                            st.write(f"  {skor}: {olas}%")

                        st.markdown(f"**Guvenilirlik:** {p['guven']} ({p['veri']}%)")

        with tab2:
            st.subheader("En Iyi Oneriler")

            # 1 Tahminleri
            if "1X2" in market_filter:
                st.markdown("### 🏠 Ev Kazanir (1)")
                ev_wins = [p for p in predictions if p['probs']['1'] >= 50 and p['guven'] in ['YUKSEK', 'ORTA']]
                if ev_wins:
                    for p in sorted(ev_wins, key=lambda x: x['probs']['1'], reverse=True)[:5]:
                        st.write(f"• [{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - **{p['probs']['1']}%**")
                else:
                    st.write("Guclu oneri yok")

                st.markdown("### 🚌 Misafir Kazanir (2)")
                away_wins = [p for p in predictions if p['probs']['2'] >= 45 and p['guven'] in ['YUKSEK', 'ORTA']]
                if away_wins:
                    for p in sorted(away_wins, key=lambda x: x['probs']['2'], reverse=True)[:5]:
                        st.write(f"• [{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - **{p['probs']['2']}%**")
                else:
                    st.write("Guclu oneri yok")

            # Ust 2.5
            if "Ust 2.5" in market_filter:
                st.markdown("### ⬆️ Ust 2.5 Gol")
                ust_25 = [p for p in predictions if p['probs']['ust_25'] >= 60]
                if ust_25:
                    for p in sorted(ust_25, key=lambda x: x['probs']['ust_25'], reverse=True)[:5]:
                        st.write(f"• [{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - **{p['probs']['ust_25']}%** (Beklenen: {p['toplam']:.1f})")
                else:
                    st.write("Guclu oneri yok")

            # Alt 2.5
            if "Alt 2.5" in market_filter:
                st.markdown("### ⬇️ Alt 2.5 Gol")
                alt_25 = [p for p in predictions if p['probs']['alt_25'] >= 55]
                if alt_25:
                    for p in sorted(alt_25, key=lambda x: x['probs']['alt_25'], reverse=True)[:5]:
                        st.write(f"• [{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - **{p['probs']['alt_25']}%** (Beklenen: {p['toplam']:.1f})")
                else:
                    st.write("Guclu oneri yok")

            # KG Var
            if "KG Var" in market_filter:
                st.markdown("### ⚽⚽ Karsilikli Gol Var")
                kg_var = [p for p in predictions if p['probs']['kg_var'] >= 60]
                if kg_var:
                    for p in sorted(kg_var, key=lambda x: x['probs']['kg_var'], reverse=True)[:5]:
                        st.write(f"• [{p['lig']}] {p['ev_sahibi']} vs {p['misafir']} - **{p['probs']['kg_var']}%**")
                else:
                    st.write("Guclu oneri yok")

        with tab3:
            st.subheader("Istatistik Analizi")

            # Lig bazinda dagilim
            df = pd.DataFrame(predictions)
            lig_counts = df['lig'].value_counts()

            st.markdown("### Lig Bazinda Mac Sayisi")
            st.bar_chart(lig_counts)

            st.markdown("### Guvenilirlik Dagilimi")
            guven_counts = df['guven'].value_counts()
            col1, col2, col3 = st.columns(3)
            for idx, (guven, count) in enumerate(guven_counts.items()):
                with [col1, col2, col3][idx % 3]:
                    st.metric(guven, count)

    except Exception as e:
        st.error(f"Hata: {str(e)}")
        st.info("Veritabani baglantisi kontrol edin veya setup_prediction_system.py calistirin.")


if __name__ == "__main__":
    main()
