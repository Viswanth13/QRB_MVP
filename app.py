
import os
import json
import pandas as pd
import duckdb
import streamlit as st
import plotly.express as px
from google import genai

st.set_page_config(page_title="Basemakers MVP - QBR Agent", layout="wide")

DATA_DIR = "data"  # repo-relative when deployed
NOTES_PATH = os.path.join(DATA_DIR, "notion_notes.parquet")
SALES_PATH = os.path.join(DATA_DIR, "partner_sales_daily.parquet")

@st.cache_data
def load_data():
    notes = pd.read_parquet(NOTES_PATH)
    sales = pd.read_parquet(SALES_PATH)
    notes["date"] = pd.to_datetime(notes["date"], errors="coerce")
    sales["date"] = pd.to_datetime(sales["date"], errors="coerce")
    return notes, sales

def build_kpis(sales: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("sales_df", sales)

    sql = """
    WITH base AS (
      SELECT
        date::DATE AS date,
        partner,
        revenue_usd,
        units_sold,
        oos_rate,
        stores_covered
      FROM sales_df
    ),
    daily AS (
      SELECT
        *,
        SUM(revenue_usd) OVER (PARTITION BY partner ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rev_7d,
        SUM(revenue_usd) OVER (PARTITION BY partner ORDER BY date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rev_28d,
        AVG(oos_rate) OVER (PARTITION BY partner ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS oos_7d_avg
      FROM base
    ),
    wow AS (
      SELECT
        *,
        LAG(rev_7d, 7) OVER (PARTITION BY partner ORDER BY date) AS rev_7d_prev_week,
        CASE
          WHEN LAG(rev_7d, 7) OVER (PARTITION BY partner ORDER BY date) IS NULL THEN NULL
          WHEN LAG(rev_7d, 7) OVER (PARTITION BY partner ORDER BY date) = 0 THEN NULL
          ELSE (rev_7d - LAG(rev_7d, 7) OVER (PARTITION BY partner ORDER BY date))
               / LAG(rev_7d, 7) OVER (PARTITION BY partner ORDER BY date)
        END AS rev_7d_wow_pct
      FROM daily
    )
    SELECT
      date, partner, revenue_usd, units_sold, oos_rate, stores_covered,
      rev_7d, rev_28d, rev_7d_prev_week, rev_7d_wow_pct, oos_7d_avg,
      CASE
        WHEN rev_7d_wow_pct IS NULL THEN NULL
        WHEN rev_7d_wow_pct <= -0.10 THEN 'Down'
        WHEN rev_7d_wow_pct >= 0.10 THEN 'Up'
        ELSE 'Flat'
      END AS momentum_label
    FROM wow
    ORDER BY partner, date;
    """
    return con.execute(sql).df()

def snapshot_28d(df_kpis: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    con.register("k", df_kpis)

    sql = """
    WITH latest AS (
      SELECT partner, MAX(date) AS as_of_date
      FROM k
      GROUP BY partner
    ),
    cur AS (
      SELECT
        k.partner,
        l.as_of_date,
        ANY_VALUE(k.rev_7d) FILTER (WHERE k.date = l.as_of_date) AS rev_7d,
        ANY_VALUE(k.rev_28d) FILTER (WHERE k.date = l.as_of_date) AS rev_28d,
        ANY_VALUE(k.rev_7d_wow_pct) FILTER (WHERE k.date = l.as_of_date) AS rev_7d_wow_pct,
        ANY_VALUE(k.oos_7d_avg) FILTER (WHERE k.date = l.as_of_date) AS oos_7d_avg,
        ANY_VALUE(k.momentum_label) FILTER (WHERE k.date = l.as_of_date) AS momentum_label
      FROM k
      JOIN latest l USING (partner)
      GROUP BY k.partner, l.as_of_date
    ),
    last_28 AS (
      SELECT
        partner,
        SUM(revenue_usd) AS revenue_28d,
        SUM(units_sold) AS units_28d,
        AVG(oos_rate) AS oos_avg_28d,
        AVG(stores_covered) AS stores_avg_28d
      FROM k
      WHERE date >= (SELECT MAX(date) FROM k) - INTERVAL 27 DAY
      GROUP BY partner
    )
    SELECT
      c.partner,
      c.as_of_date,
      l28.revenue_28d,
      l28.units_28d,
      l28.oos_avg_28d,
      l28.stores_avg_28d,
      c.rev_7d,
      c.rev_28d,
      c.rev_7d_wow_pct,
      c.oos_7d_avg,
      c.momentum_label
    FROM cur c
    JOIN last_28 l28 USING (partner)
    ORDER BY partner;
    """
    df = con.execute(sql).df()
    df["rev_7d_wow_pct_%"] = (df["rev_7d_wow_pct"] * 100).round(2)
    df.drop(columns=["rev_7d_wow_pct"], inplace=True)
    return df

def normalize_for_json(df_snapshot: pd.DataFrame, notes: pd.DataFrame, partner: str):
    snap_df = df_snapshot[df_snapshot["partner"] == partner].copy()
    for col in snap_df.columns:
        if pd.api.types.is_datetime64_any_dtype(snap_df[col]):
            snap_df[col] = snap_df[col].dt.strftime("%Y-%m-%d")
    snap = snap_df.to_dict(orient="records")

    notes_df = notes[notes["partner"] == partner].copy()
    notes_df["date"] = pd.to_datetime(notes_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    def norm_tags(x):
        if x is None:
            return []
        if isinstance(x, list):
            return x
        return [str(x)]

    notes_df["tags"] = notes_df["tags"].apply(norm_tags)
    notes_list = notes_df.sort_values("date", ascending=False).head(5)[["date","name","tags","priority","note"]].to_dict(orient="records")
    return snap, notes_list

st.title("Basemakers MVP — One-Click QBR Agent")

notes, sales = load_data()
df_kpis = build_kpis(sales)
df_snap = snapshot_28d(df_kpis)

partners = df_snap["partner"].tolist()
partner = st.selectbox("Select partner", partners)

left, right = st.columns([1,1])

with left:
    st.subheader("KPIs (28d snapshot)")
    st.dataframe(df_snap[df_snap["partner"] == partner], use_container_width=True)

with right:
    st.subheader("Recent notes (last 28d)")
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=28)
    notes_recent = notes[(notes["partner"] == partner) & (notes["date"] >= cutoff)].copy()
    st.dataframe(notes_recent.sort_values("date", ascending=False)[["date","name","tags","priority","note"]], use_container_width=True)

st.subheader("Trends")
partner_kpis = df_kpis[df_kpis["partner"] == partner].copy()
fig = px.line(partner_kpis, x="date", y="revenue_usd", title="Daily Revenue")
st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Generate QBR (Gemini)")

# Prefer Streamlit Secrets; fallback to env var for local/Colab testing
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "") or os.environ.get("GEMINI_API_KEY", "")
model_name = st.text_input("Model", value="models/gemini-2.5-flash")

if st.button("Generate QBR Draft"):
    if not GEMINI_API_KEY:
        st.error("Missing GEMINI_API_KEY. Add it in Streamlit Secrets (or set env var locally).")
    else:
        client = genai.Client(api_key=GEMINI_API_KEY)
        snap, notes_list = normalize_for_json(df_snap, notes_recent, partner)

        prompt = f"""
You are an analytics & automation assistant writing a Quarterly Business Review (QBR) draft.

Rules:
- Use ONLY the provided metrics + notes. Do not invent facts.
- Be specific and action-oriented.
- If something can’t be concluded from the data, say “needs validation”.

Partner: {partner}

METRICS (snapshot):
{json.dumps(snap, indent=2)}

RECENT FIELD/OPS NOTES:
{json.dumps(notes_list, indent=2)}

Write in Markdown:
1) Executive summary (4-6 bullets)
2) What happened (key metrics, momentum)
3) Why it happened (tie to notes; if unclear say “needs validation”)
4) Risks
5) Next 30 days plan (bullets with suggested owners)
6) Questions to ask the partner (3-5)
""".strip()

        resp = client.models.generate_content(model=model_name, contents=prompt)
        qbr_md = resp.text

        st.success("Generated!")
        st.markdown(qbr_md)

        st.download_button(
            label="Download QBR Markdown",
            data=qbr_md,
            file_name=f"QBR_{partner}.md".replace(" ", "_"),
            mime="text/markdown"
        )
