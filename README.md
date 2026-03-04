# Basemakers MVP — One-Click QBR Agent (Notion + SQL + Streamlit + Gemini)

Live app: https://qrb-mvp.streamlit.app/

This project is a lightweight MVP of an internal “One-Click QBR Agent”.It demonstrates:
- pulling partner context (qualitative notes) from **Notion**
- modeling KPIs with **SQL (CTEs + window functions)** using **DuckDB**
- presenting an interactive **Streamlit + Plotly** data product
- generating a QBR draft with **Gemini** based strictly on the computed metrics + notes

---

## What it does (end-to-end)

1) **Partner Notes (Notion)**
- A Notion database stores partner notes and field/ops updates (Partner, Date, Tags, Priority, Owner, Note).
- Notes are exported into a Parquet dataset used by the app.

2) **Performance Metrics (Shopify-like simulation)**
- The project uses a small simulated daily metrics dataset (revenue, units, OOS rate, stores covered) to mimic commerce signals.
- The connector layer is designed so a real Shopify/Salesforce/Repsly connector can replace the simulated dataset later.

3) **KPI Modeling (DuckDB SQL)**
- KPIs are built using SQL transformations:
  - rolling **7-day** and **28-day** revenue
  - **week-over-week** change
  - rolling OOS averages
  - simple momentum label (Up/Flat/Down)

4) **QBR Draft Generation (Gemini)**
- The app bundles:
  - partner KPI snapshot (last 28 days)
  - most recent partner notes (last 28 days)
- Gemini generates a QBR draft in Markdown:
  - Executive summary
  - What happened (metrics)
  - Why it happened (grounded to notes; flags “needs validation” if unclear)
  - Risks
  - Next 30-day plan
  - Partner questions

---

## Tech Stack

- **Python**: data extraction, automation, app logic
- **Notion API**: partner notes source (integration token)
- **DuckDB**: in-app analytics modeling with advanced SQL
- **Streamlit**: interactive data product UI
- **Plotly**: KPI visualization
- **Gemini API**: QBR draft generation (tested with `models/gemini-2.5-flash`)
- **Parquet**: lightweight, fast local data storage for MVP

---
