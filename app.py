from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import altair as alt

DB_PATH = Path(__file__).resolve().parent / "warehouse/charlton.duckdb"

st.set_page_config(page_title="Charlton Season Tracker", layout="wide")
st.title("Charlton Athletic — Season Tracker")

if not DB_PATH.exists():
    st.error(f"Database not found at {DB_PATH}. Run the ingest pipeline first.")
    st.stop()

con = duckdb.connect(str(DB_PATH), read_only=True)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Current league table")
    table = con.execute("select * from mart_league_table_current").df()
    st.dataframe(table, use_container_width=True, hide_index=True)

with col2:
    st.subheader("Charlton position through time")
    pos = con.execute("select * from mart_team_position_through_time").df()
    if not pos.empty:
        teams_in_league = con.execute("select count(*) as n from mart_league_table_current").fetchone()[0]
        y_max = int(teams_in_league) if teams_in_league and teams_in_league > 0 else int(pos["position"].max())
        chart = (
            alt.Chart(pos)
            .mark_line(point=True)
            .encode(
                x=alt.X("matchday:Q", title="Matchday"),
                y=alt.Y(
                    "position:Q",
                    title="Position",
                    scale=alt.Scale(reverse=True, domain=[1, y_max]),
                    axis=alt.Axis(values=list(range(1, y_max + 1))),
                ),
                tooltip=["matchday", "position", "points", "gd", "as_of_date"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
        st.caption("Lower is better (1st at the top). Y-axis reversed.")
    else:
        st.info("No Charlton data found for current competition/season config.")

st.subheader("Last 5 games")
last5 = con.execute("select * from mart_team_last_5").df()
st.dataframe(last5, use_container_width=True, hide_index=True)

st.subheader("Match detail")
match_ids = last5["match_id"].tolist() if not last5.empty else []
selected = st.selectbox("Select a match", match_ids)

if selected:
    detail = con.execute(
        """
        select
          match_id, utc_date, status, matchday,
          home_team_name, away_team_name,
          home_score_full, away_score_full,
          home_score_half, away_score_half,
          winner, last_updated_utc
        from stg_raw_matches
        where match_id = ?
        """,
        [int(selected)]
    ).df()
    st.dataframe(detail, use_container_width=True, hide_index=True)
