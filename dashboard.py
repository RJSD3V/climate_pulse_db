"""ClimateWorks DB dashboard.

    streamlit run dashboard.py

Reads the dbt marts from the database in $CLIMATE_DB (default: dev_database.duckdb) and
schema $CLIMATE_SCHEMA (default: dev_sode). For MotherDuck production:

    CLIMATE_DB=md:climate_works CLIMATE_SCHEMA=prod_sode streamlit run dashboard.py
"""

from __future__ import annotations

import os

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DB = os.getenv("CLIMATE_DB", "dev_database.duckdb")
SCHEMA = os.getenv("CLIMATE_SCHEMA", "dev_sode")
MAX_COUNTRIES = 8

# Categorical palette, light and dark steps (validated for colour-vision deficiency).
PALETTE = {
    "light": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"],
    "dark": ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"],
}
MUTED = "#898781"
# Diverging poles for anomalies: warmer = red, cooler = blue.
WARM = {"light": "#e34948", "dark": "#e66767"}
COOL = {"light": "#2a78d6", "dark": "#3987e5"}

st.set_page_config(page_title="ClimateWorks DB", page_icon="🌡️", layout="wide")


def theme_mode() -> str:
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except AttributeError:
        return "light"


MODE = theme_mode()


@st.cache_resource
def connection() -> duckdb.DuckDBPyConnection:
    read_only = not DB.startswith("md:")
    return duckdb.connect(DB, read_only=read_only)


@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    return connection().sql(sql).df()


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    monthly = query(
        f"""
        select *, coalesce(country_name, country_code) as country
        from {SCHEMA}.fct_monthly_country_climate
        order by month_start
        """
    )
    stations = query(
        f"""
        select station_id, station_name, latitude, longitude,
               coalesce(country_name, country_code) as country, observation_days
        from {SCHEMA}.dim_stations
        where observation_days > 0
        """
    )
    monthly["month_start"] = pd.to_datetime(monthly["month_start"])
    return monthly, stations


def stable_colors(selected: list[str]) -> dict[str, str]:
    """Give each country a palette slot that it keeps for as long as it stays selected,
    so removing one country never repaints the others."""
    slots: dict[str, int] = st.session_state.setdefault("color_slots", {})
    for country in list(slots):
        if country not in selected:
            del slots[country]
    for country in selected:
        if country not in slots:
            used = set(slots.values())
            slots[country] = next(i for i in range(MAX_COUNTRIES) if i not in used)
    return {c: PALETTE[MODE][slots[c]] for c in selected}


def field(name: str) -> str:
    """Escape characters Vega-Lite treats as field-path syntax."""
    return name.replace(".", "\\.").replace("[", "\\[").replace("]", "\\]")


def series_chart(df, y, y_title, series, colors, value_format=".1f", height=320):
    """Multi-series line chart with a hover crosshair whose tooltip lists every series."""
    domain = list(colors)
    hover = alt.selection_point(fields=["month_start"], nearest=True, on="pointerover", empty=False)
    base = alt.Chart(df).encode(x=alt.X("month_start:T", title=None, axis=alt.Axis(grid=False)))
    lines = base.mark_line(strokeWidth=2, interpolate="monotone").encode(
        y=alt.Y(f"{y}:Q", title=y_title, axis=alt.Axis(tickCount=5)),
        color=alt.Color(
            f"{series}:N",
            scale=alt.Scale(domain=domain, range=[colors[c] for c in domain]),
            legend=alt.Legend(orient="top", title=None, symbolType="stroke", symbolStrokeWidth=3),
        ),
    )
    points = lines.mark_point(size=70, filled=True, opacity=1).transform_filter(hover)
    rule = (
        base.transform_pivot(series, value=y, groupby=["month_start"])
        .mark_rule(color=MUTED, strokeWidth=1)
        .encode(
            opacity=alt.condition(hover, alt.value(0.7), alt.value(0)),
            tooltip=[alt.Tooltip("month_start:T", title="Month", format="%b %Y")]
            + [alt.Tooltip(f"{field(c)}:Q", title=c, format=value_format) for c in domain],
        )
        .add_params(hover)
    )
    return (lines + points + rule).properties(height=height)


# --------------------------------------------------------------------------------------
st.title("ClimateWorks DB")
st.caption(
    "Daily weather from NOAA's Global Historical Climatology Network (GHCN-D), quality-controlled "
    "and modelled with dbt. Values are monthly averages across every station that reported in a "
    "country, so they shift a little as stations join or leave the network."
)

try:
    monthly, stations = load_data()
except duckdb.Error as exc:
    st.error(
        f"Couldn't read the dbt marts from `{DB}` (schema `{SCHEMA}`).\n\n"
        "Build them first:\n\n```\npython get_data.py 2023\ndbt build\n```\n\n"
        f"Details: {exc}"
    )
    st.stop()

if monthly.empty:
    st.warning("The marts are empty. Load some data with `python get_data.py <year>` and run `dbt build`.")
    st.stop()

# Countries ranked by how much data they have, so the defaults are well-covered countries.
ranked = (
    monthly.groupby("country")["station_days"].sum().sort_values(ascending=False).index.tolist()
)
months = sorted(monthly["month_start"].dt.date.unique())

# ---- Filters (one row, above the charts) ----
f1, f2 = st.columns([3, 2])
with f1:
    selected = st.multiselect(
        "Countries",
        options=ranked,
        default=ranked[:3],
        max_selections=MAX_COUNTRIES,
        help=f"Up to {MAX_COUNTRIES}. Ordered by amount of data.",
    )
with f2:
    if len(months) > 1:
        start, end = st.select_slider(
            "Months",
            options=months,
            value=(months[0], months[-1]),
            format_func=lambda d: d.strftime("%b %Y"),
        )
    else:
        start = end = months[0]
        st.markdown(f"**Months**  \n{start:%b %Y}")

if not selected:
    st.info("Pick at least one country.")
    st.stop()

colors = stable_colors(selected)
in_range = monthly["month_start"].dt.date.between(start, end)
view = monthly[in_range & monthly["country"].isin(selected)]
view_stations = stations[stations["country"].isin(selected)]

# ---- Headline numbers ----
k1, k2, k3, k4 = st.columns(4)
k1.metric("Stations reporting", f"{view_stations['station_id'].nunique():,}")
k2.metric("Countries", f"{len(selected)}")
k3.metric("Months", f"{view['month_start'].nunique():,}")
k4.metric("Station-days", f"{int(view['station_days'].sum()):,}")

# ---- Temperature and precipitation by country ----
c1, c2 = st.columns(2)
with c1:
    st.subheader("Average daily temperature")
    st.altair_chart(
        series_chart(view, "avg_tavg_c", "°C", "country", colors),
        width="stretch",
    )
with c2:
    st.subheader("Average daily precipitation")
    st.altair_chart(
        series_chart(view, "avg_daily_precipitation_mm", "mm per day", "country", colors),
        width="stretch",
    )

# ---- One country in detail ----
st.divider()
focus = st.selectbox("Country detail", options=selected)
detail = view[view["country"] == focus]

d1, d2 = st.columns(2)
with d1:
    st.subheader(f"Highs and lows: {focus}")
    long = detail.melt(
        id_vars=["month_start"],
        value_vars=["avg_tmax_c", "avg_tmin_c"],
        var_name="measure",
        value_name="temperature_c",
    )
    long["measure"] = long["measure"].map({"avg_tmax_c": "Avg daily high", "avg_tmin_c": "Avg daily low"})
    hilo_colors = {"Avg daily high": PALETTE[MODE][1], "Avg daily low": PALETTE[MODE][0]}
    st.altair_chart(
        series_chart(long, "temperature_c", "°C", "measure", hilo_colors),
        width="stretch",
    )

with d2:
    st.subheader(f"Yearly temperature anomaly: {focus}")
    full = monthly[monthly["country"] == focus].copy()
    full["year"] = full["month_start"].dt.year
    full["weighted"] = full["avg_tavg_c"] * full["temperature_station_days"]
    yearly = full.groupby("year").agg(
        months=("month_start", "nunique"),
        weighted=("weighted", "sum"),
        days=("temperature_station_days", "sum"),
    )
    yearly = yearly[(yearly["months"] == 12) & (yearly["days"] > 0)]
    if len(yearly) < 3:
        st.info(
            "Needs at least 3 complete years for this country. "
            "Load more years, e.g. `python get_data.py 2000 2024`, then run `dbt build`."
        )
    else:
        yearly["tavg_c"] = yearly["weighted"] / yearly["days"]
        yearly["anomaly_c"] = yearly["tavg_c"] - yearly["tavg_c"].mean()
        yearly["direction"] = yearly["anomaly_c"].map(lambda a: "Warmer than average" if a >= 0 else "Cooler than average")
        yearly = yearly.reset_index()
        bars = (
            alt.Chart(yearly)
            .mark_bar(cornerRadiusEnd=4, binSpacing=2)
            .encode(
                x=alt.X("year:O", title=None, axis=alt.Axis(labelAngle=0, labelOverlap=True)),
                y=alt.Y("anomaly_c:Q", title="°C vs. average of years shown"),
                color=alt.Color(
                    "direction:N",
                    scale=alt.Scale(
                        domain=["Warmer than average", "Cooler than average"],
                        range=[WARM[MODE], COOL[MODE]],
                    ),
                    legend=alt.Legend(orient="top", title=None),
                ),
                tooltip=[
                    alt.Tooltip("year:O", title="Year"),
                    alt.Tooltip("tavg_c:Q", title="Avg temperature (°C)", format=".2f"),
                    alt.Tooltip("anomaly_c:Q", title="Anomaly (°C)", format="+.2f"),
                ],
            )
        )
        zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color=MUTED).encode(y="y:Q")
        st.altair_chart((bars + zero).properties(height=320), width="stretch")
        st.caption("Complete years only. Averages are weighted by station-days.")

# ---- Stations ----
st.divider()
st.subheader("Reporting stations")
st.caption("Coloured to match each country's line in the charts above.")
st.map(
    view_stations.assign(color=view_stations["country"].map(colors)),
    latitude="latitude",
    longitude="longitude",
    size=2000,
    color="color",
)

# ---- Table view (every chart's numbers) ----
with st.expander("Data table"):
    st.dataframe(
        view[
            [
                "country",
                "month_start",
                "station_count",
                "avg_tavg_c",
                "avg_tmax_c",
                "avg_tmin_c",
                "max_tmax_c",
                "min_tmin_c",
                "avg_daily_precipitation_mm",
            ]
        ].sort_values(["country", "month_start"]),
        hide_index=True,
        column_config={
            "month_start": st.column_config.DateColumn("Month", format="MMM YYYY"),
            "avg_tavg_c": st.column_config.NumberColumn("Avg temp (°C)", format="%.1f"),
            "avg_tmax_c": st.column_config.NumberColumn("Avg high (°C)", format="%.1f"),
            "avg_tmin_c": st.column_config.NumberColumn("Avg low (°C)", format="%.1f"),
            "max_tmax_c": st.column_config.NumberColumn("Highest (°C)", format="%.1f"),
            "min_tmin_c": st.column_config.NumberColumn("Lowest (°C)", format="%.1f"),
            "avg_daily_precipitation_mm": st.column_config.NumberColumn("Precip (mm/day)", format="%.2f"),
        },
        width="stretch",
    )

st.caption(
    "Source: NOAA GHCN-Daily, https://docs.opendata.aws/noaa-ghcn-pds/readme.html. "
    "Values that failed any NOAA quality check are excluded."
)
