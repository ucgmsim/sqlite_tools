import sqlite3
from pathlib import Path
from typing import Annotated

import typer
import pandas as pd

from sqlite_tools import query

app = typer.Typer()


def _connect_db(db_path: Path) -> sqlite3.Connection:
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database {db_path}: {e}")
        raise typer.Exit(code=1)


@app.command()
def cpt_measurements(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    nzgd_id: Annotated[int, typer.Option(help="The NZGD ID to query.")],
):
    """
    Extracts CPT measurements for a given NZGD ID and prints them to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.cpt_measurements_for_one_nzgd(nzgd_id, conn)
        print(df.to_csv(index=False))
    finally:
        conn.close()


@app.command()
def spt_measurements(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    nzgd_id: Annotated[int, typer.Option(help="The NZGD ID to query.")],
):
    """
    Extracts SPT measurements for a given NZGD ID and prints them to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.spt_measurements_for_one_nzgd(nzgd_id, conn)
        print(df.to_csv(index=False))
    finally:
        conn.close()


@app.command()
def spt_soil_types(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    nzgd_id: Annotated[int, typer.Option(help="The NZGD ID to query.")],
):
    """
    Extracts SPT soil types for a given NZGD ID and prints them to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.spt_soil_types_for_one_nzgd(nzgd_id, conn)
        print(df.to_csv(index=False))
    finally:
        conn.close()


@app.command()
def cpt_vs30s(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    nzgd_id: Annotated[int, typer.Option(help="The NZGD ID to query.")],
):
    """
    Extracts CPT Vs30 estimates for a given NZGD ID and prints them to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.cpt_vs30s_for_one_nzgd_id(nzgd_id, conn)
        print(df.to_csv(index=False))
    finally:
        conn.close()


@app.command()
def spt_vs30s(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    nzgd_id: Annotated[int, typer.Option(help="The NZGD ID to query.")],
):
    """
    Extracts SPT Vs30 estimates for a given NZGD ID and prints them to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.spt_vs30s_for_one_nzgd_id(nzgd_id, conn)
        print(df.to_csv(index=False))
    finally:
        conn.close()


@app.command()
def all_vs30s(
    db_path: Annotated[
        Path,
        typer.Option(
            help="Path to the SQLite database file.", exists=True, file_okay=True, dir_okay=False, readable=True
        ),
    ],
    vs30_correlation: Annotated[str, typer.Option(help="Selected Vs to Vs30 correlation name.")],
    cpt_to_vs_correlation: Annotated[str, typer.Option(help="Selected CPT to Vs correlation name.")],
    spt_to_vs_correlation: Annotated[str, typer.Option(help="Selected SPT to Vs correlation name.")],
    hammer_type: Annotated[str, typer.Option(help="Selected hammer type name.")],
):
    """
    Extracts all CPT and SPT Vs30 data based on selected correlations and hammer type, then prints to CSV.
    """
    conn = _connect_db(db_path)
    try:
        df = query.all_vs30s_given_correlations(
            selected_vs30_correlation=vs30_correlation,
            selected_cpt_to_vs_correlation=cpt_to_vs_correlation,
            selected_spt_to_vs_correlation=spt_to_vs_correlation,
            selected_hammer_type=hammer_type,
            conn=conn,
        )
        print(df.to_csv(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    app()
