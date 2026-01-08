"""
Pipeline PySpark IMDb : Raw -> Staging -> DW Core -> Marts BI.
Étapes :
1) Télécharger (optionnel) les fichiers TSV.GZ IMDb.
2) Lire la zone raw.
3) Nettoyer / caster en staging.
4) Construire les dimensions (année, titre, genre + bridge) et le fait fact_ratings.
5) Construire les marts (KPI par année, top par genre/année).
6) Exporter en Parquet (partitionné quand utile).

Exemple (local ou Colab) :
    python src/etl_imdb.py --raw-dir raw --dw-dir dw --marts-dir marts --download
"""

from __future__ import annotations

import argparse
import os
import socket
import socketserver
import sys
from pathlib import Path
from typing import Iterable, Optional


def _patch_socketserver_for_windows() -> None:
    """Ajouter des classes socket Unix factices sur Windows pour PySpark."""
    if hasattr(socketserver, "UnixStreamServer"):
        return

    class _DummyUnixStreamServer(socketserver.TCPServer):
        address_family = socket.AF_INET

    class _DummyUnixDatagramServer(socketserver.UDPServer):
        address_family = socket.AF_INET

    class _DummyThreadingUnixStreamServer(socketserver.ThreadingTCPServer):
        address_family = socket.AF_INET

    class _DummyThreadingUnixDatagramServer(socketserver.ThreadingUDPServer):
        address_family = socket.AF_INET

    socketserver.UnixStreamServer = _DummyUnixStreamServer
    socketserver.UnixDatagramServer = _DummyUnixDatagramServer
    socketserver.ThreadingUnixStreamServer = _DummyThreadingUnixStreamServer
    socketserver.ThreadingUnixDatagramServer = _DummyThreadingUnixDatagramServer


_patch_socketserver_for_windows()

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

IMDB_BASE_URL = "https://datasets.imdbws.com"
TITLE_BASICS = "title.basics.tsv.gz"
TITLE_RATINGS = "title.ratings.tsv.gz"
RAW_FILES = [TITLE_BASICS, TITLE_RATINGS]
JAVA_HOME_DEFAULT_WINDOWS = Path("C:/Program Files/Java/jdk1.8.0_66")
JAVA_HOME_DEFAULT_LINUX = Path("/usr/lib/jvm/java-11-openjdk-amd64")


def ensure_java_home() -> None:
    """S'assurer que JAVA_HOME/PATH pointent vers un JDK accessible."""
    default_home = JAVA_HOME_DEFAULT_WINDOWS if os.name == "nt" else JAVA_HOME_DEFAULT_LINUX
    java_home = Path(os.environ.get("JAVA_HOME", default_home))
    java_bin = java_home / "bin"
    java_exe = java_bin / ("java.exe" if os.name == "nt" else "java")

    if not java_exe.exists():
        raise FileNotFoundError(
            f"Java introuvable. Spécifiez JAVA_HOME ou installez un JDK (attendu: {java_exe})."
        )

    os.environ["JAVA_HOME"] = str(java_home)
    sep = ";" if os.name == "nt" else ":"
    os.environ["PATH"] = f"{java_bin}{sep}{os.environ.get('PATH','')}"


def build_spark(app_name: str = "imdb_etl") -> SparkSession:
    """Créer une session Spark locale avec des paramètres raisonnables."""
    ensure_java_home()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def ensure_files_exist(paths: Iterable[Path]) -> None:
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        missing_str = ", ".join(missing)
        raise FileNotFoundError(f"Fichiers manquants dans raw/: {missing_str}")


def download_imdb(raw_dir: Path, overwrite: bool = False) -> None:
    """Télécharger les fichiers IMDb TSV.GZ dans raw_dir si absents."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    for fname in RAW_FILES:
        url = f"{IMDB_BASE_URL}/{fname}"
        dest = raw_dir / fname
        if dest.exists() and not overwrite:
            print(f"[download] Skip existing {dest}")
            continue
        try:
            import requests

            print(f"[download] Fetch {url} -> {dest}")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1_048_576):
                        if chunk:
                            f.write(chunk)
        except Exception as exc:  # noqa: BLE001
            print(f"[download] Failed {url}: {exc}")
            raise


def read_tsv_gz(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.option("sep", "\t").option("header", "true").csv(str(path))


def replace_null_markers(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    return df.replace(to_replace={"\\N": None}, subset=list(columns))


def stage_titles(titles_raw: DataFrame) -> DataFrame:
    df = replace_null_markers(
        titles_raw,
        columns=["startYear", "runtimeMinutes", "genres", "primaryTitle", "originalTitle", "titleType"],
    )
    return (
        df.withColumn("startYear", F.col("startYear").cast("int"))
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
        .withColumn("isAdult", F.col("isAdult").cast("int"))
        .filter(F.col("titleType") == F.lit("movie"))
        .dropDuplicates(["tconst"])
    )


def stage_ratings(ratings_raw: DataFrame) -> DataFrame:
    return (
        ratings_raw.replace({"\\N": None}, subset=["averageRating", "numVotes"])  # just in case
        .withColumn("averageRating", F.col("averageRating").cast("double"))
        .withColumn("numVotes", F.col("numVotes").cast("int"))
        .dropDuplicates(["tconst"])
    )


def build_dim_year(titles_stg: DataFrame) -> DataFrame:
    return titles_stg.select(F.col("startYear").alias("year")).where(F.col("year").isNotNull()).dropDuplicates()


def build_dim_title(titles_stg: DataFrame) -> DataFrame:
    return titles_stg.select(
        F.col("tconst").alias("titlekey"),
        "primaryTitle",
        "originalTitle",
        "titleType",
        "startYear",
        "runtimeMinutes",
        "isAdult",
    )


def build_dim_genre_and_bridge(titles_stg: DataFrame) -> tuple[DataFrame, DataFrame]:
    title_genres = (
        titles_stg.select(
            F.col("tconst").alias("titlekey"),
            F.when(F.col("genres") == "\\N", None).otherwise(F.col("genres")).alias("genres"),
        )
        .where(F.col("genres").isNotNull())
        .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
        .withColumn("genre", F.trim(F.lower(F.col("genre"))))
    )

    dim_genre = title_genres.select("genre").dropDuplicates().withColumnRenamed("genre", "genrekey")
    bridge = title_genres.join(dim_genre, title_genres["genre"] == dim_genre["genrekey"], "inner")
    bridge = bridge.select("titlekey", "genrekey").dropDuplicates()
    return dim_genre, bridge


def build_fact_ratings(titles_stg: DataFrame, ratings_stg: DataFrame) -> DataFrame:
    joined = titles_stg.join(ratings_stg, "tconst", "inner")
    return joined.select(
        F.col("tconst").alias("titlekey"),
        F.col("startYear").alias("yearkey"),
        F.col("averageRating").alias("avg_rating"),
        F.col("numVotes").alias("num_votes"),
        F.col("runtimeMinutes").alias("runtime_min"),
    )


def build_mart_year_kpi(fact_ratings: DataFrame) -> DataFrame:
    return (
        fact_ratings.groupBy("yearkey")
        .agg(
            F.count("*").alias("n_movies"),
            F.avg("avg_rating").alias("mean_rating"),
            F.sum("num_votes").alias("total_votes"),
        )
        .orderBy("yearkey")
    )


def build_mart_top_genre_year(fact_ratings: DataFrame, bridge: DataFrame) -> DataFrame:
    joined = fact_ratings.join(bridge, "titlekey", "inner")
    w = Window.partitionBy("yearkey", "genrekey").orderBy(F.desc("num_votes"))
    ranked = joined.withColumn("rk", F.row_number().over(w))
    return ranked.where(F.col("rk") <= 10).select("yearkey", "genrekey", "titlekey", "avg_rating", "num_votes", "rk")


def write_parquet(df: DataFrame, path: Path, partition_cols: Optional[list[str]] = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(str(path))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="IMDb PySpark ETL pipeline")
    parser.add_argument("--raw-dir", type=Path, default=Path("raw"), help="Directory containing raw TSV.GZ files")
    parser.add_argument("--dw-dir", type=Path, default=Path("dw"), help="Output directory for DW core tables")
    parser.add_argument("--marts-dir", type=Path, default=Path("marts"), help="Output directory for BI marts")
    parser.add_argument("--download", action="store_true", help="Download IMDb files before running")
    parser.add_argument("--overwrite-download", action="store_true", help="Force re-download raw files")
    parser.add_argument("--show-counts", action="store_true", help="Afficher les volumes après chaque étape")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    if args.download:
        download_imdb(args.raw_dir, overwrite=args.overwrite_download)

    ensure_files_exist([args.raw_dir / TITLE_BASICS, args.raw_dir / TITLE_RATINGS])

    spark = build_spark()

    titles_raw = read_tsv_gz(spark, args.raw_dir / TITLE_BASICS)
    ratings_raw = read_tsv_gz(spark, args.raw_dir / TITLE_RATINGS)

    titles_stg = stage_titles(titles_raw)
    ratings_stg = stage_ratings(ratings_raw)

    dim_year = build_dim_year(titles_stg)
    dim_title = build_dim_title(titles_stg)
    dim_genre, bridge_title_genre = build_dim_genre_and_bridge(titles_stg)
    fact_ratings = build_fact_ratings(titles_stg, ratings_stg)

    mart_year_kpi = build_mart_year_kpi(fact_ratings)
    mart_top_genre_year = build_mart_top_genre_year(fact_ratings, bridge_title_genre)

    if args.show_counts:
        print("[stats] titles_stg:", titles_stg.count())
        print("[stats] ratings_stg:", ratings_stg.count())
        print("[stats] dim_year:", dim_year.count())
        print("[stats] dim_title:", dim_title.count())
        print("[stats] dim_genre:", dim_genre.count())
        print("[stats] bridge_title_genre:", bridge_title_genre.count())
        print("[stats] fact_ratings:", fact_ratings.count())
        print("[stats] mart_year_kpi:", mart_year_kpi.count())
        print("[stats] mart_top_genre_year:", mart_top_genre_year.count())

    args.dw_dir.mkdir(parents=True, exist_ok=True)
    args.marts_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(dim_year, args.dw_dir / "dim_year")
    write_parquet(dim_title, args.dw_dir / "dim_title")
    write_parquet(dim_genre, args.dw_dir / "dim_genre")
    write_parquet(bridge_title_genre, args.dw_dir / "bridge_title_genre")
    write_parquet(fact_ratings, args.dw_dir / "fact_ratings", partition_cols=["yearkey"])

    write_parquet(mart_year_kpi, args.marts_dir / "mart_year_kpi")
    write_parquet(mart_top_genre_year, args.marts_dir / "mart_top_genre_year")

    print("[done] DW written to", args.dw_dir)
    print("[done] Marts written to", args.marts_dir)


if __name__ == "__main__":
    main()
