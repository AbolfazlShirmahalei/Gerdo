from typing import List, Tuple, Type

from pyspark.sql import DataFrame, SparkSession


def display_query_result_line_by_line(
    query_result: List[Tuple[Type]]
):
    for row in query_result:
        print(row)


def read_query(query_path: str) -> str:
    with open(query_path, "r") as file:
        query = file.read()

    return query


def get_spark_session(
    spark_threads: int = 2,
    spark_driver_memory: int = 5,
    spark_max_result_size: int = 4,
) -> SparkSession:
    return (
        SparkSession.builder.master(f"local[{spark_threads}]")
        .config("spark.driver.memory", f"{spark_driver_memory}g")
        .config("spark.driver.maxResultSize", f"{spark_max_result_size}g")
        .config("spark.sql.session.timeZone", "Asia/Tehran")
        .getOrCreate()
    )


def display_df(
    df: DataFrame,
    show_schema: bool = True,
    display: bool = False,
    show_count: int = 5,
):
    if show_schema:
        df.printSchema()
    if display:
        print("DataFrame size:", df.count())
        df.show(show_count, False)

