from spark import get_spark
from jdbc import load_tables
import queries
from cli import parse_args
import os
from dotenv import load_dotenv
import shutil


load_dotenv()


QUERY_MAP = {
    1: queries.query_1,
    2: queries.query_2,
    3: queries.query_3,
    4: queries.query_4,
    5: queries.query_5,
    6: queries.query_6,
    7: queries.query_7
}


def main():
    args = parse_args()

    if args.q not in QUERY_MAP:
        raise ValueError("Unknown query number")

    spark_tmp_path = os.getenv("SPARK_TMP_PATH")

    if os.path.exists(spark_tmp_path):
        try:
            shutil.rmtree(spark_tmp_path)
        except PermissionError:
            print(f"Failed to delete {spark_tmp_path}, the file may be in use by a process")

    spark = get_spark()
    tables = load_tables(spark)

    df = QUERY_MAP[args.q](tables)

    df.show(truncate=False)

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{args.out}/query_{args.q}")
    )

    spark.stop()


if __name__ == "__main__":
    main()
