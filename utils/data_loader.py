import os
import glob

def load_csv_as_df(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def discover_table_parquet_info(folder, table):
    result = {"default": None, "cases": {}}

    default_path = os.path.join(folder, f"{table}.csv")
    if os.path.exists(default_path):
        result["default"] = default_path

    for p in glob.glob(os.path.join(folder, f"{table}_case*.csv")):
        file = os.path.basename(p)
        cid = file.split("case")[-1].split(".")[0]
        result["cases"][cid] = p

    return result

def resolve_parquet_for_case(info, caseid):
    return info["cases"].get(caseid)
