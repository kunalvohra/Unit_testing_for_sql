import os
import glob

def discover_table_parquet_info(base_folder: str, table: str):
    info = {"default": None, "cases": {}}
    if not os.path.isdir(base_folder):
        return info
    default = os.path.join(base_folder, f"{table}.csv")
    if os.path.exists(default):
        info["default"] = default
    pattern = os.path.join(base_folder, f"{table}_case*.csv")
    for p in sorted(glob.glob(pattern)):
        fname = os.path.basename(p)
        part = fname.split(".")[0]
        if "_" in part:
            caseid = part.split("_", 1)[1]
        else:
            caseid = "default"
        info["cases"][caseid] = p
    return info

def resolve_parquet_for_case(info: dict, caseid: str):
    if caseid in info.get("cases", {}):
        return info["cases"][caseid]
    return info.get("default")

def load_csv_as_df(spark, path: str):
    return spark.read.csv(path, header=True, inferSchema=True)
