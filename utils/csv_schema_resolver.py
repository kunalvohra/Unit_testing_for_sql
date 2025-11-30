import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, BooleanType, TimestampType, StringType

_NUMERIC_INT = re.compile(r'^-?\d+$')
_NUMERIC_FLOAT = re.compile(r'^-?\d+\.\d+$')
_ISO_TS = re.compile(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2})?$')

def _infer_type_from_value(val: str):
    if val is None:
        return None
    s = str(val).strip()
    if s == '':
        return None
    s_low = s.lower()
    if s_low in ('true', 'false', 't', 'f', '1', '0'):
        return BooleanType()
    if _NUMERIC_INT.match(s):
        return IntegerType()
    if _NUMERIC_FLOAT.match(s):
        return DoubleType()
    if _ISO_TS.match(s):
        return TimestampType()
    return StringType()

def _most_specific_type(types):
    if any(isinstance(t, BooleanType) for t in types if t): return BooleanType()
    if any(isinstance(t, IntegerType) for t in types if t): return IntegerType()
    if any(isinstance(t, LongType) for t in types if t): return LongType()
    if any(isinstance(t, DoubleType) for t in types if t): return DoubleType()
    if any(isinstance(t, TimestampType) for t in types if t): return TimestampType()
    return StringType()

def normalize_csv_df(df, table_name=None, sample_rows=100):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    sample = df.limit(sample_rows).collect()
    inferred = {}
    for c in df.columns:
        vals = [row[c] for row in sample if c in row and row[c] is not None]
        types = []
        for v in vals:
            t = _infer_type_from_value(v)
            if t:
                types.append(t)
        inferred[c] = _most_specific_type(types) if types else StringType()
    for c, t in inferred.items():
        if isinstance(t, IntegerType):
            df = df.withColumn(c, F.col(c).cast('int'))
        elif isinstance(t, LongType):
            df = df.withColumn(c, F.col(c).cast('long'))
        elif isinstance(t, DoubleType):
            df = df.withColumn(c, F.col(c).cast('double'))
        elif isinstance(t, BooleanType):
            df = df.withColumn(c, F.when(F.lower(F.col(c).cast('string')).isin('true','t','1'),'true').otherwise(F.when(F.lower(F.col(c).cast('string')).isin('false','f','0'),'false').otherwise(None)))
            df = df.withColumn(c, F.col(c).cast('boolean'))
        elif isinstance(t, TimestampType):
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
        else:
            df = df.withColumn(c, F.col(c).cast('string'))
    return df
