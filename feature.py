# Cell 1 - inspect partition dtypes and examples for Sentiment
import pandas as pd, numpy as np
def part_info(part):
    # return dtype and up-to-5 non-null unique sample values (as strings)
    s = part['Sentiment']
    vals = s.dropna().astype(str).head(5).tolist()
    return pd.Series({
        "len": len(part),
        "dtype": str(s.dtype),
        "nnull": int(s.isnull().sum()),
        "examples": ",".join(vals[:5])
    })

info = ddf.map_partitions(part_info).compute()
print("Per-partition Sentiment info (first 10 partitions):")
print(info.head(10))
# Show global dtypes
print("\nGlobal dtypes:")
print(ddf.dtypes)

# Cell 2 - coerce Sentiment to string per-partition (safe), coerce other columns too
import dask.dataframe as dd

# Ensure Date is datetime
ddf['Date'] = dd.to_datetime(ddf['Date'], errors='coerce')

# Per-partition conversion for Sentiment: fillna, convert to str
def fix_sentiment(part):
    part = part.copy()
    part['Sentiment'] = part['Sentiment'].fillna('').astype(str)
    return part

# apply per-partition
ddf = ddf.map_partitions(fix_sentiment, meta=ddf._meta)

# Coerce numeric columns to stable types
ddf['Volume'] = ddf['Volume'].fillna(0).astype('int64')
ddf['SentimentScore'] = ddf['SentimentScore'].fillna(0.0).astype('float64')

# Ensure return columns are float
for col in ['PrevClose','DailyReturn','FutureReturn']:
    if col in ddf.columns:
        ddf[col] = ddf[col].astype('float64')

# Label to int8
if 'LabelUp' in ddf.columns:
    ddf['LabelUp'] = ddf['LabelUp'].astype('int8')

print("Converted partition dtypes (lazy):")
print(ddf.dtypes)
# Persist small amount to ensure conversion works (optional)
_dd = ddf.head(1)   # triggers a small compute to validate
print("Head row after coercion:", _dd)

# Cell 3 - persist and write using pyarrow with explicit schema
import pyarrow as pa, shutil, os

out_path = "/content/drive/MyDrive/stocks_processed_parquet_combined_fixed/"
if os.path.exists(out_path):
    shutil.rmtree(out_path)

# define explicit pyarrow schema (match your columns)
schema = pa.schema([
    ("Ticker", pa.string()),
    ("Date", pa.timestamp("ns")),
    ("Close", pa.float64()),
    ("High", pa.float64()),
    ("Low", pa.float64()),
    ("Open", pa.float64()),
    ("Volume", pa.int64()),
    ("Sentiment", pa.string()),
    ("SentimentScore", pa.float64()),
    ("PrevClose", pa.float64()),
    ("DailyReturn", pa.float64()),
    ("FutureReturn", pa.float64()),
    ("LabelUp", pa.int8())
])

# persist ddf to memory/dask workers (speeds up write and ensures types settled)
ddf = ddf.persist()
print("Persisted Dask dataframe with", ddf.npartitions, "partitions")

# Write with explicit schema (pyarrow)
ddf.to_parquet(out_path, engine="pyarrow", schema=schema, write_index=False, overwrite=True)
print("✅ Wrote parquet to:", out_path)

# Quick verify
import dask.dataframe as dd
test = dd.read_parquet(out_path)
print("Read back dtypes:")
print(test.dtypes)
print("Sample rows:")
print(test.head(5))


import dask.dataframe as dd

# Load the corrected combined parquet
test_ddf = dd.read_parquet("/content/drive/MyDrive/stocks_processed_parquet_combined_fixed/")

# Filter for top tickers (for modeling)
top_tickers = (
    test_ddf.groupby("Ticker")["Volume"]
    .mean()
    .nlargest(50)
    .compute()
    .index
)

# Convert to pandas (safe now because we only load top tickers)
pandas_df = test_ddf[test_ddf["Ticker"].isin(top_tickers)].compute()
print("✅ Loaded subset safely:", len(pandas_df), "rows and", pandas_df['Ticker'].nunique(), "tickers")

# Sort, select features, split 80/20 by time
pandas_df = pandas_df.sort_values(["Ticker","Date"]).reset_index(drop=True)
feature_cols = ["DailyReturn","SentimentScore","Volume"]
df = pandas_df.dropna(subset=feature_cols + ["FutureReturn"]).reset_index(drop=True)

dates = np.sort(df["Date"].unique())
split_date = dates[int(0.8*len(dates))]
train_df = df[df["Date"] <= split_date].reset_index(drop=True)
test_df  = df[df["Date"] >  split_date].reset_index(drop=True)
print("Train:", len(train_df), "Test:", len(test_df))

X_train = train_df[feature_cols].fillna(0).values
y_reg_train = train_df["FutureReturn"].values
y_clf_train = train_df["LabelUp"].values
X_test = test_df[feature_cols].fillna(0).values
y_reg_test = test_df["FutureReturn"].values
y_clf_test = test_df["LabelUp"].values

scaler = StandardScaler().fit(X_train)
X_train_s, X_test_s = scaler.transform(X_train), scaler.transform(X_test)
joblib.dump(scaler, "scaler.joblib")
print("Shapes:", X_train_s.shape, X_test_s.shape)
