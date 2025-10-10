#!/usr/bin/env python3
"""
parallel_models_mpi.py

MPI-parallel training & prediction script (per-rank subset of tickers).

Usage:
  mpiexec --oversubscribe -n 4 python3 -u parallel_models_mpi.py

This script:
 - Loads the dataset (parquet preferred, falls back to CSV)
 - Each MPI rank takes a slice of tickers
 - Handles missing values robustly (ffill/bfill then rolling mean)
 - Trains linear / lasso / random-forest regressors and logistic / RF classifier
 - Produces per-ticker averaged predictions and saves per-rank CSV
 - Rank 0 combines all per-rank CSVs into mpi_outputs/predicted_returns_all.csv
"""
import os, socket
# determine per-node allocation; set manually or compute from env
# Example: set threads per rank to 4
os.environ["OMP_NUM_THREADS"]    = "4"
os.environ["MKL_NUM_THREADS"]    = "4"
os.environ["OPENBLAS_NUM_THREADS"]= "4"
os.environ["NUMEXPR_NUM_THREADS"] = "4"






from mpi4py import MPI
import os, sys, time, warnings, math
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, Lasso, LogisticRegression
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
import joblib

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# ---------- CONFIG ----------
DATA_PATH = os.environ.get("DATA_PATH", "stocks_merged_fixed.parquet")
OUT_DIR = "mpi_outputs"
os.makedirs(OUT_DIR, exist_ok=True)
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)

t_start = time.time()
if rank == 0:
    print(f"🚀 MPI RandomForest + regressors: started with {size} ranks; DATA_PATH={DATA_PATH}")

# ---------- Utility: read dataset (parquet or csv) ----------
def read_dataset(path):
    if path.endswith(".parquet") or path.endswith(".parq"):
        return pd.read_parquet(path)
    else:
        # robust CSV reader: allow large file, parse dates if possible
        return pd.read_csv(path, parse_dates=['Date'], low_memory=False, encoding='utf-8', errors='replace')

# ---------- Try read and broadcast error if missing ----------
try:
    # Only read small metadata on rank 0? Simpler: each rank reads entire file (safe on local)
    df = read_dataset(DATA_PATH)
except Exception as e:
    # broadcast failure to other ranks then exit gracefully
    if rank == 0:
        print(f"❌ Could not read DATA_PATH={DATA_PATH}: {e}", file=sys.stderr)
    comm.Barrier()
    sys.exit(1)

# ---------- Basic sanity & ensure columns present ----------
required_cols = {"Ticker", "Date", "Close", "Open", "High", "Low", "Volume"}
missing = required_cols - set(df.columns)
if missing:
    # not fatal but warn
    if rank == 0:
        print(f"⚠️ Warning: dataset missing columns: {missing} - continue, some features may be absent")

# ---------- Select tickers assigned to this rank ----------
all_tickers = sorted(df['Ticker'].dropna().unique())
if len(all_tickers) == 0:
    if rank == 0:
        print("❌ No tickers found in dataset. Exiting.")
    comm.Barrier()
    sys.exit(0)

# Balanced split
def chunked(lst, n):
    k = len(lst) // n
    parts = [lst[i*k:(i+1)*k] for i in range(n-1)]
    parts.append(lst[(n-1)*k:])
    return parts

parts = chunked(all_tickers, size)
my_tickers = parts[rank] if rank < len(parts) else []

if not my_tickers:
    print(f"⚠️ Rank {rank} has no tickers assigned; exiting.")
    comm.Barrier()
    sys.exit(0)

df = df[df['Ticker'].isin(my_tickers)].copy()
if df.empty:
    print(f"⚠️ Rank {rank}: after filtering for assigned tickers found no rows. Exiting.")
    comm.Barrier()
    sys.exit(0)

# ---------- Convert types & sort ----------
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

# ---------- Missing value handling ----------
def fill_missing(group):
    # group is a DataFrame for a single ticker
    # 1) forward/backward fill on timeseries columns
    group = group.copy()
    # prefer .ffill()/.bfill() over deprecated fillna(method=...)
    group = group.ffill().bfill()

    # 2) for numeric columns still missing, use small rolling mean (window=3)
    numeric_cols = ['Close','Open','High','Low','Volume','SentimentScore']
    for c in numeric_cols:
        if c in group.columns:
            # coerce to numeric then fillna with rolling mean
            group[c] = pd.to_numeric(group[c], errors='coerce')
            if group[c].isna().any():
                group[c] = group[c].fillna(group[c].rolling(3, min_periods=1).mean())
    return group

# Use groupby.apply but make it explicit and safe: apply to each group and reset_index
# This avoids ambiguous behavior in future pandas releases
grp = df.groupby('Ticker', group_keys=False)
df = grp.apply(lambda g: fill_missing(g.reset_index(drop=True))).reset_index(drop=True)

# ---------- Feature engineering: returns and labels ----------
df['PrevClose'] = df.groupby('Ticker')['Close'].shift(1)
df['DailyReturn'] = (df['Close'] - df['PrevClose']) / df['PrevClose']
df['FutureReturn'] = df.groupby('Ticker')['DailyReturn'].shift(-1)
# Label for classification (up next day)
df['LabelUp'] = (df['FutureReturn'] > 0).astype('int8')

# Drop rows without the key targets
df = df.dropna(subset=['DailyReturn','FutureReturn']).reset_index(drop=True)
if df.empty:
    print(f"⚠️ Rank {rank}: no usable rows after computing returns. Exiting.")
    comm.Barrier()
    sys.exit(0)

# ---------- Modeling features ----------
feature_cols = []
# Choose a small stable feature set; skip missing features gracefully
for c in ['DailyReturn', 'SentimentScore', 'Volume']:
    if c in df.columns:
        feature_cols.append(c)
if not feature_cols:
    print(f"❌ Rank {rank}: no feature columns found among expected {['DailyReturn','SentimentScore','Volume']}. Exiting.")
    comm.Barrier()
    sys.exit(1)

X = df[feature_cols].fillna(0.0).astype(float).values
y_reg = df['FutureReturn'].values
y_clf = df['LabelUp'].values

# Scale features
scaler = StandardScaler()
Xs = scaler.fit_transform(X)

# ---------- Instantiate models with safer settings ----------
# Regression models
lr = LinearRegression()
lasso = Lasso(alpha=5e-4, max_iter=20000)
rf_reg = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=RANDOM_SEED, n_jobs=1)

# Classification models
logr = LogisticRegression(solver='saga', max_iter=5000, random_state=RANDOM_SEED)
rf_clf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=RANDOM_SEED, n_jobs=1, class_weight='balanced')

# Fit models (wrapped in try/except so a single model failure won't stop the rank)
trained = {}
t0 = time.time()
try:
    lr.fit(Xs, y_reg); trained['lr'] = lr
except Exception as e:
    print(f"⚠️ Rank {rank}: LinearRegression failed: {e}")
try:
    lasso.fit(Xs, y_reg); trained['lasso'] = lasso
except Exception as e:
    print(f"⚠️ Rank {rank}: Lasso failed: {e}")
try:
    rf_reg.fit(Xs, y_reg); trained['rf_reg'] = rf_reg
except Exception as e:
    print(f"⚠️ Rank {rank}: RF reg failed: {e}")
try:
    logr.fit(Xs, y_clf); trained['logr'] = logr
except Exception as e:
    print(f"⚠️ Rank {rank}: Logistic failed: {e}")
try:
    rf_clf.fit(Xs, y_clf); trained['rf_clf'] = rf_clf
except Exception as e:
    print(f"⚠️ Rank {rank}: RF clf failed: {e}")

train_secs = time.time() - t0

# ---------- Predictions ----------

preds = pd.DataFrame({
    'Ticker': df['Ticker'].values,
    'Date': df['Date'].values
})

# regression preds
if 'rf_reg' in trained:
    preds['PredRF'] = trained['rf_reg'].predict(Xs)
else:
    preds['PredRF'] = np.nan

if 'lasso' in trained:
    preds['PredLasso'] = trained['lasso'].predict(Xs)
else:
    preds['PredLasso'] = np.nan

if 'lr' in trained:
    preds['PredLR'] = trained['lr'].predict(Xs)
else:
    preds['PredLR'] = np.nan

# classification prob (prob of up)
if 'rf_clf' in trained:
    try:
        preds['PredUpProb'] = trained['rf_clf'].predict_proba(Xs)[:,1]
    except Exception:
        preds['PredUpProb'] = np.nan
elif 'logr' in trained:
    try:
        preds['PredUpProb'] = trained['logr'].predict_proba(Xs)[:,1]
    except Exception:
        preds['PredUpProb'] = np.nan
else:
    preds['PredUpProb'] = np.nan

# ---------- Aggregate per-ticker mean predictions ----------
agg = preds.groupby('Ticker').agg({
    'PredRF': 'mean',
    'PredLasso': 'mean',
    'PredLR': 'mean',
    'PredUpProb': 'mean'
}).rename_axis('Ticker').reset_index()

# ensemble predicted return: weighted average (adjust weights as you like)
w_rf, w_lasso, w_lr, w_prob = 0.4, 0.25, 0.25, 0.10
# Note: PredUpProb is a probability, not a return; we map it to a tiny expected-return effect:
# normalize PredUpProb -> small adjustment
if 'PredUpProb' in agg.columns:
    # center probability -> from 0.5 baseline
    agg['PredUpAdj'] = (agg['PredUpProb'] - 0.5) * 0.001
else:
    agg['PredUpAdj'] = 0.0

# Ensure numeric columns exist and fillna with 0
for col in ['PredRF','PredLasso','PredLR','PredUpAdj']:
    if col not in agg.columns:
        agg[col] = 0.0
agg = agg.fillna(0.0)

agg['PredictedReturn'] = (w_rf*agg['PredRF'] + w_lasso*agg['PredLasso'] + w_lr*agg['PredLR']) + w_prob*agg['PredUpAdj']

# ---------- Save per-rank results ----------
out_path = os.path.join(OUT_DIR, f"predictions_rank{rank}.csv")
agg.to_csv(out_path, index=False)
elapsed = time.time() - t_start
print(f"✅ Rank {rank}: processed {len(my_tickers)} tickers, rows={len(df)} -> saved {out_path} (secs: {train_secs:.1f}, total elapsed: {elapsed:.1f})")

comm.Barrier()

# ---------- Rank 0: combine all parts ----------
if rank == 0:
    import glob
    parts = sorted(glob.glob(os.path.join(OUT_DIR, "predictions_rank*.csv")))
    if not parts:
        print("❌ No prediction parts found; aborting combine.")
        sys.exit(1)

    combined = []
    for p in parts:
        try:
            part = pd.read_csv(p)
            if 'Ticker' not in part.columns:
                # attempt fallback
                part.columns = ['Ticker','PredRF','PredLasso','PredLR','PredUpProb','PredictedReturn'][:part.shape[1]]
            combined.append(part)
        except Exception as e:
            print("⚠️ Failed to read part", p, ":", e)

    if not combined:
        print("❌ Nothing to combine after reading parts.")
        sys.exit(1)

    df_all = pd.concat(combined, axis=0, ignore_index=True)
    # If duplicates (same ticker across ranks), groupby ticker mean
    final = df_all.groupby('Ticker', as_index=False).mean(numeric_only=True)
    # Round columns for neatness
    for c in ['PredRF','PredLasso','PredLR','PredUpProb','PredictedReturn']:
        if c in final.columns:
            final[c] = final[c].astype(float)

    out_all = os.path.join(OUT_DIR, "predicted_returns_all.csv")
    final.to_csv(out_all, index=False)
    total_elapsed = time.time() - t_start
    print(f"🏁 Combined: {out_all} (tickers: {len(final)}, elapsed: {total_elapsed:.1f}s)")

comm.Barrier()
