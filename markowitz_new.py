#!/usr/bin/env python3
"""
markowitz_mpi_optimize.py

Performs portfolio optimization using predicted returns from parallel MPI models.
Uses Markowitz Modern Portfolio Theory with Ledoit–Wolf covariance shrinkage.
Compares ML-predicted portfolio vs historical mean-return portfolio.
Outputs results to mpi_outputs/ for later visualization.
"""

import os, time, warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.optimize import minimize
from sklearn.covariance import LedoitWolf

# ---------------- CONFIG ----------------
PRED_PATH = "mpi_outputs/predicted_returns_all.csv"
HIST_PATH = "stocks_merged_fixed.parquet"   # can also be your combined parquet
OUT_DIR   = "mpi_outputs"
TOP_K     = 300                             # top tickers by volatility
os.makedirs(OUT_DIR, exist_ok=True)

t0 = time.time()
print(f"1️⃣ Loading predicted returns from: {PRED_PATH}")

if not os.path.exists(PRED_PATH):
    raise FileNotFoundError(f"Predicted returns not found at {PRED_PATH}. Run parallel_models_mpi.py first.")

# ---------------- LOAD PREDICTED RETURNS ----------------
preds = pd.read_csv(PRED_PATH)
if 'PredictedReturn' not in preds.columns:
    print("⚠️ Could not detect correct columns. Showing first few:")
    print(preds.head(5))
    raise KeyError("Missing 'PredictedReturn' column in predictions file.")

preds = preds.dropna(subset=['PredictedReturn'])
preds = preds.groupby('Ticker', as_index=False).mean(numeric_only=True)
print(f"✅ Loaded predicted returns for {len(preds)} tickers.")

# ---------------- LOAD HISTORICAL RETURNS ----------------
print("2️⃣ Building historical returns pivot...")
try:
    import dask.dataframe as dd
    ddf = dd.read_parquet(HIST_PATH)
    df = ddf.compute()
except Exception as e:
    print(f"⚠️ Falling back to pandas: {e}")
    if HIST_PATH.endswith(".parquet"):
        df = pd.read_parquet(HIST_PATH)
    else:
        df = pd.read_csv(HIST_PATH, parse_dates=['Date'])

if not {'Ticker','Date','Close'}.issubset(df.columns):
    raise ValueError("Historical dataset missing Ticker/Date/Close columns.")

df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df = df.sort_values(['Ticker','Date']).dropna(subset=['Close'])
df['PrevClose'] = df.groupby('Ticker')['Close'].shift(1)
df['DailyReturn'] = (df['Close'] - df['PrevClose']) / df['PrevClose']
df = df.dropna(subset=['DailyReturn'])

pivot = (
    df.groupby(['Date','Ticker'])['DailyReturn']
    .mean()
    .reset_index()
    .pivot(index='Date', columns='Ticker', values='DailyReturn')
    .fillna(0)
)
print(f"✅ Pivot shape (dates x tickers): {pivot.shape}")
tickers = pivot.columns.intersection(preds['Ticker'])
pivot = pivot[tickers]
print(f"✅ Using {len(tickers)} tickers for optimization.")

# ---------------- SELECT TOP-K ----------------
vols = pivot.std()
top_tickers = vols.nlargest(TOP_K).index
pivot_sub = pivot[top_tickers]
print(f"Selecting top {TOP_K} tickers by variance for optimization.")

# ---------------- COVARIANCE SHINKAGE ----------------
print("3️⃣ Computing covariance matrix (Ledoit–Wolf shrinkage)...")
lw = LedoitWolf().fit(pivot_sub.fillna(0.0).values)
cov = lw.covariance_
mean_hist = pivot_sub.mean().values

# Align predicted means with selected tickers
mu_pred = preds.set_index('Ticker').loc[top_tickers, 'PredictedReturn'].fillna(0).values
mu_hist = mean_hist

# diagnostic stats
print(f"Diagnostic stats: mu_pred mean={mu_pred.mean():.6f}, std={mu_pred.std():.6f}, "
      f"min={mu_pred.min():.6f}, max={mu_pred.max():.6f}")
print(f"Covariance cond. number: {np.linalg.cond(cov):.2e}")

# ---------------- MARKOWITZ OPTIMIZATION ----------------
def portfolio_stats(weights, mean_returns, cov_matrix, rf=0.0):
    w = np.array(weights)
    ret = np.dot(w, mean_returns)
    vol = np.sqrt(np.dot(w, cov_matrix.dot(w)))
    sharpe = (ret - rf) / vol if vol > 0 else 0
    return ret, vol, sharpe

def neg_sharpe(weights, mean_returns, cov_matrix):
    return -portfolio_stats(weights, mean_returns, cov_matrix)[2]

def optimize_portfolio(mu, cov):
    n = len(mu)
    x0 = np.ones(n) / n
    bounds = tuple((0, 1) for _ in range(n))
    cons = ({'type':'eq', 'fun': lambda w: np.sum(w) - 1},)
    try:
        res = minimize(neg_sharpe, x0, args=(mu, cov), method='SLSQP',
                       bounds=bounds, constraints=cons, tol=1e-8)
        if not res.success:
            print("⚠️ Optimizer failed:", res.message)
        return res.x
    except Exception as e:
        print("⚠️ Optimization crashed:", e)
        # fallback heuristic: weight top positive returns
        w = np.maximum(mu, 0)
        if w.sum() == 0:
            w = np.ones_like(mu)
        w /= w.sum()
        return w

print("4️⃣ Running Markowitz optimization (max Sharpe)...")
w_pred = optimize_portfolio(mu_pred, cov)
w_hist = optimize_portfolio(mu_hist, cov)

# ---------------- BACKTEST ----------------
print("5️⃣ Computing portfolio metrics...")
def backtest(weights, pivot_data):
    R = pivot_data.fillna(0.0).values
    daily = R.dot(weights)
    cum_ret = np.prod(1 + daily) - 1
    ann_ret = (1 + cum_ret) ** (252/len(daily)) - 1
    vol = np.std(daily) * np.sqrt(252)
    sharpe = (np.mean(daily)*252) / vol if vol > 0 else 0
    return {
        "Cumulative Return": cum_ret,
        "Annual Return": ann_ret,
        "Annual Volatility": vol,
        "Sharpe": sharpe
    }

res_pred = backtest(w_pred, pivot_sub)
res_hist = backtest(w_hist, pivot_sub)

# ---------------- EFFICIENT FRONTIER ----------------
print("6️⃣ Computing efficient frontier...")
try:
    n = len(mu_pred)
    target_returns = np.linspace(mu_pred.min(), mu_pred.max(), 30)
    frontier_risk, frontier_ret = [], []
    for tr in target_returns:
        cons = (
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},
            {'type': 'eq', 'fun': lambda w: np.dot(w, mu_pred) - tr}
        )
        res = minimize(lambda w: np.sqrt(np.dot(w, cov.dot(w))),
                       x0=np.ones(n)/n,
                       bounds=tuple((0, 1) for _ in range(n)),
                       constraints=cons, tol=1e-6)
        if res.success:
            frontier_risk.append(np.sqrt(np.dot(res.x, cov.dot(res.x))))
            frontier_ret.append(tr)
    if len(frontier_risk) > 0:
        plt.figure(figsize=(8,5))
        plt.plot(frontier_risk, frontier_ret, label="Efficient Frontier")
        plt.scatter(res_pred['Annual Volatility'], res_pred['Annual Return'],
                    c='red', label='Predicted Opt', marker='*', s=120)
        plt.scatter(res_hist['Annual Volatility'], res_hist['Annual Return'],
                    c='blue', label='Historical Opt', marker='o', s=80)
        plt.xlabel("Volatility (Risk)")
        plt.ylabel("Return")
        plt.title("Efficient Frontier")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(OUT_DIR, "efficient_frontier.png"), dpi=150)
        plt.close()
        print("💾 Saved efficient_frontier.png")
    else:
        print("⚠️ Could not compute efficient frontier (insufficient variation).")
except Exception as e:
    print("⚠️ Could not compute efficient frontier:", e)

# ---------------- SAVE RESULTS ----------------
weights_df = pd.DataFrame({
    "Ticker": top_tickers,
    "Predicted_Weight": w_pred,
    "Historical_Weight": w_hist
})
weights_df.to_csv(os.path.join(OUT_DIR, "weights_all.csv"), index=False)
pd.DataFrame([res_pred, res_hist], index=["Predicted","Historical"]).to_csv(
    os.path.join(OUT_DIR, "portfolio_summary.csv")
)

elapsed = time.time() - t0
print(f"\n✅ Done. Time elapsed: {elapsed:.2f}s")
print("📊 Predicted Portfolio:", res_pred)
print("📊 Historical Portfolio:", res_hist)
print(f"💾 Saved weights -> {OUT_DIR}/weights_all.csv")
print(f"💾 Saved summary -> {OUT_DIR}/portfolio_summary.csv")
