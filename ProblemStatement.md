Big Data-Driven Portfolio Optimization Using Classical Models and High-Performance Computing

Problem Statement:

Financial markets produce massive datasets daily (stock prices, trading volume, company news, investor sentiment).

Traditional models like Markowitz Modern Portfolio Theory (MPT) assume small and clean datasets, and thus fail when applied to big, noisy data.

In this project, we deal with 10+ years of historical stock data (~3GB) combined with sentiment scores. Processing this sequentially is infeasible.

To handle such large-scale data, we use High-Performance Computing (HPC) with OpenMP (shared-memory parallelism) and MPI (distributed-memory parallelism).

Goal: Build a scalable, sentiment-augmented portfolio optimization pipeline that integrates big financial data and parallel computing for faster, reliable decision-making.


Phase 1: Data & Sentiment Extraction

Data Acquisition (Weeks 1–3)
Extracted daily OHLCV data (Open, High, Low, Close, Volume) for multiple NSE tickers.
Time span: 2000–2025 (~25 years).
Data collected using Yahoo Finance API (yfinance).
Stored as a multi-gigabyte dataset (~3GB) in CSV format.
Output file: india_stocks_4gb.csv.

Sentiment Dataset
Generated synthetic stock-related comments using GPT-2 (text generation model).
Classified each comment using FinBERT into:
Positive
Negative
Neutral

Output file: ticker_sentiments.csv
Structure: Ticker | Comment | Sentiment | Score

Phase 2 & Phase 3: HPC Preprocessing and Optimization
Phase 2: HPC Preprocessing (Weeks 4–7)

Implemented in C++17 + OpenMP for parallelism.
Key steps:
Robust parallel CSV reader (handles missing values and headers).
Deduplication by (Ticker, Date) → ensures only one record per trading day.
Join operation attaches sentiment scores to daily stock data.

MPI for distributed preprocessing:
Dataset split across multiple nodes by ticker or date range.
Each node preprocesses independently.
Results combined using MPI_Gather.
Final output: preprocessed_output.csv (~2 GB) (clean, joined dataset).


Phase 3: Portfolio Optimization & Parallel Modeling (Weeks 8–10)

Applied Markowitz Modern Portfolio Theory (MPT)
Parallelization:

MPI distributes covariance matrix computations across nodes.
OpenMP accelerates matrix multiplications inside each node.
Hybrid MPI + OpenMP → scalable optimization for thousands of assets.
Final output: Efficient Frontier (Risk vs Return curve).

Final Deliverables

✅ Cleaned & joined dataset (~2 GB) for ML and optimization.
✅ Sentiment-augmented dataset (ticker_sentiments.csv).
✅ Visualizations: Daily stock trends (Adani Green, Reliance, TCS).
✅ Efficient Frontier plot (Risk vs Return curve).
✅ Performance comparisons: Sequential vs OpenMP vs MPI.
✅ Scalability proof: HPC makes large-scale financial optimization feasible.

Project Timeline

Phase 1 (Weeks 1–3): Data Acquisition & Sentiment Extraction.
Phase 2 (Weeks 4–7): HPC Preprocessing (OpenMP + MPI).
Phase 3 (Weeks 8–10): Portfolio Optimization & Modeling and results.