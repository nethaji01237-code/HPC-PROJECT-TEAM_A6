# 🚀 Big Data–Driven Portfolio Optimization using Classical Models & High-Performance Computing

### Team A6 — HPC Project (2025)

This repository showcases the complete workflow of **Portfolio Optimization enhanced by High-Performance Computing (HPC)**, **Big Data analytics**, and **Financial Sentiment Modeling**.  
We bridge classical financial theory with modern parallel computing to efficiently analyze large-scale financial datasets and optimize asset allocation strategies.

---

## 🧩 Problem Statement

> Traditional portfolio optimization methods (like **Markowitz’s Modern Portfolio Theory**) struggle with **large datasets**, **real-time processing**, and **multi-factor data (e.g., sentiment)**.  
>  
> This project aims to integrate **Big Data**, **sentiment-driven signals**, and **parallel processing (HPC)** to accelerate and enhance portfolio construction accuracy.

---

## ⚙️ Project Architecture

┌────────────────────────────┐
│ Data Collection (Yahoo) │ ← 3GB of OHLCV data
└────────────┬───────────────┘
│
▼
┌────────────────────────────┐
│ Sentiment Analysis (FinBERT + GPT-2) │
└────────────┬───────────────┘
│
▼
┌────────────────────────────┐
│ Parallel Preprocessing (C++17 + OpenMP) │
└────────────┬───────────────┘
│
▼
┌────────────────────────────┐
│ Feature Engineering (Python + Dask) │
└────────────┬───────────────┘
│
▼
┌────────────────────────────┐
│ Optimization Models (Markowitz, HRP) │
└────────────┬───────────────┘
│
▼
┌────────────────────────────┐
│ Parallel Model Execution (MPI) │



---

## 💡 Core Objectives

1. **Integrate Classical Financial Models with HPC**  
   Apply **Markowitz** and **Hierarchical Risk Parity (HRP)** models to a 10-year dataset using **parallelization**.

2. **Leverage Big Data Techniques**  
   Manage and process large financial datasets (~3GB) efficiently using **Python (Dask, Pandas, PyArrow)** and **C++ OpenMP**.

3. **Incorporate Sentiment Analysis**  
   Combine **FinBERT** and **GPT-2** to generate and classify sentiment-driven features, improving market trend understanding.

4. **Accelerate Computation via MPI**  
   Use **MPI (mpi4py)** for distributed model evaluation to demonstrate speedup and scalability.

---

## 🧠 Technical Stack

| Layer | Technology |
|-------|-------------|
| Data Collection | `yfinance`, `pandas`, `joblib`, `tqdm` |
| NLP/Sentiment | `FinBERT`, `transformers`, `GPT-2`, `PyTorch` |
| Big Data Processing | `Dask`, `PyArrow`, `FastParquet` |
| HPC Preprocessing | `C++17`, `OpenMP` |
| Parallel Execution | `MPI`, `mpi4py` |
| Visualization | `Matplotlib`, `NetworkX` |
| Optimization Models | `Markowitz`, `HRP`, `Regression-based Backtesting` |

---

## 📊 Dataset Workflow

1. **Data Extraction (`dataset.py`)**
   - Automated extraction of multi-year OHLCV data for NSE tickers using `yfinance`.
   - Handles ticker failures and missing values gracefully.
   - Produces a unified dataset for downstream analysis.

2. **Sentiment Integration (`ticket_sentiment_analysis.py`)**
   - Generated synthetic comments using **GPT-2**.
   - Classified sentiment into Positive / Neutral / Negative using **FinBERT**.
   - Combined sentiment scores with financial time series data.

3. **Parallel Preprocessing (`preprocessing.cpp`)**
   - Implemented using **C++17** and **OpenMP**.
   - Handles CSV reading, deduplication (`Ticker + Date`), and joins in parallel.
   - Demonstrated major performance improvement vs sequential Python preprocessing.

4. **Feature Engineering (`feature.py`)**
   - Merges sentiment and OHLCV datasets.
   - Computes derived metrics (returns, volatility, moving averages, etc.).
   - Saves cleaned, feature-rich datasets for model training.

5. **Portfolio Modeling**
   - **`markowitz_new.py`**: Implements Markowitz optimization with efficient frontier visualization.  
   - **`hierarchial_risk_parity.ipynb`**: Uses HRP for cluster-based portfolio construction.
   - **`parallel_models_mpi.py`**: Runs both models in parallel using MPI.

6. **Backtesting (`regression_Markowitz_backtesting.ipynb`)**
   - Evaluates models using regression-based techniques for performance validation.



---

## ⚡ High-Performance Implementation

### 🔹 OpenMP (C++ Preprocessing)
Used for fast CSV and data operations:

```cpp
#pragma omp parallel for
for (int i = 0; i < files.size(); i++) {
    // Parallel file processing
}

🔹 MPI (Parallel Model Execution)
mpiexec -n 4 python3 parallel_models_mpi.py

👨‍💻 Team Contributions (Team - 6)

Nethaji PA – CB.AI.U4AID23024 Implemented parallel preprocessing in C++17 with OpenMP. and markowitz_optimization and Hierarchial 
risk parity and mpi way of implementation

Vishal K S – CB.AI.U4AID23052 Extracted sentiment scores & synthetic label datasets. Used FinBERT for sentiment classification and GPT2 for synthetic comments Built the ticker_sentiments.csv dataset for integration with stock prices. and built 2 classification model and the resulting plots

Jayanthi Vinay Kumar – CB.AI.U4AID23053 Implemented graph extraction and visualization using matplotlib. Generated daily trend plots for multiple tickers (e.g., Adani Green, Reliance).

Nishanth N – CB.AI.U4AID23068 Extracted price dataset (~3GB) from Yahoo Finance using yfinance. Automated large-scale data download with ticker handling. EDA and Feature engineering

📈 Visualization

Examples include:

Stock trend plots for each ticker.

Portfolio efficient frontier comparison (Markowitz vs HRP).




🧩 Future Enhancements

Integration with Quantum Portfolio Optimization (QAOA, VQE).

Real-time portfolio rebalancing with Kafka + Dask streaming.

Deployment as a Flask API for dashboard visualization.

Extend sentiment models with Llama 3 and FinGPT.


