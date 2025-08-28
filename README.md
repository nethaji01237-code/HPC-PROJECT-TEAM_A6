# HPC-PROJECT-TEAM_A6
Big Data-Driven Portfolio Optimization Using Classical Models and High-Performance Computing
📌 Overview

This project focuses on Portfolio Optimization using classical models (e.g., Markowitz) enhanced with Big Data and High-Performance Computing (HPC) techniques.
We collected and processed 10+ years of stock market data (~3GB) along with financial sentiment analysis to build a dataset for trend prediction and portfolio management.

By integrating Yahoo Finance stock data with sentiment analysis (FinBERT + GPT2), and using parallel preprocessing (C++ OpenMP), we demonstrate how big data and HPC can transform traditional financial optimization methods.

👨‍💻 Team Contributions (Team - 6)

Nethaji PA – CB.AI.U4AID23024
Implemented parallel preprocessing in C++17 with OpenMP.

Vishal K S – CB.AI.U4AID23052
Extracted sentiment scores & synthetic label datasets.
Used FinBERT for sentiment classification and GPT2 for synthetic comments
Built the ticker_sentiments.csv dataset for integration with stock prices.

Jayanthi Vinay Kumar – CB.AI.U4AID23053
Implemented graph extraction and visualization using matplotlib.
Generated daily trend plots for multiple tickers (e.g., Adani Green, Reliance).

Nishanth N – CB.AI.U4AID23068
Extracted price dataset (~3GB) from Yahoo Finance using yfinance.
Automated large-scale data download with ticker handling.

✨ Key Features

📊 Dataset Extraction: Downloaded multi-year OHLCV stock data (~3GB) from Yahoo Finance.
📰 Sentiment Analysis:
Generated pseudo-comments using GPT2.
Classified sentiment using FinBERT (positive/negative/neutral).

⚡ HPC Preprocessing:
Implemented in C++17 + OpenMP.
Parallel CSV reading, deduplication (Ticker+Date), dataset join.
Reduced preprocessing time significantly on large datasets.

📈 Visualization: Stock trend graphs plotted using Matplotlib.

🧮 Optimization: Foundation for applying Markowitz Portfolio Theory on cleaned data.
--------