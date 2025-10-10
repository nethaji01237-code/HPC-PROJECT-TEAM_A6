#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <omp.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct Stock {
    std::string Date;
    std::string Ticker;
    double Price{};
    double Close{};
    double Open{};
    double High{};
    double Low{};
    double Volume{};
};

struct Sentiment {
    std::string Ticker;
    std::string Comment;
    std::string SentimentScore;
};

struct JoinedRecord {
    Stock stock;
    std::vector<Sentiment> sentiments;
};

/* ---------------- Utility Functions ---------------- */

static inline std::string trim(const std::string &s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace((unsigned char)s[a])) ++a;
    while (b > a && std::isspace((unsigned char)s[b - 1])) --b;
    return s.substr(a, b - a);
}

static inline bool iequals(const std::string &a, const std::string &b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (std::tolower((unsigned char)a[i]) != std::tolower((unsigned char)b[i]))
            return false;
    return true;
}

static inline double safe_stod(const std::string &field) {
    std::string t = trim(field);
    if (t.empty() || iequals(t, "NA") || iequals(t, "NaN") ||
        iequals(t, "NULL") || iequals(t, "null"))
        return 0.0;
    std::string cleaned;
    cleaned.reserve(t.size());
    for (char c : t)
        if (c != ',') cleaned.push_back(c);
    try {
        return std::stod(cleaned);
    } catch (...) {
        return 0.0;
    }
}

/* ---------------- CSV Splitter (handles quotes safely) ---------------- */

static inline std::vector<std::string> splitCSVLine(const std::string &line) {
    std::vector<std::string> out;
    std::string cur;
    bool inQuotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (inQuotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    cur.push_back('"');
                    ++i;
                } else {
                    inQuotes = false;
                }
            } else
                cur.push_back(c);
        } else {
            if (c == '"')
                inQuotes = true;
            else if (c == ',') {
                out.push_back(cur);
                cur.clear();
            } else
                cur.push_back(c);
        }
    }
    out.push_back(cur);
    return out;
}

/* ---------------- Column Detection ---------------- */

static inline int find_col(const std::vector<std::string> &headers,
                           const std::vector<std::string> &candidates) {
    auto lower = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return s;
    };
    std::map<std::string, int> hmap;
    for (int i = 0; i < (int)headers.size(); ++i)
        hmap[lower(headers[i])] = i;
    for (const auto &c : candidates) {
        auto it = hmap.find(lower(c));
        if (it != hmap.end()) return it->second;
    }
    return -1;
}

/* ---------------- Read Stocks CSV ---------------- */

std::vector<Stock> readStocksCSV(const std::string &filename, int threads) {
    std::ifstream file(filename);
    if (!file) throw std::runtime_error("Cannot open stock CSV: " + filename);

    std::string headerLine;
    if (!std::getline(file, headerLine)) throw std::runtime_error("Empty CSV file");

    auto headers = splitCSVLine(headerLine);
    int idxDate = find_col(headers, {"Date", "DATE", "Timestamp", "datetime"});
    int idxTicker = find_col(headers, {"Ticker", "Symbol", "SYMBOL"});
    int idxClose = find_col(headers, {"Close", "Adj Close", "Last", "LTP"});
    int idxOpen = find_col(headers, {"Open", "OPEN"});
    int idxHigh = find_col(headers, {"High", "HIGH"});
    int idxLow = find_col(headers, {"Low", "LOW"});
    int idxVol = find_col(headers, {"Volume", "VOL", "Total Trade Quantity"});

    if (idxTicker < 0 || idxClose < 0)
        throw std::runtime_error("Critical columns missing (Ticker/Close)");

    std::vector<std::string> lines;
    std::string line;
    while (std::getline(file, line))
        if (!line.empty()) lines.push_back(std::move(line));

    std::vector<Stock> stocks;
    stocks.resize(lines.size());

#pragma omp parallel for num_threads(threads) schedule(static)
    for (size_t i = 0; i < lines.size(); ++i) {
        auto f = splitCSVLine(lines[i]);
        if (f.empty()) continue;
        Stock s;
        s.Date = (idxDate >= 0 && idxDate < (int)f.size()) ? trim(f[idxDate]) : "Unknown";
        s.Ticker = (idxTicker >= 0 && idxTicker < (int)f.size()) ? trim(f[idxTicker]) : "Unknown";
        s.Close = (idxClose >= 0 && idxClose < (int)f.size()) ? safe_stod(f[idxClose]) : 0.0;
        s.Open = (idxOpen >= 0 && idxOpen < (int)f.size()) ? safe_stod(f[idxOpen]) : 0.0;
        s.High = (idxHigh >= 0 && idxHigh < (int)f.size()) ? safe_stod(f[idxHigh]) : 0.0;
        s.Low = (idxLow >= 0 && idxLow < (int)f.size()) ? safe_stod(f[idxLow]) : 0.0;
        s.Volume = (idxVol >= 0 && idxVol < (int)f.size()) ? safe_stod(f[idxVol]) : 0.0;
        s.Price = s.Close;
        stocks[i] = std::move(s);
    }

    stocks.erase(std::remove_if(stocks.begin(), stocks.end(),
                                [](const Stock &s) { return s.Ticker == "Unknown"; }),
                 stocks.end());
    return stocks;
}

/* ---------------- Read Sentiments CSV ---------------- */

std::vector<Sentiment> readSentimentsCSV(const std::string &filename) {
    std::ifstream file(filename);
    if (!file) throw std::runtime_error("Cannot open sentiments CSV: " + filename);

    std::vector<Sentiment> sentiments;
    std::string line;
    bool headerSkipped = false;
    while (std::getline(file, line)) {
        if (!headerSkipped) {
            headerSkipped = true;
            continue;
        }
        auto f = splitCSVLine(line);
        if (f.size() < 3) f.resize(3);
        Sentiment s;
        s.Ticker = trim(f[0]);
        s.Comment = f[1].empty() ? "Unknown" : f[1];
        s.SentimentScore = f[2].empty() ? "0.0" : trim(f[2]);
        if (!s.Ticker.empty()) sentiments.push_back(std::move(s));
    }
    return sentiments;
}

/* ---------------- Deduplication ---------------- */

std::vector<Stock> dedup_stocks_by_ticker_date(const std::vector<Stock> &items) {
    std::unordered_set<std::string> seen;
    std::vector<Stock> out;
    out.reserve(items.size());
    for (const auto &s : items) {
        std::string key = s.Ticker + "|" + s.Date;
        if (seen.insert(key).second)
            out.push_back(s);
    }
    return out;
}

template <typename T, typename KeyFn>
std::vector<T> dedup_generic(const std::vector<T> &items, KeyFn key_fn) {
    std::unordered_set<std::string> seen;
    std::vector<T> unique;
    unique.reserve(items.size());
    for (const auto &it : items) {
        auto k = key_fn(it);
        if (seen.insert(k).second) unique.push_back(it);
    }
    return unique;
}

/* ---------------- Join Datasets in Parallel ---------------- */

std::vector<JoinedRecord> joinDatasets(const std::vector<Stock> &stocks,
                                       const std::vector<Sentiment> &sentiments,
                                       int threads) {
    std::unordered_map<std::string, std::vector<Sentiment>> smap;
    smap.reserve(sentiments.size() * 2);
    for (const auto &s : sentiments)
        smap[s.Ticker].push_back(s);

    std::vector<JoinedRecord> joined(stocks.size());

#pragma omp parallel for num_threads(threads) schedule(static)
    for (size_t i = 0; i < stocks.size(); ++i) {
        JoinedRecord jr;
        jr.stock = stocks[i];
        auto it = smap.find(stocks[i].Ticker);
        if (it != smap.end())
            jr.sentiments = it->second;
        joined[i] = std::move(jr);
    }
    return joined;
}

/* ---------------- Exporters ---------------- */

void exportCSV(const std::vector<Stock> &stocks, const std::vector<Sentiment> &sentiments,
               const std::vector<JoinedRecord> &joined) {
    std::ofstream st("deduped_stocks.csv");
    st << "Date,Ticker,Price,Close,Open,High,Low,Volume\n";
    for (const auto &s : stocks)
        st << s.Date << "," << s.Ticker << "," << s.Price << "," << s.Close << ","
           << s.Open << "," << s.High << "," << s.Low << "," << s.Volume << "\n";

    std::ofstream se("deduped_sentiments.csv");
    se << "Ticker,Comment,SentimentScore\n";
    for (const auto &s : sentiments)
        se << s.Ticker << ",\"" << s.Comment << "\"," << s.SentimentScore << "\n";

    std::ofstream out("preprocessed_output.csv");
    out << "Date,Ticker,Price,Close,Open,High,Low,Volume,Sentiments\n";
    for (const auto &r : joined) {
        out << r.stock.Date << "," << r.stock.Ticker << "," << r.stock.Price << ","
            << r.stock.Close << "," << r.stock.Open << "," << r.stock.High << ","
            << r.stock.Low << "," << r.stock.Volume << ",\"";
        for (size_t j = 0; j < r.sentiments.size(); ++j) {
            out << r.sentiments[j].Comment;
            if (j + 1 < r.sentiments.size()) out << " | ";
        }
        out << "\"\n";
    }
}

/* ---------------- Main ---------------- */

int main(int argc, char **argv) {
    double start_time = omp_get_wtime();
    std::string stock_file = (argc >= 2) ? argv[1] : "india_stocks.csv";
    std::string senti_file = (argc >= 3) ? argv[2] : "ticker_sentiments.csv";
    int threads = (argc >= 4) ? std::stoi(argv[3]) : omp_get_max_threads();
    omp_set_num_threads(threads);

    std::cout << "Using " << threads << " threads\n";

    try {
        auto stocks = readStocksCSV(stock_file, threads);
        auto sentiments = readSentimentsCSV(senti_file);

        std::cout << "Loaded " << stocks.size() << " stock rows and " << sentiments.size()
                  << " sentiment rows\n";

        stocks = dedup_stocks_by_ticker_date(stocks);
        sentiments =
            dedup_generic(sentiments, [](const Sentiment &s) { return s.Ticker + "|" + s.Comment; });

        std::cout << "After dedup: " << stocks.size() << " stocks, " << sentiments.size()
                  << " sentiments\n";

        auto joined = joinDatasets(stocks, sentiments, threads);
        std::cout << "Join produced " << joined.size() << " records\n";

        exportCSV(stocks, sentiments, joined);
        std::cout << "✅ Preprocessing complete.\n";
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    double end_time = omp_get_wtime();
    std::cout << "Total runtime: " << (end_time - start_time) << " seconds\n";
    return 0;
}
