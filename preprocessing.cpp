

#include <algorithm>
#include <cctype>
#include <cmath>        // for std::floor
#include <fstream>
#include <iostream>
#include <map>
#include <omp.h>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct Stock {
    std::string Date;   // <-- keep date as string (ISO or whatever your CSV has)
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
    std::vector<Sentiment> sentiments; // ticker-level sentiments repeated per day
};

/* ----------------- helpers ----------------- */

static inline std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace((unsigned char)s[a])) ++a;
    while (b > a && std::isspace((unsigned char)s[b - 1])) --b;
    return s.substr(a, b - a);
}

static inline bool iequals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (std::tolower((unsigned char)a[i]) != std::tolower((unsigned char)b[i]))
            return false;
    return true;
}

static inline double safe_stod(const std::string& field) {
    std::string t = trim(field);
    if (t.empty()) return 0.0;
    if (iequals(t, "NA") || iequals(t, "NaN") || iequals(t, "NULL") || iequals(t, "null"))
        return 0.0;
    // remove thousands separators
    std::string cleaned; cleaned.reserve(t.size());
    for (char c : t) if (c != ',') cleaned.push_back(c);
    try { return std::stod(cleaned); } catch (...) { return 0.0; }
}

/* ----------------- CSV split (handles quotes) ----------------- */
static inline std::vector<std::string> splitCSVLine(const std::string& line) {
    std::vector<std::string> out;
    std::string cur;
    bool inQuotes = false;
    size_t N = line.size();
    if (N && (line.back()=='\r' || line.back()=='\n')) --N;

    for (size_t i=0;i<N;++i) {
        char c = line[i];
        if (inQuotes) {
            if (c=='"') {
                if (i+1<N && line[i+1]=='"') { cur.push_back('"'); ++i; }
                else inQuotes=false;
            } else cur.push_back(c);
        } else {
            if (c=='"') inQuotes=true;
            else if (c==',') { out.push_back(cur); cur.clear(); }
            else cur.push_back(c);
        }
    }
    out.push_back(cur);
    return out;
}

/* ----------------- header utilities ----------------- */
static inline int find_col(const std::vector<std::string>& headers,
                           const std::vector<std::string>& candidates) {
    auto lower = [](std::string s){ for (auto& c: s) c=(char)std::tolower((unsigned char)c); return s; };
    std::map<std::string,int> hmap;
    for (int i=0;i<(int)headers.size();++i) hmap[lower(headers[i])] = i;
    for (const auto& c : candidates) {
        auto it = hmap.find(lower(c));
        if (it != hmap.end()) return it->second;
    }
    return -1;
}

static inline void print_headers(const std::vector<std::string>& headers) {
    std::cerr << "=== Headers (index : name) ===\n";
    for (size_t i=0;i<headers.size();++i) {
        std::cerr << i << " : " << headers[i] << "\n";
    }
    std::cerr << "==============================\n";
}

static inline void print_first_row_preview(std::ifstream& file,
                                           const std::vector<std::string>& headers) {
    std::streampos pos = file.tellg();
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        auto cells = splitCSVLine(line);
        file.clear();
        file.seekg(pos);
        std::cerr << "=== First data row (index : value) ===\n";
        for (size_t i=0;i<cells.size();++i) {
            std::cerr << i << " : " << cells[i] << "\n";
        }
        std::cerr << "======================================\n";
        return;
    }
    file.clear();
    file.seekg(pos);
}

/* ----------------- readers ----------------- */

std::vector<Stock> readStocksCSV(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) throw std::runtime_error("Failed to open stocks CSV: " + filename);

    std::vector<Stock> stocks;
    std::string line;

    if (!std::getline(file, line)) return stocks;
    auto headers = splitCSVLine(line);

    // Inspect columns
    print_headers(headers);
    print_first_row_preview(file, headers);

    // Detect key columns
    int idxDate   = find_col(headers, {"Date","DATE","date","Timestamp","timestamp","Datetime","datetime"});
    int idxTicker = find_col(headers, {"Ticker","SYMBOL","Symbol","Security","SECURITY","TickerSymbol","Symbol Name","SYMBOL NAME"});
    int idxClose  = find_col(headers, {"Close","CLOSE","close","Last","LAST","Adj Close","AdjClose","LTP","Last Traded Price","Close Price"});
    int idxOpen   = find_col(headers, {"Open","OPEN","open","Open Price"});
    int idxHigh   = find_col(headers, {"High","HIGH","high","High Price"});
    int idxLow    = find_col(headers, {"Low","LOW","low","Low Price"});
    int idxVol    = find_col(headers, {"Volume","VOL","volume","Shares Traded","Total Trade Quantity","Traded Volume","Volume Traded"});

    // Price = Close/Last by default (common for EOD)
    int idxPrice = idxClose;

    std::cerr << "[stocks] mapping -> "
              << "Date:"   << idxDate   << " "
              << "Ticker:" << idxTicker << " "
              << "Price:"  << idxPrice  << " "
              << "Close:"  << idxClose  << " "
              << "Open:"   << idxOpen   << " "
              << "High:"   << idxHigh   << " "
              << "Low:"    << idxLow    << " "
              << "Volume:" << idxVol    << "\n";

    // Read rows (keep all days; only true duplicates Ticker+Date are removed later)
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        auto f = splitCSVLine(line);
        if ((int)f.size() < (int)headers.size()) f.resize(headers.size());

        auto get = [&](int idx)->std::string {
            return (idx >= 0 && idx < (int)f.size()) ? f[idx] : "";
        };

        Stock s;
        s.Date   = trim(get(idxDate));
        s.Ticker = trim(get(idxTicker));
        if (s.Ticker.empty()) continue;     // need ticker
        // (We allow empty Date; but for time-series you should have it)
        if (idxDate < 0 || s.Date.empty()) {
            // If no date column found, you can uncomment to skip:
            // continue;
        }

        s.Price  = (idxPrice >= 0) ? safe_stod(get(idxPrice)) : 0.0;
        s.Close  = (idxClose >= 0) ? safe_stod(get(idxClose)) : s.Price;
        s.Open   = (idxOpen  >= 0) ? safe_stod(get(idxOpen))  : 0.0;
        s.High   = (idxHigh  >= 0) ? safe_stod(get(idxHigh))  : 0.0;
        s.Low    = (idxLow   >= 0) ? safe_stod(get(idxLow))   : 0.0;
        s.Volume = (idxVol   >= 0) ? safe_stod(get(idxVol))   : 0.0;

        stocks.push_back(std::move(s));
    }

    // Warn if Price looks like years (to catch bad mapping)
    int yearish = 0;
    for (size_t i=0;i<std::min<size_t>(stocks.size(),2000);++i) {
        double p = stocks[i].Price;
        if (p >= 1900 && p <= 2100 && std::floor(p) == p) ++yearish;
    }
    if (yearish > 100) {
        std::cerr << "⚠️  WARNING: Price values look like years. Check mapping above.\n";
    }

    return stocks;
}

std::vector<Sentiment> readSentimentsCSV(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) throw std::runtime_error("Failed to open sentiments CSV: " + filename);

    std::vector<Sentiment> sentiments;
    std::string line;
    if (!std::getline(file, line)) return sentiments;

    while (std::getline(file, line)) {
        if (line.empty()) continue;
        auto fields = splitCSVLine(line);
        if (fields.size() < 3) fields.resize(3);

        Sentiment s;
        s.Ticker = trim(fields[0]);
        s.Comment = fields[1].empty() ? "Unknown" : fields[1];
        s.SentimentScore = fields[2].empty() ? "Unknown" : trim(fields[2]);

        if (!s.Ticker.empty()) sentiments.push_back(std::move(s));
    }
    return sentiments;
}

/* ----------------- dedup + join ----------------- */

// Dedup stocks by (Ticker, Date) to keep exactly one row per day per ticker.
std::vector<Stock> dedup_stocks_by_ticker_date(const std::vector<Stock>& items) {
    std::unordered_set<std::string> seen;
    std::vector<Stock> out;
    out.reserve(items.size());
    for (const auto& s : items) {
        std::string key = s.Ticker + "|" + s.Date;   // <-- time-series dedup key
        if (seen.insert(key).second) out.push_back(s);
        else {
            // duplicate: replace previous with latest read (optional):
            // We won't replace to avoid searching; first occurrence wins.
        }
    }
    return out;
}

template <typename T, typename KeyFn>
std::vector<T> dedup_generic(const std::vector<T>& items, KeyFn key_fn) {
    std::unordered_set<std::string> seen;
    std::vector<T> unique;
    unique.reserve(items.size());
    for (const auto& it : items) {
        auto k = key_fn(it);
        if (seen.insert(k).second) unique.push_back(it);
    }
    return unique;
}

std::vector<JoinedRecord> joinDatasets(const std::vector<Stock>& stocks,
                                       const std::vector<Sentiment>& sentiments,
                                       int threads) {
    // Build ticker->sentiments map
    std::unordered_map<std::string,std::vector<Sentiment>> smap;
    smap.reserve(sentiments.size()*2+1);
    for (const auto& s: sentiments) smap[s.Ticker].push_back(s);

    std::vector<JoinedRecord> joined(stocks.size());
    #pragma omp parallel for num_threads(threads) schedule(static)
    for (size_t i=0;i<stocks.size();++i) {
        joined[i].stock = stocks[i];
        auto it = smap.find(stocks[i].Ticker);
        if (it != smap.end()) joined[i].sentiments = it->second;
    }
    return joined;
}

/* ----------------- exporters ----------------- */

void exportStocks(const std::vector<Stock>& stocks) {
    std::ofstream out("deduped_stocks.csv", std::ios::binary);
    out << "Date,Ticker,Price,Close,Open,High,Low,Volume\n";
    for (const auto& s: stocks) {
        out << s.Date << "," << s.Ticker << ","
            << s.Price << "," << s.Close << ","
            << s.Open  << "," << s.High  << ","
            << s.Low   << "," << s.Volume << "\n";
    }
    std::cerr << "💾 Wrote deduped_stocks.csv\n";
}

void exportSentiments(const std::vector<Sentiment>& sentiments) {
    std::ofstream out("deduped_sentiments.csv", std::ios::binary);
    out << "Ticker,Comment,SentimentScore\n";
    for (const auto& s: sentiments) {
        // Quote comment to preserve commas
        out << s.Ticker << ",\"" << s.Comment << "\"," << s.SentimentScore << "\n";
    }
    std::cerr << "💾 Wrote deduped_sentiments.csv\n";
}

void exportJoined(const std::vector<JoinedRecord>& joined) {
    std::ofstream out("preprocessed_output.csv", std::ios::binary);
    out << "Date,Ticker,Price,Close,Open,High,Low,Volume,Sentiments\n";
    for (const auto& row : joined) {
        out << row.stock.Date   << ","
            << row.stock.Ticker << ","
            << row.stock.Price  << ","
            << row.stock.Close  << ","
            << row.stock.Open   << ","
            << row.stock.High   << ","
            << row.stock.Low    << ","
            << row.stock.Volume << ",\"";

        for (size_t j=0;j<row.sentiments.size();++j) {
            out << row.sentiments[j].Comment;
            if (j+1<row.sentiments.size()) out << " | ";
        }
        out << "\"\n";
    }
    std::cerr << "💾 Wrote preprocessed_output.csv\n";
}

/* ----------------- main ----------------- */

int main(int argc, char** argv) {
    try {
        std::string stocks_path     = (argc>=2)? argv[1] : "india_stocks.csv";
        std::string sentiments_path = (argc>=3)? argv[2] : "ticker_sentiments.csv";
        int threads = (argc>=4)? std::max(1, std::atoi(argv[3])) : omp_get_max_threads();
        omp_set_num_threads(threads);

        std::cout << "Threads: " << threads << "\n";
        std::cout << "Reading datasets...\n";

        auto stocks     = readStocksCSV(stocks_path);
        auto sentiments = readSentimentsCSV(sentiments_path);

        std::cout << "Read stocks: " << stocks.size() << " rows\n";
        std::cout << "Read sentiments: " << sentiments.size() << " rows\n";

        std::cout << "Dedup by (Ticker,Date)...\n";
        stocks     = dedup_stocks_by_ticker_date(stocks); // <-- keeps daily history
        sentiments = dedup_generic(sentiments, [](const Sentiment& s){ return s.Ticker + "|" + s.Comment; });

        std::cout << "After dedup: stocks=" << stocks.size()
                  << ", sentiments=" << sentiments.size() << "\n";

        std::cout << "Joining datasets (by Ticker)...\n";
        auto joined = joinDatasets(stocks, sentiments, threads);
        std::cout << "Join produced " << joined.size() << " rows\n";

        std::cout << "Sample joined data:\n";
        for (size_t i=0;i<std::min<size_t>(joined.size(),5);++i) {
            std::cout << "Date: " << joined[i].stock.Date
                      << ", Ticker: " << joined[i].stock.Ticker
                      << ", Price: " << joined[i].stock.Price
                      << ", Sentiments: ";
            const auto& sv = joined[i].sentiments;
            for (size_t j=0; j<std::min<size_t>(sv.size(), 3); ++j) {
                std::cout << sv[j].Comment;
                if (j+1<std::min<size_t>(sv.size(),3)) std::cout << " | ";
            }
            if (sv.size() > 3) std::cout << " (+ " << (sv.size()-3) << " more)";
            std::cout << "\n";
        }

        // Exports — WARNING: preprocessed_output.csv can be multi-GB (expected)
        exportStocks(stocks);
        exportSentiments(sentiments);
        exportJoined(joined);

        std::cout << "✅ Preprocessing complete!\n";
        std::cout << "Files: deduped_stocks.csv, deduped_sentiments.csv, preprocessed_output.csv\n";
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
