import torch
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    AutoModelForCausalLM,
    pipeline
)
import pandas as pd
from tqdm import tqdm

# -------------------------------
# Device Setup
# -------------------------------
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# -------------------------------
# Load Tickers
# -------------------------------
with open("/content/20MICRONS.txt", "r") as f:
    tickers = [line.strip() for line in f.readlines()]
print(f"Loaded {len(tickers)} tickers.")

# -------------------------------
# Sentiment Model (financial domain)
# -------------------------------
print("Loading sentiment model...")
SENT_MODEL = "yiyanghkust/finbert-tone"   # robust, open model
sent_tokenizer = AutoTokenizer.from_pretrained(SENT_MODEL)
sent_model = AutoModelForSequenceClassification.from_pretrained(SENT_MODEL).to(device)
sentiment_pipeline = pipeline("sentiment-analysis", model=sent_model, tokenizer=sent_tokenizer, device=0 if device.type == "cuda" else -1)

# -------------------------------
# Text Generator Model (faster)
# -------------------------------
print("Loading generator model...")
GEN_MODEL = "distilgpt2"   # lightweight, fast
gen_tokenizer = AutoTokenizer.from_pretrained(GEN_MODEL)
gen_model = AutoModelForCausalLM.from_pretrained(GEN_MODEL).to(device)

def generate_comment(ticker, max_length=40):
    """Generate a short pseudo-comment for the ticker."""
    input_ids = gen_tokenizer.encode(f"{ticker} stock outlook:", return_tensors="pt").to(device)
    output = gen_model.generate(
        input_ids,
        max_length=max_length,
        num_return_sequences=1,
        do_sample=True,
        top_k=50,
        top_p=0.95
    )
    text = gen_tokenizer.decode(output[0], skip_special_tokens=True)
    return text

# -------------------------------
# Run Sentiment + Generation
# -------------------------------
results = []

for ticker in tqdm(tickers, desc="Tickers"):
    try:
        # Generate mock comment
        comment = generate_comment(ticker)

        # Sentiment
        sentiment = sentiment_pipeline(comment)[0]

        results.append({
            "Ticker": ticker,
            "Comment": comment,
            "Sentiment": sentiment["label"],
            "Score": sentiment["score"]
        })
    except Exception as e:
        print(f"⚠ Error with {ticker}: {e}")

# -------------------------------
# Save Results
# -------------------------------
df = pd.DataFrame(results)
df.to_csv("ticker_sentiments.csv", index=False)
print("✅ Analysis complete. Saved to ticker_sentiments.csv")
