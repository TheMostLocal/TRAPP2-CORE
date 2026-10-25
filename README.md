# TRAPP2-CORE — the shared pipeline (z158)

One copy of every script the four book repos (TRAPP2, TRAPP2-1, TRAPP2-2, TRAPP2-3) used to carry byte-identical.
Each book's workflows check this repo out at `_core/` and copy `pipeline/` over their own before running, so a fix
ships **once**. If this repo can't be checked out, the books fall back to their own copies — nothing stops.

Also here: **Alpaca market data** (`pipeline/alpaca_data.py`) — official US stock / ETF data, hundreds of symbols per
request. With the repo secrets `ALPACA_KEY_ID` / `ALPACA_SECRET_KEY` set in a book, its quotes and 5-minute bars come
from Alpaca (consolidated SIP to 16 minutes ago + IEX since) and Yahoo covers only what Alpaca doesn't (indices, FX,
futures, crypto, foreign listings, anything missing).

Book-specific scripts (`fetch_data.py`, `fetch_macro.py`, `fetch_financials.py`, `regime_certainty.py`,
`fetch_options.py`) stay in their books.
