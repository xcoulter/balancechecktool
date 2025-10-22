# Bitwave Balance Validator Streamlit App (Avalanche)
# (see conversation for detailed code; full working version included here)

import os, io, json, time, math, base64, pytz, datetime as dt, requests
import pandas as pd
import streamlit as st
from dataclasses import dataclass
from typing import Optional
from web3 import Web3
from web3.exceptions import ContractLogicError

# Setup
st.set_page_config(page_title="Bitwave Balance Validator (Avalanche)", layout="wide")
st.title("Bitwave Balance Validator — Token Balances vs On‑Chain (Avalanche)")

RPC_URL = os.getenv("AVALANCHE") or (st.secrets.get("rpc", {}).get("AVALANCHE") if hasattr(st, "secrets") else None)
if not RPC_URL:
    st.error("Missing RPC URL. Set via env or Streamlit secrets.")
    st.stop()

ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "account", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
]

@dataclass
class ColumnMap:
    address: str
    chain: Optional[str]
    token_symbol: Optional[str]
    token_contract: Optional[str]
    reported_balance: str

def _is_native(token_address):
    if token_address is None: return True
    s = str(token_address).strip().lower()
    # Check for empty, N/A, or non-hex strings
    if s in {"", "n/a", "na", "none", "nan"}: return True
    # Check if it's a valid hex address (starts with 0x and is 42 chars)
    if not s.startswith("0x"): return True
    if len(s) != 42: return True
    # Check if it contains only valid hex characters after 0x
    try:
        int(s, 16)  # Will raise ValueError if not valid hex
    except ValueError:
        return True
    return False

def human_to_decimal(val):
    try:
        if pd.isna(val): return None
        return float(str(val).replace(',', '').strip())
    except: return None

@st.cache_resource
def make_w3(rpc): return Web3(Web3.HTTPProvider(rpc))

def fetch_native(rpc, addr, blk): return make_w3(rpc).eth.get_balance(Web3.to_checksum_address(addr), blk)

def find_block_by_timestamp(rpc, target_ts):
    """Binary search to find block closest to target timestamp"""
    w3 = make_w3(rpc)
    latest_block = w3.eth.block_number
    latest_ts = w3.eth.get_block(latest_block).timestamp
    
    # Check if target is in the future
    if target_ts >= latest_ts:
        return latest_block
    
    # Binary search
    low, high = 0, latest_block
    closest_block = latest_block
    
    while low <= high:
        mid = (low + high) // 2
        block = w3.eth.get_block(mid)
        block_ts = block.timestamp
        
        if abs(block_ts - target_ts) < abs(w3.eth.get_block(closest_block).timestamp - target_ts):
            closest_block = mid
        
        if block_ts < target_ts:
            low = mid + 1
        elif block_ts > target_ts:
            high = mid - 1
        else:
            return mid
    
    return closest_block

@st.cache_data
def fetch_token_decimals(rpc, token):
    w3 = make_w3(rpc)
    c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    try: 
        decimals = c.functions.decimals().call()
        return int(decimals)
    except: 
        return 18  # default fallback

def fetch_erc20(rpc, token, addr, blk):
    w3 = make_w3(rpc)
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        balance = c.functions.balanceOf(Web3.to_checksum_address(addr)).call(block_identifier=blk)
        return int(balance)
    except Exception as e: 
        # Return None on error, let caller handle the error message
        return None

# --- Sidebar ---
st.sidebar.header("Balance Validation Settings")

# Choose between current or historical
validation_mode = st.sidebar.radio(
    "Validation Mode",
    ["Current Balances", "Historical Balances"],
    help="Current: Latest blockchain state. Historical: Balances at a specific date/time."
)

if validation_mode == "Historical Balances":
    st.sidebar.subheader("Historical Date/Time")
    col1, col2, col3 = st.sidebar.columns(3)
    with col1: asof_date = st.date_input("Date", dt.date.today())
    with col2: asof_time = st.time_input("Time", dt.time(23,59,59))
    with col3: tzname = st.selectbox("Timezone", pytz.all_timezones, index=pytz.all_timezones.index("UTC"))
    
    blk_input = st.sidebar.text_input("Block number (optional)", help="Leave empty to auto-detect block from timestamp")
    explicit_blk = int(blk_input) if blk_input.strip().isdigit() else None
else:
    asof_date = None
    asof_time = None
    tzname = None
    explicit_blk = None

# --- Upload ---
upload = st.file_uploader("Upload Bitwave CSV/XLSX", type=["csv","xlsx"])
if not upload:
    st.info("Upload file to start.")
    st.stop()

df = pd.read_csv(upload) if upload.name.endswith('.csv') else pd.read_excel(upload)
st.dataframe(df.head())

cols = list(df.columns)
suggest = lambda keys: next((c for c in cols if any(k in c.lower() for k in keys)), None)

# Auto-suggest columns
suggested_mapping = {
    "address": suggest(["walletaddress","wallet","address"]),
    "chain": suggest(["blockchain","chain","network"]),
    "token_symbol": suggest(["symbol","ticker"]),
    "token_contract": suggest(["tokenaddress","contract"]),
    "reported_balance": suggest(["value","balance","amount"])
}

# Let user confirm/adjust column mapping
st.subheader("Column Mapping")
col_map_cols = st.columns(5)
with col_map_cols[0]:
    addr_col = st.selectbox("Wallet Address", cols, index=cols.index(suggested_mapping["address"]) if suggested_mapping["address"] in cols else 0)
with col_map_cols[1]:
    token_col = st.selectbox("Token Contract (optional)", [None] + cols, index=cols.index(suggested_mapping["token_contract"])+1 if suggested_mapping["token_contract"] in cols else 0)
with col_map_cols[2]:
    symbol_col = st.selectbox("Token Symbol (optional)", [None] + cols, index=cols.index(suggested_mapping["token_symbol"])+1 if suggested_mapping["token_symbol"] in cols else 0)
with col_map_cols[3]:
    balance_col = st.selectbox("Reported Balance", cols, index=cols.index(suggested_mapping["reported_balance"]) if suggested_mapping["reported_balance"] in cols else 0)
with col_map_cols[4]:
    chain_col = st.selectbox("Chain (optional)", [None] + cols, index=cols.index(suggested_mapping["chain"])+1 if suggested_mapping["chain"] in cols else 0)

mapping = {
    "address": addr_col,
    "chain": chain_col,
    "token_symbol": symbol_col,
    "token_contract": token_col,
    "reported_balance": balance_col
}

cmap = ColumnMap(**mapping)

# Determine block number based on mode
if validation_mode == "Current Balances":
    blk = make_w3(RPC_URL).eth.block_number
    st.info(f"✅ Validating **current balances** at block **{blk}**")
else:
    # Historical mode
    local_dt = dt.datetime.combine(asof_date, asof_time)
    utc_ts = int(pytz.timezone(tzname).localize(local_dt).astimezone(pytz.UTC).timestamp())
    
    if explicit_blk:
        blk = explicit_blk
        st.info(f"✅ Validating **historical balances** at block **{blk}** (manually specified)")
    else:
        # Find block number by timestamp using binary search
        with st.spinner("Finding block number for specified timestamp..."):
            blk = find_block_by_timestamp(RPC_URL, utc_ts)
        st.info(f"✅ Validating **historical balances** at block **{blk}** (timestamp: {utc_ts})")

res = []
with st.spinner("Fetching on-chain balances..."):
    progress_bar = st.progress(0)
    for i, r in df.iterrows():
        progress_bar.progress((i + 1) / len(df))
        addr = str(r[cmap.address]).strip()
        token_raw = str(r[cmap.token_contract]).strip() if cmap.token_contract else None
        rep = human_to_decimal(r[cmap.reported_balance])
        
        # Validate wallet address (basic check)
        if not addr or len(addr) < 10:
            res.append({"row": i+1, "wallet": addr,"token": token_raw or "AVAX","reported": rep,"onchain": None,"delta": None,"error": "Invalid or empty wallet address"})
            continue
            
        try:
            if _is_native(token_raw):
                try:
                    bal = fetch_native(RPC_URL, addr, blk) / 1e18
                    sym = "AVAX"
                except Exception as e:
                    res.append({"row": i+1, "wallet": addr,"token": "AVAX","reported": rep,"onchain": None,"delta": None,"error": f"Invalid address format: {str(e)}"})
                    continue
            else:
                raw = fetch_erc20(RPC_URL, token_raw, addr, blk)
                if raw is None:
                    res.append({"row": i+1, "wallet": addr,"token": token_raw,"reported": rep,"onchain": None,"delta": None,"error": "Failed to fetch balance"})
                    continue
                decimals = fetch_token_decimals(RPC_URL, token_raw)
                bal = raw / (10 ** decimals)
                sym = r.get(cmap.token_symbol,"TOKEN") if cmap.token_symbol else "TOKEN"
            res.append({"row": i+1, "wallet": addr,"token": sym,"reported": rep,"onchain": bal,"delta": (bal-rep if rep is not None else None)})
        except Exception as e:
            res.append({"row": i+1, "wallet": addr,"token": token_raw if token_raw else "AVAX","reported": rep,"onchain": None,"delta": None,"error": f"Error: {str(e)}"})
    progress_bar.empty()

out = pd.DataFrame(res)
st.dataframe(out)
csv = out.to_csv(index=False).encode()
st.download_button("Download CSV", csv, "validation_report.csv", "text/csv")
