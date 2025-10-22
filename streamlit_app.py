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
st.set_page_config(page_title="Bitwave Balance Validator (Multi-Chain)", layout="wide")
st.title("Bitwave Balance Validator — Multi-Chain Token Balance Validation")

# Multi-chain RPC configuration
CHAIN_CONFIG = {
    "AVAX": {
        "name": "Avalanche C-Chain",
        "rpc": os.getenv("AVALANCHE") or (st.secrets.get("rpc", {}).get("AVALANCHE") if hasattr(st, "secrets") else None),
        "native_token": "AVAX",
        "aliases": ["avax", "avalanche", "avax-c", "avalanche c-chain", "avalanche c chain"]
    },
    "ETH": {
        "name": "Ethereum Mainnet",
        "rpc": os.getenv("ETHEREUM") or (st.secrets.get("rpc", {}).get("ETHEREUM") if hasattr(st, "secrets") else None),
        "native_token": "ETH",
        "aliases": ["eth", "ethereum", "ethereum mainnet", "mainnet"]
    },
    "ARB": {
        "name": "Arbitrum One",
        "rpc": os.getenv("ARBITRUM") or (st.secrets.get("rpc", {}).get("ARBITRUM") if hasattr(st, "secrets") else None),
        "native_token": "ETH",
        "aliases": ["arb", "arbitrum", "arbitrum one", "arb1"]
    },
    "BASE": {
        "name": "Base",
        "rpc": os.getenv("BASE") or (st.secrets.get("rpc", {}).get("BASE") if hasattr(st, "secrets") else None),
        "native_token": "ETH",
        "aliases": ["base", "base mainnet"]
    }
}

# Check which chains are configured
configured_chains = {k: v for k, v in CHAIN_CONFIG.items() if v["rpc"]}
if not configured_chains:
    st.error("⚠️ No RPC URLs configured. Please set environment variables or Streamlit secrets for: AVALANCHE, ETHEREUM, ARBITRUM, BASE")
    st.stop()

st.success(f"✅ Configured chains: {', '.join([v['name'] for v in configured_chains.values()])}")

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
def make_w3(rpc):
    from web3.middleware import ExtraDataToPOAMiddleware
    try:
        # Add timeout to prevent hanging
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 30}))
        # Inject POA middleware to handle chains with extra data in blocks (Avalanche, etc.)
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return w3
    except Exception as e:
        st.error(f"Failed to connect to RPC: {rpc}. Error: {str(e)}")
        raise

def identify_chain(chain_value):
    """Identify which chain based on the blockchain column value"""
    if not chain_value or pd.isna(chain_value):
        return None
    
    chain_str = str(chain_value).strip().lower()
    
    for chain_key, config in CHAIN_CONFIG.items():
        if chain_str in config["aliases"]:
            return chain_key
    
    return None

def fetch_native(rpc, addr, blk): 
    return make_w3(rpc).eth.get_balance(Web3.to_checksum_address(addr), blk)

@st.cache_data(ttl=3600)
def find_block_by_timestamp(rpc, target_ts):
    """Binary search to find block closest to target timestamp"""
    w3 = make_w3(rpc)
    latest_block = w3.eth.block_number
    latest_ts = w3.eth.get_block(latest_block).timestamp
    
    # Check if target is in the future
    if target_ts >= latest_ts:
        st.warning(f"⚠️ Target timestamp is in the future. Using latest block {latest_block}.")
        return latest_block
    
    # Check if target is too far in the past
    genesis_block = w3.eth.get_block(0)
    if target_ts < genesis_block.timestamp:
        st.error(f"❌ Target timestamp is before genesis block. Using block 0.")
        return 0
    
    # Binary search
    low, high = 0, latest_block
    closest_block = latest_block
    closest_diff = abs(latest_ts - target_ts)
    
    iterations = 0
    max_iterations = 100  # Safety limit
    
    while low <= high and iterations < max_iterations:
        mid = (low + high) // 2
        block = w3.eth.get_block(mid)
        block_ts = block.timestamp
        diff = abs(block_ts - target_ts)
        
        if diff < closest_diff:
            closest_block = mid
            closest_diff = diff
        
        # If we're within 15 seconds, that's close enough
        if diff < 15:
            return mid
        
        if block_ts < target_ts:
            low = mid + 1
        elif block_ts > target_ts:
            high = mid - 1
        else:
            return mid
        
        iterations += 1
    
    # Verify the result
    result_block = w3.eth.get_block(closest_block)
    result_ts = result_block.timestamp
    time_diff = abs(result_ts - target_ts)
    
    st.success(f"✅ Found block {closest_block} (timestamp difference: {time_diff} seconds)")
    
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

# Manual blockchain override
use_manual_chain = st.sidebar.checkbox(
    "Override blockchain detection",
    help="Manually select a blockchain instead of using the blockchain column"
)

if use_manual_chain:
    manual_chain = st.sidebar.selectbox(
        "Select Blockchain",
        options=list(configured_chains.keys()),
        format_func=lambda x: CHAIN_CONFIG[x]["name"]
    )
else:
    manual_chain = None

st.sidebar.divider()

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

# Better column matching - exact match first, then partial
def find_column(keywords):
    # First try exact match (case insensitive)
    for col in cols:
        col_lower = col.lower().replace(' ', '').replace('_', '')
        for keyword in keywords:
            keyword_lower = keyword.lower().replace(' ', '').replace('_', '')
            if col_lower == keyword_lower:
                return col
    # Then try partial match
    for col in cols:
        col_lower = col.lower()
        for keyword in keywords:
            if keyword.lower() in col_lower:
                return col
    return None

# Auto-suggest columns with better matching (prioritize exact matches)
suggested_mapping = {
    "address": find_column(["walletaddress", "wallet address", "wallet", "address"]),
    "chain": find_column(["blockchain", "chain", "network"]),
    "token_symbol": find_column(["symbol", "ticker", "token"]),
    "token_contract": find_column(["tokenaddress", "token address", "contractaddress", "contract address", "contract"]),
    "reported_balance": find_column(["value", "balance", "amount"])
}

# Validate that chain column exists for multi-chain support
if not suggested_mapping["chain"]:
    st.warning("⚠️ No 'blockchain' or 'chain' column detected. Multi-chain validation requires a chain identifier column.")

# Show detected columns
st.write("**Auto-detected columns:**")
detection_cols = st.columns(5)
with detection_cols[0]: st.caption(f"Wallet: `{suggested_mapping['address'] or 'Not found'}`")
with detection_cols[1]: st.caption(f"Token Contract: `{suggested_mapping['token_contract'] or 'Not found'}`")
with detection_cols[2]: st.caption(f"Symbol: `{suggested_mapping['token_symbol'] or 'Not found'}`")
with detection_cols[3]: st.caption(f"Balance: `{suggested_mapping['reported_balance'] or 'Not found'}`")
with detection_cols[4]: st.caption(f"Chain: `{suggested_mapping['chain'] or 'Not found'}`")

# Let user confirm/adjust column mapping
st.subheader("📋 Confirm Column Mapping")
st.write("Verify the column mappings below. Adjust if needed.")
col_map_cols = st.columns(5)
with col_map_cols[0]:
    addr_col = st.selectbox(
        "Wallet Address *", 
        cols, 
        index=cols.index(suggested_mapping["address"]) if suggested_mapping["address"] in cols else 0,
        help="Column containing wallet addresses (required)"
    )
with col_map_cols[1]:
    token_col = st.selectbox(
        "Token Contract", 
        [None] + cols, 
        index=cols.index(suggested_mapping["token_contract"])+1 if suggested_mapping["token_contract"] in cols else 0,
        help="Column containing token contract addresses. Leave as 'None' if only validating native AVAX."
    )
with col_map_cols[2]:
    symbol_col = st.selectbox(
        "Token Symbol", 
        [None] + cols, 
        index=cols.index(suggested_mapping["token_symbol"])+1 if suggested_mapping["token_symbol"] in cols else 0,
        help="Column containing token symbols (e.g., USDC, USDT)"
    )
with col_map_cols[3]:
    balance_col = st.selectbox(
        "Reported Balance *", 
        cols, 
        index=cols.index(suggested_mapping["reported_balance"]) if suggested_mapping["reported_balance"] in cols else 0,
        help="Column containing the balance values to validate (required)"
    )
with col_map_cols[4]:
    chain_col = st.selectbox(
        "Chain", 
        [None] + cols, 
        index=cols.index(suggested_mapping["chain"])+1 if suggested_mapping["chain"] in cols else 0,
        help="Column containing blockchain/network info"
    )

mapping = {
    "address": addr_col,
    "chain": chain_col,
    "token_symbol": symbol_col,
    "token_contract": token_col,
    "reported_balance": balance_col
}

cmap = ColumnMap(**mapping)

# Group data by chain for processing
if use_manual_chain:
    st.info(f"🔧 **Manual Override:** All entries will be validated on **{CHAIN_CONFIG[manual_chain]['name']}**")
    df['_chain_id'] = manual_chain
elif cmap.chain:
    df['_chain_id'] = df[cmap.chain].apply(identify_chain)
    chains_in_data = df['_chain_id'].dropna().unique()
    st.write(f"**Chains detected in data:** {', '.join([CHAIN_CONFIG[c]['name'] for c in chains_in_data if c in CHAIN_CONFIG])}")
    
    # Check if all detected chains are configured
    missing_chains = [c for c in chains_in_data if c not in configured_chains]
    if missing_chains:
        st.error(f"❌ Missing RPC configuration for: {', '.join([CHAIN_CONFIG.get(c, {}).get('name', c) for c in missing_chains])}")
        st.stop()
else:
    st.warning("⚠️ No chain column mapped. Assuming all entries are for the first configured chain.")
    df['_chain_id'] = list(configured_chains.keys())[0]

res = []
with st.spinner("Fetching on-chain balances..."):
    progress_bar = st.progress(0)
    
    for i, r in df.iterrows():
        progress_bar.progress((i + 1) / len(df))
        addr = str(r[cmap.address]).strip()
        token_raw = str(r[cmap.token_contract]).strip() if cmap.token_contract else None
        rep = human_to_decimal(r[cmap.reported_balance])
        chain_id = r.get('_chain_id')
        
        # Validate chain
        if not chain_id or chain_id not in configured_chains:
            res.append({"row": i+1, "chain": chain_id or "Unknown", "wallet": addr,"token": token_raw or "N/A","reported": rep,"onchain": None,"delta": None,"error": "Chain not configured or not recognized"})
            continue
        
        chain_config = CHAIN_CONFIG[chain_id]
        rpc_url = chain_config["rpc"]
        native_token = chain_config["native_token"]
        
        # Get block number for this chain with error handling
        try:
            if validation_mode == "Current Balances":
                blk = make_w3(rpc_url).eth.block_number
            else:
                local_dt = dt.datetime.combine(asof_date, asof_time)
                utc_ts = int(pytz.timezone(tzname).localize(local_dt).astimezone(pytz.UTC).timestamp())
                if explicit_blk:
                    blk = explicit_blk
                else:
                    blk = find_block_by_timestamp(rpc_url, utc_ts)
        except Exception as e:
            res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": token_raw or native_token,"reported": rep,"onchain": None,"delta": None,"error": f"RPC Error: {str(e)[:100]}"})
            continue
        
        # Validate wallet address (basic check)
        if not addr or len(addr) < 10:
            res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": token_raw or native_token,"reported": rep,"onchain": None,"delta": None,"error": "Invalid or empty wallet address"})
            continue
            
        try:
            if _is_native(token_raw):
                try:
                    bal = fetch_native(rpc_url, addr, blk) / 1e18
                    sym = native_token
                except Exception as e:
                    res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": native_token,"reported": rep,"onchain": None,"delta": None,"error": f"Invalid address format: {str(e)}"})
                    continue
            else:
                raw = fetch_erc20(rpc_url, token_raw, addr, blk)
                if raw is None:
                    res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": token_raw,"reported": rep,"onchain": None,"delta": None,"error": "Failed to fetch balance"})
                    continue
                decimals = fetch_token_decimals(rpc_url, token_raw)
                bal = raw / (10 ** decimals)
                sym = r.get(cmap.token_symbol,"TOKEN") if cmap.token_symbol else "TOKEN"
            res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": sym,"reported": rep,"onchain": bal,"delta": (bal-rep if rep is not None else None)})
        except Exception as e:
            res.append({"row": i+1, "chain": chain_config["name"], "wallet": addr,"token": token_raw if token_raw else native_token,"reported": rep,"onchain": None,"delta": None,"error": f"Error: {str(e)}"})
    progress_bar.empty()

out = pd.DataFrame(res)
st.dataframe(out)
csv = out.to_csv(index=False).encode()
st.download_button("Download CSV", csv, "validation_report.csv", "text/csv")
