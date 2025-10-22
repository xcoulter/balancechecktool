# Bitwave Balance Validator - Multi-Chain

A Streamlit application for validating token balances across multiple blockchain networks by comparing reported balances against on-chain data.

## Supported Chains

- **Avalanche C-Chain** (AVAX)
- **Ethereum Mainnet** (ETH)
- **Arbitrum One** (ARB)
- **Base** (BASE)

## Features

- ✅ **Multi-chain support** - Automatically detects and validates balances across different chains
- ✅ **Current & Historical balances** - Validate current state or historical balances at specific dates/times
- ✅ **Smart column mapping** - Auto-detects CSV/Excel columns with manual override
- ✅ **Native & ERC20 tokens** - Supports both native tokens (AVAX, ETH) and ERC20 tokens
- ✅ **Automatic decimals detection** - Fetches token decimals from contracts
- ✅ **Progress tracking** - Visual progress bar and detailed error reporting
- ✅ **Export results** - Download validation report as CSV

## Setup

### Environment Variables

Set RPC URLs for the chains you want to use:

```bash
export AVALANCHE="https://api.avax.network/ext/bc/C/rpc"
export ETHEREUM="https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
export ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
export BASE="https://base-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
```

### Streamlit Secrets (for Streamlit Cloud)

Create `.streamlit/secrets.toml`:

```toml
[rpc]
AVALANCHE = "https://api.avax.network/ext/bc/C/rpc"
ETHEREUM = "https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
ARBITRUM = "https://arb-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
BASE = "https://base-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
```

## CSV/Excel Format

Your input file should contain the following columns:

| Column | Description | Required |
|--------|-------------|----------|
| `WalletAddress` | Wallet/account address | ✅ Yes |
| `Blockchain` | Chain identifier (e.g., "AVAX", "ETH", "Arbitrum", "Base") | ✅ Yes |
| `TokenAddress` | Token contract address (leave empty/N/A for native tokens) | No |
| `Symbol` | Token symbol (e.g., "USDC", "USDT") | No |
| `Value` or `Balance` | Reported balance amount | ✅ Yes |

### Blockchain Column Values

The app recognizes these chain identifiers (case-insensitive):

- **Avalanche**: `avax`, `avalanche`, `avax-c`, `avalanche c-chain`
- **Ethereum**: `eth`, `ethereum`, `ethereum mainnet`, `mainnet`
- **Arbitrum**: `arb`, `arbitrum`, `arbitrum one`, `arb1`
- **Base**: `base`, `base mainnet`

## Usage

1. **Select Validation Mode**
   - Current Balances: Latest blockchain state
   - Historical Balances: Specific date/time (automatically finds correct block)

2. **Upload CSV/Excel File**
   - Supports `.csv` and `.xlsx` formats

3. **Verify Column Mapping**
   - Auto-detected columns are shown
   - Adjust mappings using dropdowns if needed

4. **Review Results**
   - View validation results in the app
   - Download CSV report with deltas and errors

## Installation

```bash
pip install -r requirements.txt
```

## Running Locally

```bash
streamlit run streamlit_app.py
```

## Output Format

The validation report includes:

- `row`: Original row number from input file
- `chain`: Blockchain network name
- `wallet`: Wallet address
- `token`: Token symbol
- `reported`: Reported balance from input
- `onchain`: Actual on-chain balance
- `delta`: Difference (onchain - reported)
- `error`: Error message (if any)

## Notes

- Historical balance validation uses binary search to find the closest block to the specified timestamp
- Token decimals are automatically fetched from contracts
- Invalid addresses or tokens will be reported in the error column
- Progress bar shows validation progress
