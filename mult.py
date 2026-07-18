import os
import sys
import time
import json
import logging
import argparse
import requests
import tronapi
from typing import List, Dict, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dataclasses import dataclass, field
from eth_utils import to_checksum_address, is_address
import base58
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account

# ====================== UNIVERSAL CONFIG ======================
CONFIG_FILE = "config.env"
load_dotenv(CONFIG_FILE)

# Load from config.env
API_KEYS = {
    "tron": os.getenv("TRON_API_KEY", ""),
    "eth": os.getenv("ETH_API_KEY", ""),
    "bsc": os.getenv("BSC_API_KEY", ""),
    "polygon": os.getenv("POLYGON_API_KEY", ""),
    "avalanche": os.getenv("AVALANCHE_API_KEY", ""),
    "fantom": os.getenv("FANTOM_API_KEY", ""),
    "arbitrum": os.getenv("ARBITRUM_API_KEY", ""),
    "optimism": os.getenv("OPTIMISM_API_KEY", ""),
}

SEED_PHRASES = json.loads(os.getenv("SEED_PHRASES", "{}"))  # {"wallet_name": "seed phrase"}
PRIVATE_KEYS = json.loads(os.getenv("PRIVATE_KEYS", "{}"))   # {"wallet_name": "private_key"}
WALLET_PERMISSIONS = json.loads(os.getenv("WALLET_PERMISSIONS", "{}"))
LINKED_APPS = json.loads(os.getenv("LINKED_APPS", "{}"))

RATE_LIMIT = int(os.getenv("RATE_LIMIT", 8))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", 3))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", 1.5))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 5))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))

# ====================== NETWORKS ======================
NETWORKS = {
    "tron": {"name": "TRON", "native_symbol": "TRX", "decimals": 6, "node_url": "https://api.trongrid.io",
             "api_key_header": "TRON-PRO-API-KEY", "scan_api": "https://apilist.tronscanapi.com", "type": "tron"},
    "eth": {"name": "Ethereum", "native_symbol": "ETH", "decimals": 18,
            "node_url": "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
            "api_key_header": "Authorization", "scan_api": "https://api.etherscan.io/api", "type": "evm"},
    "bsc": {"name": "BSC", "native_symbol": "BNB", "decimals": 18, "node_url": "https://bsc-dataseed.binance.org",
            "api_key_header": None, "scan_api": "https://api.bscscan.com/api", "type": "evm"},
    "polygon": {"name": "Polygon", "native_symbol": "MATIC", "decimals": 18, "node_url": "https://polygon-rpc.com",
                "api_key_header": None, "scan_api": "https://api.polygonscan.com/api", "type": "evm"},
    "avalanche": {"name": "Avalanche", "native_symbol": "AVAX", "decimals": 18,
                  "node_url": "https://api.avax.network/ext/bc/C/rpc",
                  "api_key_header": None, "scan_api": "https://api.snowtrace.io/api", "type": "evm"},
    "fantom": {"name": "Fantom", "native_symbol": "FTM", "decimals": 18, "node_url": "https://rpc.ftm.tools",
               "api_key_header": None, "scan_api": "https://api.ftmscan.com/api", "type": "evm"},
    "arbitrum": {"name": "Arbitrum", "native_symbol": "ETH", "decimals": 18, "node_url": "https://arb1.arbitrum.io/rpc",
                 "api_key_header": None, "scan_api": "https://api.arbiscan.io/api", "type": "evm"},
    "optimism": {"name": "Optimism", "native_symbol": "ETH", "decimals": 18,
                 "node_url": "https://mainnet.optimism.io",
                 "api_key_header": None, "scan_api": "https://api-optimistic.etherscan.io/api", "type": "evm"},
}

# ====================== LOGGING ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WalletManager")


# ====================== CONFIG DATACLASS ======================
@dataclass
class Config:
    api_key: str
    network: str = "tron"
    token_contract: Optional[str] = None
    node_url: Optional[str] = None
    output_dir: str = "."
    found_file: str = field(init=False)
    invalid_file: str = field(init=False)

    def __post_init__(self):
        self.found_file = os.path.join(self.output_dir, "found.txt")
        self.invalid_file = os.path.join(self.output_dir, "invalid.txt")
        if self.network not in NETWORKS:
            raise ValueError(f"Unsupported network: {self.network}")


# ====================== RATE LIMITER ======================
class RateLimiter:
    def __init__(self, max_calls: int, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = Lock()

    def __enter__(self):
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.calls.append(time.time())
        return self

    def __exit__(self, *args):
        pass


# ====================== MULTI-CHAIN CLIENT ======================
class MultiChainClient:
    def __init__(self, config: Config):
        self.config = config
        self.network = NETWORKS[config.network]
        self.rate_limiter = RateLimiter(RATE_LIMIT)
        self.session = requests.Session()

        api_key = config.api_key or API_KEYS.get(config.network, "")
        if self.network["api_key_header"] and api_key:
            if self.network["type"] == "tron":
                self.session.headers.update({self.network["api_key_header"]: api_key})
            else:
                self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    # ====================== TRON ======================
    def _get_tron_balance(self, address: str) -> float:
        node_url = self.config.node_url or self.network["node_url"]
        tron = tronapi.Tron(full_node=tronapi.HttpProvider(node_url),
                            solidity_node=tronapi.HttpProvider(node_url),
                            event_server=tronapi.HttpProvider(node_url))
        return tron.trx.get_balance(address) / 1_000_000

    def _get_tron_token_balance(self, address: str, contract: str) -> Tuple[float, str]:
        url = f"{self.network['scan_api']}/api/account/wallet?address={address}&asset_type=0"
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for token in data.get("data", []):
            if token.get("token_id") == contract:
                return float(token.get("balance", 0)), token.get("token_abbr", "")
        return 0.0, ""

    def _get_tron_address_from_pk(self, private_key: str) -> str:
        node_url = self.config.node_url or self.network["node_url"]
        tron = tronapi.Tron(full_node=tronapi.HttpProvider(node_url),
                            solidity_node=tronapi.HttpProvider(node_url),
                            event_server=tronapi.HttpProvider(node_url))
        return tron.address.from_private_key(private_key).base58

    def _transfer_trc20(self, private_key: str, to_address: str, amount: float, contract: str, decimals: int) -> str:
        node_url = self.config.node_url or self.network["node_url"]
        tron = tronapi.Tron(full_node=tronapi.HttpProvider(node_url),
                            solidity_node=tronapi.HttpProvider(node_url),
                            event_server=tronapi.HttpProvider(node_url))
        tron.private_key = private_key
        tron.default_address = tron.address.from_private_key(private_key).base58
        contract_obj = tron.get_contract(contract)
        amount_in_units = int(amount * (10 ** decimals))
        tx = contract_obj.functions.transfer(to_address, amount_in_units)\
            .with_owner(tron.default_address).fee_limit(100_000_000).build()\
            .sign(tron.private_key).broadcast()
        return tx.get("txid", "")

    def _approve_trc20(self, private_key: str, spender: str, amount: float, contract: str, decimals: int) -> str:
        node_url = self.config.node_url or self.network["node_url"]
        tron = tronapi.Tron(full_node=tronapi.HttpProvider(node_url),
                            solidity_node=tronapi.HttpProvider(node_url),
                            event_server=tronapi.HttpProvider(node_url))
        tron.private_key = private_key
        tron.default_address = tron.address.from_private_key(private_key).base58
        contract_obj = tron.get_contract(contract)
        amount_in_units = int(amount * (10 ** decimals))
        tx = contract_obj.functions.approve(spender, amount_in_units)\
            .with_owner(tron.default_address).fee_limit(100_000_000).build()\
            .sign(tron.private_key).broadcast()
        return tx.get("txid", "")

    # ====================== EVM ======================
    def _get_evm_balance(self, address: str) -> float:
        node_url = self.config.node_url or self.network["node_url"]
        w3 = Web3(Web3.HTTPProvider(node_url))
        balance_wei = w3.eth.get_balance(to_checksum_address(address))
        return float(w3.from_wei(balance_wei, 'ether'))

    def _get_evm_token_balance(self, address: str, contract: str) -> Tuple[float, str]:
        node_url = self.config.node_url or self.network["node_url"]
        w3 = Web3(Web3.HTTPProvider(node_url))
        erc20_abi = [
            {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        contract_address = to_checksum_address(contract)
        token_contract = w3.eth.contract(address=contract_address, abi=erc20_abi)
        balance = token_contract.functions.balanceOf(to_checksum_address(address)).call()
        symbol = token_contract.functions.symbol().call()
        decimals = token_contract.functions.decimals().call()
        return balance / (10 ** decimals), symbol

    def _get_evm_address_from_pk(self, private_key: str) -> str:
        return Account.from_key(private_key).address

    def _transfer_erc20(self, private_key: str, to_address: str, amount: float, contract: str, decimals: int) -> str:
        node_url = self.config.node_url or self.network["node_url"]
        w3 = Web3(Web3.HTTPProvider(node_url))
        account = Account.from_key(private_key)
        contract_address = to_checksum_address(contract)
        to_address = to_checksum_address(to_address)
        erc20_abi = [
            {"constant": False, "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        token_contract = w3.eth.contract(address=contract_address, abi=erc20_abi)
        amount_in_units = int(amount * (10 ** decimals))
        nonce = w3.eth.get_transaction_count(account.address)
        gas_price = w3.eth.gas_price
        tx = token_contract.functions.transfer(to_address, amount_in_units).build_transaction({
            'from': account.address, 'nonce': nonce, 'gas': 100000, 'gasPrice': gas_price
        })
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()

    def _approve_erc20(self, private_key: str, spender: str, amount: float, contract: str, decimals: int) -> str:
        node_url = self.config.node_url or self.network["node_url"]
        w3 = Web3(Web3.HTTPProvider(node_url))
        account = Account.from_key(private_key)
        contract_address = to_checksum_address(contract)
        spender = to_checksum_address(spender)
        erc20_abi = [
            {"constant": False, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        token_contract = w3.eth.contract(address=contract_address, abi=erc20_abi)
        amount_in_units = int(amount * (10 ** decimals))
        nonce = w3.eth.get_transaction_count(account.address)
        gas_price = w3.eth.gas_price
        tx = token_contract.functions.approve(spender, amount_in_units).build_transaction({
            'from': account.address, 'nonce': nonce, 'gas': 100000, 'gasPrice': gas_price
        })
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex()

    # ====================== PUBLIC ======================
    def get_native_balance(self, address: str) -> float:
        if self.network["type"] == "tron":
            return self._retry_request(self._get_tron_balance, address)
        return self._retry_request(self._get_evm_balance, address)

    def get_token_balance(self, address: str, contract: str) -> Tuple[float, str]:
        if self.network["type"] == "tron":
            return self._retry_request(self._get_tron_token_balance, address, contract)
        return self._retry_request(self._get_evm_token_balance, address, contract)

    def get_address_from_private_key(self, private_key: str) -> str:
        if self.network["type"] == "tron":
            return self._retry_request(self._get_tron_address_from_pk, private_key)
        return self._retry_request(self._get_evm_address_from_pk, private_key)

    def transfer_token(self, private_key: str, to_address: str, amount: float, contract: str, decimals: int) -> str:
        if self.network["type"] == "tron":
            return self._retry_request(self._transfer_trc20, private_key, to_address, amount, contract, decimals)
        return self._retry_request(self._transfer_erc20, private_key, to_address, amount, contract, decimals)

    def approve_token(self, private_key: str, spender: str, amount: float, contract: str, decimals: int) -> str:
        if self.network["type"] == "tron":
            return self._retry_request(self._approve_trc20, private_key, spender, amount, contract, decimals)
        return self._retry_request(self._approve_erc20, private_key, spender, amount, contract, decimals)

    def _retry_request(self, func, *args, **kwargs):
        for attempt in range(RETRY_ATTEMPTS):
            try:
                with self.rate_limiter:
                    return func(*args, **kwargs)
            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    raise
                time.sleep(RETRY_BACKOFF ** attempt)
                logger.warning(f"Retry {attempt + 1}/{RETRY_ATTEMPTS}: {e}")


# ====================== WALLET MANAGER ======================
class WalletManager:
    def __init__(self):
        self.wallets = {**PRIVATE_KEYS, **SEED_PHRASES}
        self.permissions = WALLET_PERMISSIONS
        self.linked_apps = LINKED_APPS

    def list_wallets(self):
        logger.info("=== WALLET LIST ===")
        for name, key in self.wallets.items():
            logger.info(f"{name}: {key[:8]}...{key[-6:]}")

    def add_wallet(self, name: str, key: str, is_seed: bool = False):
        if is_seed:
            SEED_PHRASES[name] = key
        else:
            PRIVATE_KEYS[name] = key
        self.wallets[name] = key
        logger.info(f"Wallet '{name}' added.")

    def remove_wallet(self, name: str):
        self.wallets.pop(name, None)
        PRIVATE_KEYS.pop(name, None)
        SEED_PHRASES.pop(name, None)
        logger.info(f"Wallet '{name}' removed.")

    def set_permission(self, wallet: str, app: str, permission: str):
        if wallet not in self.permissions:
            self.permissions[wallet] = {}
        self.permissions[wallet][app] = permission
        logger.info(f"Permission set: {wallet} -> {app} = {permission}")

    def link_app(self, wallet: str, app_name: str, app_data: dict):
        if wallet not in self.linked_apps:
            self.linked_apps[wallet] = {}
        self.linked_apps[wallet][app_name] = app_data
        logger.info(f"App '{app_name}' linked to {wallet}")

    def edit_wallet(self, name: str, new_key: str):
        if name in self.wallets:
            self.wallets[name] = new_key
            if name in PRIVATE_KEYS:
                PRIVATE_KEYS[name] = new_key
            elif name in SEED_PHRASES:
                SEED_PHRASES[name] = new_key
            logger.info(f"Wallet '{name}' updated.")

    def save_config(self):
        with open(CONFIG_FILE, "w") as f:
            f.write(f"TRON_API_KEY={API_KEYS.get('tron','')}\n")
            f.write(f"ETH_API_KEY={API_KEYS.get('eth','')}\n")
            f.write(f"SEED_PHRASES={json.dumps(SEED_PHRASES)}\n")
            f.write(f"PRIVATE_KEYS={json.dumps(PRIVATE_KEYS)}\n")
            f.write(f"WALLET_PERMISSIONS={json.dumps(self.permissions)}\n")
            f.write(f"LINKED_APPS={json.dumps(self.linked_apps)}\n")
        logger.info("Config saved to config.env")


# ====================== ADDRESS SCANNER ======================
class AddressScanner:
    def __init__(self, config: Config):
        self.config = config
        self.client = MultiChainClient(config)
        self.found_lock = Lock()
        self.invalid_lock = Lock()
        self.network = NETWORKS[config.network]

    def _save_found(self, address: str, native: float, token: float = 0.0, token_symbol: str = ""):
        with self.found_lock:
            with open(self.config.found_file, "a") as f:
                line = f"{address} : {self.network['native_symbol']} {native:.6f}"
                if token > 0:
                    line += f", {token_symbol} {token:.6f}"
                f.write(line + "\n")

    def _save_invalid(self, key: str):
        with self.invalid_lock:
            with open(self.config.invalid_file, "a") as f:
                f.write(key + "\n")

    def check_address(self, address: str) -> Optional[Dict]:
        try:
            native = self.client.get_native_balance(address)
            token = 0.0
            symbol = ""
            if self.config.token_contract:
                token, symbol = self.client.get_token_balance(address, self.config.token_contract)
            if native > 0 or token > 0:
                self._save_found(address, native, token, symbol)
                return {"address": address, "native": native, "token": token, "token_symbol": symbol, "network": self.config.network}
        except Exception as e:
            logger.error(f"Error checking {address}: {e}")
        return None

    def process_file(self, filename: str) -> List[str]:
        addresses = []
        with open(filename, "r") as f:
            lines = [line.strip() for line in f if line.strip()]
        for line in lines:
            if len(line) == 34 and line.startswith("T"):
                addresses.append(line)
            elif is_address(line):
                addresses.append(to_checksum_address(line))
            elif len(line) == 64 and all(c.lower() in "0123456789abcdef" for c in line):
                try:
                    addr = self.client.get_address_from_private_key(line)
                    addresses.append(addr)
                except Exception:
                    self._save_invalid(line)
            else:
                self._save_invalid(line)
        return addresses

    def scan(self, addresses: List[str]) -> List[Dict]:
        results = []
        total = len(addresses)
        processed = 0
        start = time.time()
        chunks = [addresses[i:i + CHUNK_SIZE] for i in range(0, len(addresses), CHUNK_SIZE)]
        for chunk in chunks:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.check_address, addr): addr for addr in chunk}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        logger.error(f"Future error: {e}")
            processed += len(chunk)
            elapsed = time.time() - start
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            logger.info(f"Progress: {processed}/{total} ({processed/total*100:.1f}%) | ETA: {eta:.0f}s")
        return results


# ====================== MAIN ======================
def main():
    parser = argparse.ArgumentParser(description="Advanced Multi-Chain Wallet Manager")
    parser.add_argument("--network", default="tron", choices=list(NETWORKS.keys()))
    parser.add_argument("--input", help="Input file with addresses/private keys")
    parser.add_argument("--token", help="Token contract")
    parser.add_argument("--api-key", help="API key")
    parser.add_argument("--node-url", help="Custom node URL")
    parser.add_argument("--transfer", action="store_true")
    parser.add_argument("--approve", action="store_true")
    parser.add_argument("--to", help="Destination address")
    parser.add_argument("--amount", type=float)
    parser.add_argument("--private-key", help="Private key")
    parser.add_argument("--decimals", type=int)
    parser.add_argument("--wallet-manager", action="store_true", help="Launch wallet manager mode")
    args = parser.parse_args()

    config = Config(
        api_key=args.api_key or API_KEYS.get(args.network, ""),
        network=args.network,
        token_contract=args.token,
        node_url=args.node_url
    )

    wm = WalletManager()

    # Wallet Manager Mode
    if args.wallet_manager:
        wm.list_wallets()
        return

    # Transfer + Approve Combined (Auto-approve before send)
    if args.transfer or args.approve:
        if not all([args.private_key, args.to, args.amount is not None]):
            logger.error("Missing required arguments for transfer/approve")
            sys.exit(1)

        client = MultiChainClient(config)
        decimals = args.decimals or (18 if args.network != "tron" else 6)

        if args.token:
            # Auto approve first
            logger.info("Auto-approving token...")
            approve_tx = client.approve_token(args.private_key, args.to, args.amount, args.token, decimals)
            logger.info(f"Approve TX: {approve_tx}")
            time.sleep(3)

        if args.transfer:
            txid = client.transfer_token(args.private_key, args.to, args.amount, args.token or "", decimals)
            logger.info(f"Transfer TXID: {txid}")
        return

    # Scanning Mode
    scanner = AddressScanner(config)
    addresses = scanner.process_file(args.input)
    logger.info(f"Loaded {len(addresses)} addresses")
    results = scanner.scan(addresses)
    logger.info(f"Scan complete. Found {len(results)} addresses with balance.")


if __name__ == "__main__":
    main()
```

**Usage Examples:**

```bash
# Scan
python wallet_manager.py --network tron --input addresses.txt

# Transfer with auto-approve
python wallet_manager.py --network tron --transfer --private-key YOURKEY --to T... --amount 100 --token CONTRACT

# Wallet Manager
python wallet_manager.py --wallet-manager
```

**config.env example:**
```env
TRON_API_KEY=your_tron_key
ETH_API_KEY=your_eth_key
SEED_PHRASES={"main":"word1 word2 ..."}
PRIVATE_KEYS={"main":"0xabc123..."}
WALLET_PERMISSIONS={"main":{"app1":"full"}}
LINKED_APPS={"main":{"app1":{"url":"https://..."}}}
RATE_LIMIT=8
