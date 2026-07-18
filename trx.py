```python
"""
Advanced TRON Token Scanner & Transfer Utility
- Professional-grade, production-ready
- Supports TRX + TRC20 token balance scanning
- Includes token transfer, approval, and optimized execution
- Rate-limited, concurrent, and resilient
"""

import os
import sys
import time
import json
import logging
import requests
import concurrent.futures
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from threading import Semaphore, Lock
from tronapi import Tron
from tronapi import HttpProvider
from eth_utils import to_checksum_address
from tronpy import Tron as TronPy
from tronpy.keys import PrivateKey
from tronpy.providers import HTTPProvider

# ====================== CONFIGURATION ======================
MAX_CALLS_PER_SECOND = 8
SLEEP_TIME = 1.0 / MAX_CALLS_PER_SECOND
CHUNK_SIZE = 5
MAX_WORKERS = 5
RETRY_ATTEMPTS = 3
BACKOFF_BASE = 1.5

# ====================== LOGGING SETUP ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("tron_scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ====================== DATA CLASSES ======================
@dataclass
class WalletResult:
    address: str
    trx_balance: float
    token_balance: float = 0.0
    token_symbol: str = ""
    has_balance: bool = False

@dataclass
class TransferResult:
    success: bool
    txid: Optional[str] = None
    error: Optional[str] = None

# ====================== API KEY MANAGEMENT ======================
def get_api_key() -> str:
    api_key_file = "api.txt"
    if os.path.isfile(api_key_file):
        with open(api_key_file, "r") as f:
            return f.read().strip()
    else:
        key = input("Enter your TronGrid API Key: ").strip()
        with open(api_key_file, "w") as f:
            f.write(key)
        return key

API_KEY = get_api_key()

# ====================== TRON CLIENTS ======================
class TronClient:
    def __init__(self):
        self.node_url = "https://api.trongrid.io"
        self.tron = Tron(
            full_node=HttpProvider(self.node_url),
            solidity_node=HttpProvider(self.node_url),
            event_server=HttpProvider(self.node_url)
        )
        self.tronpy = TronPy(HTTPProvider(self.node_url, api_key=API_KEY))
        self.semaphore = Semaphore(MAX_CALLS_PER_SECOND)
        self.lock = Lock()
        self.last_call = time.time()

    def _rate_limit(self):
        with self.semaphore:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_call
                if elapsed < SLEEP_TIME:
                    time.sleep(SLEEP_TIME - elapsed)
                self.last_call = time.time()

    def get_trx_balance(self, address: str) -> float:
        self._rate_limit()
        try:
            balance_sun = self.tron.trx.get_balance(address)
            return balance_sun / 1_000_000
        except Exception as e:
            logger.warning(f"TRX balance error for {address}: {e}")
            return 0.0

    def get_token_balance(self, address: str, contract: str) -> Tuple[float, str]:
        self._rate_limit()
        url = f"https://apilist.tronscanapi.com/api/account/wallet?address={address}&asset_type=0"
        headers = {"TRON-PRO-API-KEY": API_KEY}
        
        for attempt in range(RETRY_ATTEMPTS):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for token in data.get("data", []):
                        if token.get("token_id") == contract:
                            return float(token.get("balance", 0)), token.get("token_abbr", "")
                time.sleep(BACKOFF_BASE ** attempt)
            except Exception as e:
                logger.warning(f"Token balance attempt {attempt+1} failed: {e}")
        return 0.0, ""

    def get_address_from_private_key(self, private_key: str) -> str:
        try:
            pk = PrivateKey(bytes.fromhex(private_key))
            return pk.public_key.to_base58check_address()
        except Exception as e:
            logger.error(f"Invalid private key: {e}")
            return ""

    def approve_token(self, private_key: str, token_contract: str, spender: str, amount: int) -> TransferResult:
        """Approve spender to spend tokens"""
        try:
            pk = PrivateKey(bytes.fromhex(private_key))
            contract = self.tronpy.get_contract(token_contract)
            
            # Build approve transaction
            txn = (
                contract.functions.approve(spender, amount)
                .with_owner(pk.public_key.to_base58check_address())
                .fee_limit(100_000_000)
                .build()
                .sign(pk)
            )
            result = txn.broadcast()
            return TransferResult(success=True, txid=result.get("txid"))
        except Exception as e:
            return TransferResult(success=False, error=str(e))

    def transfer_token(self, private_key: str, token_contract: str, to_address: str, amount: int) -> TransferResult:
        """Transfer TRC20 tokens"""
        try:
            pk = PrivateKey(bytes.fromhex(private_key))
            contract = self.tronpy.get_contract(token_contract)
            
            txn = (
                contract.functions.transfer(to_address, amount)
                .with_owner(pk.public_key.to_base58check_address())
                .fee_limit(100_000_000)
                .build()
                .sign(pk)
            )
            result = txn.broadcast()
            return TransferResult(success=True, txid=result.get("txid"))
        except Exception as e:
            return TransferResult(success=False, error=str(e))

    def transfer_trx(self, private_key: str, to_address: str, amount_sun: int) -> TransferResult:
        """Transfer native TRX"""
        try:
            pk = PrivateKey(bytes.fromhex(private_key))
            txn = (
                self.tronpy.trx.transfer(pk.public_key.to_base58check_address(), to_address, amount_sun)
                .build()
                .sign(pk)
            )
            result = txn.broadcast()
            return TransferResult(success=True, txid=result.get("txid"))
        except Exception as e:
            return TransferResult(success=False, error=str(e))

# ====================== CORE SCANNER ======================
class TronScanner:
    def __init__(self, token_contract: Optional[str] = None):
        self.client = TronClient()
        self.token_contract = token_contract
        self.results: List[WalletResult] = []
        self.found_file = "found.txt"
        self.invalid_file = "invalid.txt"

    def load_wallets(self, filename: str = "wallets.txt") -> List[str]:
        addresses = []
        with open(filename, "r") as f:
            lines = f.readlines()

        with open(self.invalid_file, "w") as invalid:
            for line in lines:
                key = line.strip()
                if len(key) == 34 and key.startswith("T"):
                    addresses.append(key)
                elif len(key) == 64:
                    addr = self.client.get_address_from_private_key(key)
                    if addr:
                        addresses.append(addr)
                    else:
                        invalid.write(key + "\n")
                else:
                    invalid.write(key + "\n")
        return addresses

    def check_wallet(self, address: str) -> Optional[WalletResult]:
        trx = self.client.get_trx_balance(address)
        token_bal, symbol = 0.0, ""
        
        if self.token_contract:
            token_bal, symbol = self.client.get_token_balance(address, self.token_contract)

        has_balance = trx > 0 or token_bal > 0
        result = WalletResult(
            address=address,
            trx_balance=trx,
            token_balance=token_bal,
            token_symbol=symbol,
            has_balance=has_balance
        )
        
        if has_balance:
            self._save_result(result)
        return result if has_balance else None

    def _save_result(self, result: WalletResult):
        with open(self.found_file, "a") as f:
            line = f"{result.address} | TRX: {result.trx_balance:.6f}"
            if result.token_balance > 0:
                line += f" | {result.token_symbol}: {result.token_balance}"
            f.write(line + "\n")

    def scan(self, addresses: List[str]) -> List[WalletResult]:
        found = []
        total = len(addresses)
        start = time.time()

        chunks = [addresses[i:i+CHUNK_SIZE] for i in range(0, len(addresses), CHUNK_SIZE)]

        for idx, chunk in enumerate(chunks):
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.check_wallet, addr): addr for addr in chunk}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        res = future.result()
                        if res:
                            found.append(res)
                    except Exception as e:
                        logger.error(f"Error checking {futures[future]}: {e}")

            processed = (idx + 1) * CHUNK_SIZE
            self._print_progress(processed, total, start)

        print("\nScan complete.")
        return found

    def _print_progress(self, current: int, total: int, start_time: float):
        elapsed = time.time() - start_time
        pct = (current / total) * 100
        eta = (elapsed / current) * (total - current) if current > 0 else 0
        sys.stdout.write(f"\rProgress: {current}/{total} ({pct:.1f}%) | ETA: {eta:.0f}s")
        sys.stdout.flush()

# ====================== MAIN EXECUTION ======================
def main():
    token_contract = None
    for arg in sys.argv:
        if arg.startswith("--token="):
            token_contract = arg.split("=")[1]

    scanner = TronScanner(token_contract=token_contract)
    addresses = scanner.load_wallets()

    if not addresses:
        print("No valid addresses found.")
        return

    print(f"Scanning {len(addresses)} addresses...")
    results = scanner.scan(addresses)

    print(f"\nFound {len(results)} wallets with balance.")
    if results:
        print("Results saved to found.txt")

    # Example usage of transfer/approve (uncomment to use)
    # client = scanner.client
    # result = client.transfer_token("YOUR_PRIVATE_KEY", token_contract, "TO_ADDRESS", 1000000)
    # print(result)

if __name__ == "__main__":
    
