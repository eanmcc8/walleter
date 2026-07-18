```python
"""
TRON Token Scanner & Transfer Utility
Advanced, production-grade, fully optimized
Features:
- TRX + TRC20 token balance scanning
- Token transfer & approval (TRC20)
- Rate limiting, retry, concurrency
- Professional logging & error handling
- CLI with argparse
"""

import os
import sys
import time
import json
import logging
import argparse
import requests
from typing import List, Dict, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from dataclasses import dataclass, field
from tronapi import Tron
from tronapi import HttpProvider
from eth_utils import to_checksum_address
import base58

# ====================== CONFIG ======================
DEFAULT_NODE = "https://api.trongrid.io"
TRONSCAN_API = "https://apilist.tronscanapi.com"
RATE_LIMIT = 8  # calls per second
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.5
CHUNK_SIZE = 5
MAX_WORKERS = 5
# ===================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("tron-scanner")


@dataclass
class Config:
    api_key: str
    token_contract: Optional[str] = None
    node_url: str = DEFAULT_NODE
    output_dir: str = "."
    found_file: str = field(init=False)
    invalid_file: str = field(init=False)

    def __post_init__(self):
        self.found_file = os.path.join(self.output_dir, "found.txt")
        self.invalid_file = os.path.join(self.output_dir, "invalid.txt")


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


class TronClient:
    def __init__(self, config: Config):
        self.config = config
        self.tron = Tron(
            full_node=HttpProvider(config.node_url),
            solidity_node=HttpProvider(config.node_url),
            event_server=HttpProvider(config.node_url)
        )
        self.rate_limiter = RateLimiter(RATE_LIMIT)
        self.session = requests.Session()
        self.session.headers.update({"TRON-PRO-API-KEY": config.api_key})

    def _retry_request(self, func, *args, **kwargs):
        for attempt in range(RETRY_ATTEMPTS):
            try:
                with self.rate_limiter:
                    return func(*args, **kwargs)
            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    raise
                time.sleep(RETRY_BACKOFF ** attempt)
                logger.warning(f"Retry {attempt + 1}/{RETRY_ATTEMPTS} after error: {e}")

    def get_trx_balance(self, address: str) -> float:
        def _get():
            balance_sun = self.tron.trx.get_balance(address)
            return balance_sun / 1_000_000
        return self._retry_request(_get)

    def get_token_balance(self, address: str, contract: str) -> Tuple[float, str]:
        def _get():
            url = f"{TRONSCAN_API}/api/account/wallet?address={address}&asset_type=0"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for token in data.get("data", []):
                if token.get("token_id") == contract:
                    return float(token.get("balance", 0)), token.get("token_abbr", "")
            return 0.0, ""
        return self._retry_request(_get)

    def get_address_from_private_key(self, private_key: str) -> str:
        local_tron = Tron(
            full_node=HttpProvider(self.config.node_url),
            solidity_node=HttpProvider(self.config.node_url),
            event_server=HttpProvider(self.config.node_url)
        )
        local_tron.private_key = private_key
        return local_tron.address.from_private_key(private_key).base58

    # ====================== TRANSFER & APPROVAL ======================
    def transfer_trc20(self, private_key: str, to_address: str, amount: float, contract: str, decimals: int = 6) -> str:
        """Transfer TRC20 tokens. Returns txid."""
        tron = Tron(
            full_node=HttpProvider(self.config.node_url),
            solidity_node=HttpProvider(self.config.node_url),
            event_server=HttpProvider(self.config.node_url)
        )
        tron.private_key = private_key
        tron.default_address = tron.address.from_private_key(private_key).base58

        contract = tron.get_contract(contract)
        amount_in_units = int(amount * (10 ** decimals))

        tx = contract.functions.transfer(to_address, amount_in_units).with_owner(tron.default_address).fee_limit(100_000_000).build().sign(tron.private_key).broadcast()
        return tx.get("txid", "")

    def approve_trc20(self, private_key: str, spender: str, amount: float, contract: str, decimals: int = 6) -> str:
        """Approve spender for TRC20 tokens. Returns txid."""
        tron = Tron(
            full_node=HttpProvider(self.config.node_url),
            solidity_node=HttpProvider(self.config.node_url),
            event_server=HttpProvider(self.config.node_url)
        )
        tron.private_key = private_key
        tron.default_address = tron.address.from_private_key(private_key).base58

        contract = tron.get_contract(contract)
        amount_in_units = int(amount * (10 ** decimals))

        tx = contract.functions.approve(spender, amount_in_units).with_owner(tron.default_address).fee_limit(100_000_000).build().sign(tron.private_key).broadcast()
        return tx.get("txid", "")


class AddressScanner:
    def __init__(self, config: Config):
        self.config = config
        self.client = TronClient(config)
        self.found_lock = Lock()
        self.invalid_lock = Lock()

    def _save_found(self, address: str, trx: float, token: float = 0.0, token_abbr: str = ""):
        with self.found_lock:
            with open(self.config.found_file, "a") as f:
                line = f"{address} : TRX {trx:.6f}"
                if token > 0:
                    line += f", {token_abbr} {token:.6f}"
                f.write(line + "\n")

    def _save_invalid(self, key: str):
        with self.invalid_lock:
            with open(self.config.invalid_file, "a") as f:
                f.write(key + "\n")

    def check_address(self, address: str) -> Optional[Dict]:
        try:
            trx = self.client.get_trx_balance(address)
            token = 0.0
            abbr = ""

            if self.config.token_contract:
                token, abbr = self.client.get_token_balance(address, self.config.token_contract)

            if trx > 0 or token > 0:
                self._save_found(address, trx, token, abbr)
                return {"address": address, "trx": trx, "token": token, "token_abbr": abbr}
        except Exception as e:
            logger.error(f"Error checking {address}: {e}")
        return None

    def process_file(self, filename: str) -> List[Dict]:
        addresses = []
        with open(filename, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        for line in lines:
            if len(line) == 34 and line.startswith("T"):
                addresses.append(line)
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


def main():
    parser = argparse.ArgumentParser(description="TRON Scanner + Transfer Tool")
    parser.add_argument("--input", required=True, help="Input file with addresses or private keys")
    parser.add_argument("--token", help="TRC20 token contract address")
    parser.add_argument("--api-key", help="Tronscan API key (or set in api.txt)")
    parser.add_argument("--transfer", action="store_true", help="Enable transfer mode")
    parser.add_argument("--approve", action="store_true", help="Enable approve mode")
    parser.add_argument("--to", help="Destination address for transfer/approve")
    parser.add_argument("--amount", type=float, help="Amount to transfer/approve")
    parser.add_argument("--private-key", help="Private key for transfer/approve")
    parser.add_argument("--decimals", type=int, default=6, help="Token decimals")
    args = parser.parse_args()

    # API Key handling
    api_key = args.api_key
    if not api_key:
        if os.path.exists("api.txt"):
            with open("api.txt") as f:
                api_key = f.read().strip()
        else:
            api_key = input("Enter Tronscan API Key: ").strip()
            with open("api.txt", "w") as f:
                f.write(api_key)

    config = Config(api_key=api_key, token_contract=args.token)

    if args.transfer or args.approve:
        if not all([args.private_key, args.to, args.amount is not None]):
            logger.error("Transfer/Approve requires --private-key, --to, and --amount")
            sys.exit(1)

        client = TronClient(config)
        if args.transfer:
            txid = client.transfer_trc20(args.private_key, args.to, args.amount, args.token or "", args.decimals)
            logger.info(f"Transfer TXID: {txid}")
        elif args.approve:
            txid = client.approve_trc20(args.private_key, args.to, args.amount, args.token or "", args.decimals)
            logger.info(f"Approve TXID: {txid}")
        return

    # Scanning mode
    scanner = AddressScanner(config)
    addresses = scanner.process_file(args.input)
    logger.info(f"Loaded {len(addresses)} valid addresses")

    results = scanner.scan(addresses)

    logger.info(f"Scan complete. Found {len(results)} addresses with balance.")
    if results:
        for r in results:
            logger.info(f"{r['address']} | TRX: {r['trx']:.6f} | Token: {r['token']:.6f} {r['token_abbr']}")


if __name__ == "__main__":
    main()
