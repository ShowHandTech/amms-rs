#!/usr/bin/env python3
"""Sync Binance Alpha token list -> Redis (tokens:{namespace}:active + changes).

This is a TEMPORARY bootstrap helper that takes the role of the future
"token service" described in the project plan. It is NOT meant to live
inside amms-rs long-term — once a proper token service exists (HTTP CRUD,
multi-source aggregation, anti-spam), this script should be retired.

What it does
------------
1. GET https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list
2. Filter to chainName == BSC, drop offline / fullyDelisted, drop low-liquidity dust.
3. Diff against tokens:{namespace}:active in Redis.
4. For new tokens:    SADD + PUBLISH "TRACK 0x..."
5. For dropped tokens: SREM + PUBLISH "UNTRACK 0x..."
6. Core tokens are NEVER removed (amms-rs would refuse, but we skip it
   client-side too so we don't spam the changes channel).

Usage
-----
    pip install requests redis
    python3 scripts/binance_alpha_sync.py \
        --redis-url "redis://:password@127.0.0.1:6379/3" \
        --namespace bsc \
        --liquidity-min 50000

    # Dry-run (print diff, don't write anything):
    python3 scripts/binance_alpha_sync.py --redis-url ... --dry-run

    # Run periodically via cron / systemd timer / a `while sleep 600` loop:
    while true; do python3 scripts/binance_alpha_sync.py --redis-url ... ; sleep 600; done
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Iterable

import redis
import requests


BINANCE_ALPHA_URL = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/"
    "cex/alpha/all/token/list"
)

# Core tokens that must NEVER be untracked. Keep in sync with the
# [core_tokens].addresses section of examples/configs/bsc-token-first.toml.
# Stored as lowercase strings for cheap set membership comparison.
DEFAULT_CORE_TOKENS_BSC = {
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  # USD1
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
}

CHAIN_TO_BINANCE_NAME = {
    "bsc": "BSC",
    "ethereum": "Ethereum",
    "base": "Base",
    "arbitrum": "Arbitrum",
    "solana": "Solana",
}


def fetch_binance_alpha() -> list[dict]:
    resp = requests.get(BINANCE_ALPHA_URL, timeout=15)
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") != "000000":
        raise RuntimeError(f"binance alpha api error: {body}")
    return body["data"]


def _safe_float(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def filter_tokens(
    raw: Iterable[dict],
    chain_name: str,
    liquidity_min: float,
    volume_min: float,
) -> set[str]:
    """Return a set of lowercase contract addresses that pass the filter."""
    out: set[str] = set()
    for t in raw:
        if t.get("chainName") != chain_name:
            continue
        if t.get("offline"):
            continue
        if t.get("fullyDelisted"):
            continue
        if _safe_float(t.get("liquidity")) < liquidity_min:
            continue
        if _safe_float(t.get("volume24h")) < volume_min:
            continue
        addr = t.get("contractAddress")
        if not addr:
            continue
        out.add(addr.lower())
    return out


def sync(
    r: redis.Redis,
    namespace: str,
    desired: set[str],
    core_tokens: set[str],
    dry_run: bool,
) -> tuple[set[str], set[str]]:
    """Apply diff to Redis. Returns (added, removed) sets."""
    active_key = f"tokens:{namespace}:active"
    changes_channel = f"tokens:{namespace}:changes"

    current_raw: set[bytes] = r.smembers(active_key)
    current = {s.decode().lower() for s in current_raw}

    # Always-on baseline: core tokens are forced into `desired` so that even if
    # Binance Alpha drops one (e.g. USDT off the list briefly), we don't try to
    # untrack a protected token.
    desired = desired | core_tokens

    to_add = desired - current
    # Core tokens never get removed, defensive even though `desired` already
    # contains them.
    to_remove = (current - desired) - core_tokens

    logging.info(
        "diff: current=%d desired=%d add=%d remove=%d (core=%d)",
        len(current),
        len(desired),
        len(to_add),
        len(to_remove),
        len(core_tokens),
    )

    if dry_run:
        for a in sorted(to_add):
            logging.info("[dry-run] TRACK   %s", a)
        for a in sorted(to_remove):
            logging.info("[dry-run] UNTRACK %s", a)
        return to_add, to_remove

    pipe = r.pipeline(transaction=False)
    if to_add:
        pipe.sadd(active_key, *to_add)
        for a in to_add:
            pipe.publish(changes_channel, f"TRACK {a}")
    if to_remove:
        pipe.srem(active_key, *to_remove)
        for a in to_remove:
            pipe.publish(changes_channel, f"UNTRACK {a}")
    pipe.execute()

    for a in sorted(to_add):
        logging.info("TRACK   %s", a)
    for a in sorted(to_remove):
        logging.info("UNTRACK %s", a)

    return to_add, to_remove


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--redis-url", required=True, help="e.g. redis://:pass@host:6379/3")
    p.add_argument(
        "--namespace",
        default="bsc",
        help="Redis key namespace; matches [redis].namespace in amms toml",
    )
    p.add_argument(
        "--chain",
        default="bsc",
        choices=sorted(CHAIN_TO_BINANCE_NAME.keys()),
        help="which chain to sync (filters Binance Alpha by chainName)",
    )
    p.add_argument(
        "--liquidity-min",
        type=float,
        default=50_000.0,
        help="minimum on-chain liquidity USD to be tracked (default 50000)",
    )
    p.add_argument(
        "--volume-min",
        type=float,
        default=0.0,
        help="minimum 24h volume USD to be tracked (default 0 = off)",
    )
    p.add_argument(
        "--core-token",
        action="append",
        default=[],
        help=(
            "additional core token (lowercase hex) that must never be untracked. "
            "Can be repeated. Built-in BSC core list is applied automatically when "
            "--chain bsc."
        ),
    )
    p.add_argument("--dry-run", action="store_true", help="print diff, do not write")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    binance_chain_name = CHAIN_TO_BINANCE_NAME[args.chain]
    logging.info(
        "syncing chain=%s namespace=%s liquidity_min=%.0f volume_min=%.0f",
        binance_chain_name,
        args.namespace,
        args.liquidity_min,
        args.volume_min,
    )

    raw = fetch_binance_alpha()
    logging.info("fetched %d total tokens from binance alpha", len(raw))

    desired = filter_tokens(
        raw,
        chain_name=binance_chain_name,
        liquidity_min=args.liquidity_min,
        volume_min=args.volume_min,
    )
    logging.info("desired (post-filter) count: %d", len(desired))

    core: set[str] = set(a.lower() for a in args.core_token)
    if args.chain == "bsc":
        core |= DEFAULT_CORE_TOKENS_BSC

    r = redis.Redis.from_url(args.redis_url)
    r.ping()

    sync(r, args.namespace, desired, core, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
