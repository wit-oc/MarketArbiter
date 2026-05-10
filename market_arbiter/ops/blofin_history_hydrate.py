from __future__ import annotations

import argparse
import json
import time

from market_arbiter.core.db import init_db
from market_arbiter.feed import BlofinEnvironment, BlofinGapRecoveryEngine, BlofinPublicRestClient


def _now_ms() -> int:
    return int(time.time() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydrate BloFin history windows into the canonical MarketArbiter store.")
    parser.add_argument("--db-path", default="data/market_arbiter.sqlite")
    parser.add_argument("--environment", choices=("demo", "prod"), default="demo")
    parser.add_argument("--symbols", default="BTC-USDT")
    parser.add_argument("--last-closed-ts-open-ms", type=int, default=None)
    parser.add_argument("--requests-per-minute", type=int, default=120)
    args = parser.parse_args()

    conn = init_db(args.db_path)
    client = BlofinPublicRestClient(
        environment=BlofinEnvironment(args.environment),
        requests_per_minute=int(args.requests_per_minute),
    )
    engine = BlofinGapRecoveryEngine(conn, client)
    now_ms = _now_ms()
    try:
        outputs = []
        for symbol in [item.strip().upper() for item in args.symbols.split(",") if item.strip()]:
            trace_id = f"blofin-hydrate:{symbol}:{now_ms}"
            outputs.append(
                engine.recover_symbol(
                    symbol=symbol,
                    now_ms=now_ms,
                    trace_id=trace_id,
                    last_closed_ts_open_ms=args.last_closed_ts_open_ms,
                )
            )
        print(json.dumps({"ok": True, "environment": args.environment, "runs": outputs}, indent=2))
    finally:
        client.close()
        conn.close()


if __name__ == "__main__":
    main()

