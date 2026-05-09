"""
meli_orders_backfill.py
=======================
Processa orders retroativamente de 6 meses atrás até hoje,
em janelas de 7 dias, cada janela salva na sua própria partição.

Partição por data de execução simulada (data fim de cada janela):
    year=2025/month=11/day=02/orders_20251026_20251102.jsonl
    year=2025/month=11/day=09/orders_20251102_20251109.jsonl
    ...

Execute:
    python meli_orders_backfill.py
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

from meli_auth import MeliAuth
from s3_helper import S3StateManager, S3Writer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------

MELI_CLIENT_ID     = os.getenv("MELI_CLIENT_ID")
MELI_CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET")
MELI_SELLER_ID     = os.getenv("MELI_SELLER_ID")

AWS_BUCKET         = os.getenv("AWS_BUCKET", "tcc-uspesalq")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")

BRONZE_PREFIX      = "bronze/mercado_livre/orders"
STATE_KEY          = "config/mercado_livre/orders_last_run.json"

MELI_BASE_URL      = "https://api.mercadolibre.com"
PAGE_SIZE          = 50
MAX_PAGES          = 200

WINDOW_DAYS        = 7
MONTHS_BACK        = 6


# ---------------------------------------------------------------------------
# Cliente de Orders
# ---------------------------------------------------------------------------

class MeliOrdersClient:
    def __init__(self, auth: MeliAuth, seller_id: str):
        self.auth      = auth
        self.seller_id = seller_id
        self.session   = requests.Session()

    def _get(self, url: str, params: dict) -> dict:
        resp = self.session.get(url, headers=self.auth.headers(), params=params, timeout=30)

        if resp.status_code == 401:
            log.warning("401 recebido — renovando token...")
            self.auth._refresh_tokens()
            resp = self.session.get(url, headers=self.auth.headers(), params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def fetch_all_orders(self, date_from: str, date_to: str) -> list[dict]:
        all_orders = []
        offset     = 0

        for page in range(MAX_PAGES):
            log.info(f"  Página {page + 1} | offset={offset}")
            data = self._get(
                url=f"{MELI_BASE_URL}/orders/search",
                params={
                    "seller":                       self.seller_id,
                    "order.date_last_updated.from": date_from,
                    "order.date_last_updated.to":   date_to,
                    "sort":                         "date_asc",
                    "offset":                       offset,
                    "limit":                        PAGE_SIZE,
                },
            )
            orders = data.get("results", [])

            if not orders:
                break

            all_orders.extend(orders)
            total  = data.get("paging", {}).get("total", 0)
            offset += len(orders)
            log.info(f"  {len(orders)} orders | acumulado: {len(all_orders)}/{total}")

            if offset >= total:
                break

        return all_orders


# ---------------------------------------------------------------------------
# Gerador de janelas
# ---------------------------------------------------------------------------

def generate_windows(months_back: int, window_days: int) -> list[tuple]:
    """
    Gera lista de tuplas (date_from, date_to, execution_dt).

    execution_dt = date_to de cada janela, usado para determinar
    a partição year/month/day de cada arquivo no S3.

    Resultado com MONTHS_BACK=6 e WINDOW_DAYS=7:
        ("2025-10-26...", "2025-11-02...", datetime(2025,11,2))
        ("2025-11-02...", "2025-11-09...", datetime(2025,11,9))
        ...
        ("2026-04-19...", "2026-04-26...", datetime(2026,4,26))
    """
    now    = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start  = now - timedelta(days=months_back * 30)
    cursor = start
    windows = []

    while cursor < now:
        window_end = min(cursor + timedelta(days=window_days), now)
        windows.append((
            cursor.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
            window_end.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
            window_end,   # execution_dt — define a partição no S3
        ))
        cursor = window_end

    return windows


# ---------------------------------------------------------------------------
# Backfill principal
# ---------------------------------------------------------------------------

def run_backfill():
    log.info("▶ Iniciando backfill de orders")

    auth = MeliAuth(
        client_id=MELI_CLIENT_ID,
        client_secret=MELI_CLIENT_SECRET,
        bucket=AWS_BUCKET,
        region=AWS_REGION,
    )
    auth.authenticate()

    client  = MeliOrdersClient(auth, MELI_SELLER_ID)
    writer  = S3Writer(bucket=AWS_BUCKET, region=AWS_REGION, prefix=BRONZE_PREFIX)
    state   = S3StateManager(bucket=AWS_BUCKET, region=AWS_REGION, state_key=STATE_KEY)

    windows       = generate_windows(MONTHS_BACK, WINDOW_DAYS)
    total_windows = len(windows)

    log.info(f"Total de janelas : {total_windows} ({MONTHS_BACK} meses / {WINDOW_DAYS} dias cada)")
    log.info(f"Período          : {windows[0][0][:10]} → {windows[-1][1][:10]}\n")

    resultados = []

    for i, (date_from, date_to, execution_dt) in enumerate(windows, start=1):
        log.info(f"{'='*55}")
        log.info(f"Janela {i}/{total_windows}: {date_from[:10]} → {date_to[:10]}")
        log.info(f"Partição: year={execution_dt.year}/month={execution_dt.month:02d}/day={execution_dt.day:02d}")
        log.info(f"{'='*55}")

        orders = client.fetch_all_orders(date_from, date_to)

        if orders:
            s3_path = writer.write(
                records=orders,
                date_from=date_from,
                date_to=date_to,
                execution_dt=execution_dt,
                file_name="orders",
            )
            log.info(f"✅ {len(orders)} orders salvas → {s3_path}\n")
        else:
            s3_path = None
            log.info(f"⚠️  Nenhuma order nessa janela — pulando\n")

        resultados.append({
            "janela":       f"{date_from[:10]} → {date_to[:10]}",
            "particao":     f"year={execution_dt.year}/month={execution_dt.month:02d}/day={execution_dt.day:02d}",
            "orders_count": len(orders),
            "s3_path":      s3_path,
        })

    # Atualiza o cursor para o fim do backfill
    state.save_last_run(windows[-1][1])

    total_orders = sum(r["orders_count"] for r in resultados)
    log.info("=" * 55)
    log.info(f"✅ Backfill concluído!")
    log.info(f"   Janelas processadas : {total_windows}")
    log.info(f"   Total de orders     : {total_orders}")
    log.info("=" * 55)

    return resultados


if __name__ == "__main__":
    resultados = run_backfill()
    print("\nResumo:")
    print(json.dumps(resultados, indent=2, ensure_ascii=False))
