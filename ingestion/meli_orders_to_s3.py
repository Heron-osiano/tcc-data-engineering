"""
meli_orders_to_s3.py
====================
Ingestão incremental de Orders do Mercado Livre → S3 Bronze.

Caminho de destino:
  s3://tcc-uspesalq/bronze/mercado_livre/orders/year=YYYY/month=MM/day=DD/

Pode ser rodado:
  - Localmente : python meli_orders_to_s3.py
  - Airflow    : PythonOperator chamando a função run()
"""

import logging
import os
from datetime import datetime, timedelta, timezone
import pytz
from typing import Optional

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


# ---------------------------------------------------------------------------
# Cliente de Orders
# ---------------------------------------------------------------------------

class MeliOrdersClient:
    def __init__(self, auth: MeliAuth, seller_id: str):
        self.auth      = auth
        self.seller_id = seller_id
        self.session   = requests.Session()

    def _get(self, url: str, params: dict) -> dict:
        """GET com retry automático em caso de 401."""
        resp = self.session.get(url, headers=self.auth.headers(), params=params, timeout=30)

        if resp.status_code == 401:
            log.warning("401 recebido durante chamada — forçando renovação do token...")
            self.auth._refresh_tokens()
            resp = self.session.get(url, headers=self.auth.headers(), params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def fetch_orders_page(self, date_from: str, date_to: str, offset: int = 0) -> dict:
        return self._get(
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

    def fetch_all_orders(self, date_from: str, date_to: str) -> list[dict]:
        all_orders = []
        offset     = 0

        for page in range(MAX_PAGES):
            log.info(f"Página {page + 1} | offset={offset}")
            data   = self.fetch_orders_page(date_from, date_to, offset)
            orders = data.get("results", [])

            if not orders:
                log.info("Paginação concluída.")
                break

            all_orders.extend(orders)

            total  = data.get("paging", {}).get("total", 0)
            offset += len(orders)
            log.info(f"  {len(orders)} orders recebidas | acumulado: {len(all_orders)}/{total}")

            if offset >= total:
                break
        else:
            log.warning(f"Limite de {MAX_PAGES} páginas atingido.")

        return all_orders


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def run(**kwargs) -> dict:
    """
    Ponto de entrada do pipeline de orders.
    Pode ser chamado pelo Airflow ou diretamente.
    """
    brasilia = pytz.timezone("America/Sao_Paulo")
    now      = datetime.now(brasilia)
    log.info(f"▶ Iniciando ingestão de orders | execution_date={now.isoformat()}")

    # 1. Autenticação — valida o token do S3 e renova se necessário
    auth = MeliAuth(
        client_id=MELI_CLIENT_ID,
        client_secret=MELI_CLIENT_SECRET,
        bucket=AWS_BUCKET,
        region=AWS_REGION,
    )
    auth.authenticate()

    # 2. Demais componentes
    client = MeliOrdersClient(auth, MELI_SELLER_ID)
    state  = S3StateManager(bucket=AWS_BUCKET, region=AWS_REGION, state_key=STATE_KEY)
    writer = S3Writer(bucket=AWS_BUCKET, region=AWS_REGION, prefix=BRONZE_PREFIX)

    # 3. Janela incremental
    last_run  = state.get_last_run()
    date_from = last_run if last_run else (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
    date_to   = now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
    log.info(f"Janela: {date_from} → {date_to}")

    # 4. Coleta
    orders = client.fetch_all_orders(date_from, date_to)

    # 5. Persiste no S3
    s3_path = None
    if orders:
        s3_path = writer.write(records=orders, date_from=date_from, date_to=date_to, execution_dt=now, file_name="orders")
    else:
        log.info("Nenhuma order nova no período.")

    # 6. Atualiza cursor incremental
    state.save_last_run(date_to)

    result = {
        "orders_count": len(orders),
        "date_from":    date_from,
        "date_to":      date_to,
        "s3_path":      s3_path,
    }
    log.info(f"✅ Concluído: {result}")
    return result


if __name__ == "__main__":
    import json
    print(json.dumps(run(), indent=2))
