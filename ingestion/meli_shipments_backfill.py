"""
meli_shipments_backfill.py
==========================
Roda UMA VEZ para buscar os custos de envio de todas as orders
já salvas na Bronze.

Estratégia:
  1. Lista todas as partições da Bronze de orders no S3
  2. Lê cada arquivo JSONL e extrai os shipping_ids únicos
  3. Busca os custos de cada shipment na API
  4. Salva na partição correta da Bronze de shipments
     (mantém a mesma partição year/month/day do arquivo de orders)

Execute:
    python meli_shipments_backfill.py
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv

from meli_auth import MeliAuth
from s3_helper import S3Writer

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

AWS_BUCKET         = os.getenv("AWS_BUCKET", "tcc-uspesalq")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")

ORDERS_PREFIX      = "bronze/mercado_livre/orders"
SHIPMENTS_PREFIX   = "bronze/mercado_livre/shipments"

MELI_BASE_URL      = "https://api.mercadolibre.com"


# ---------------------------------------------------------------------------
# Lista todos os arquivos JSONL da Bronze de orders
# ---------------------------------------------------------------------------

def list_all_order_files(bucket: str, region: str) -> list[dict]:
    """
    Lista todos os arquivos JSONL na Bronze de orders.
    Retorna lista de dicts com key e execution_dt inferida da partição.

    Exemplo de key:
        bronze/mercado_livre/orders/year=2026/month=04/day=26/orders_20260419_20260426.jsonl
    """
    s3       = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    pages    = paginator.paginate(Bucket=bucket, Prefix=f"{ORDERS_PREFIX}/")

    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue

            # Extrai year/month/day da partição para replicar na Bronze de shipments
            # ex: bronze/mercado_livre/orders/year=2026/month=04/day=26/orders_...jsonl
            try:
                parts  = key.split("/")
                year   = int(parts[3].split("=")[1])
                month  = int(parts[4].split("=")[1])
                day    = int(parts[5].split("=")[1])
                exec_dt = datetime(year, month, day, tzinfo=timezone.utc)
            except (IndexError, ValueError):
                log.warning(f"Não foi possível extrair data da partição: {key} — pulando")
                continue

            files.append({"key": key, "execution_dt": exec_dt})

    log.info(f"Total de arquivos encontrados na Bronze: {len(files)}")
    return files


# ---------------------------------------------------------------------------
# Extrai shipping_ids de um arquivo JSONL
# ---------------------------------------------------------------------------

def get_shipping_ids(bucket: str, region: str, key: str) -> list[int]:
    s3   = boto3.client("s3", region_name=region)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    shipping_ids = set()
    for line in body.strip().splitlines():
        order       = json.loads(line)
        shipping_id = order.get("shipping", {}).get("id")
        if shipping_id:
            shipping_ids.add(int(shipping_id))

    return list(shipping_ids)


# ---------------------------------------------------------------------------
# Cliente de Shipments
# ---------------------------------------------------------------------------

class MeliShipmentsClient:
    def __init__(self, auth: MeliAuth):
        self.auth    = auth
        self.session = requests.Session()

    def _get(self, url: str, headers_extra: dict = None) -> dict:
        headers = {**self.auth.headers(), **(headers_extra or {})}
        resp    = self.session.get(url, headers=headers, timeout=30)

        if resp.status_code == 401:
            log.warning("401 recebido — renovando token...")
            self.auth._refresh_tokens()
            headers = {**self.auth.headers(), **(headers_extra or {})}
            resp    = self.session.get(url, headers=headers, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def fetch_costs(self, shipment_id: int) -> dict:
        return self._get(
            url=f"{MELI_BASE_URL}/shipments/{shipment_id}/costs",
            headers_extra={"x-format-new": "true"},
        )

    def fetch_all(self, shipment_ids: list[int]) -> list[dict]:
        results = []
        for i, shipment_id in enumerate(shipment_ids, start=1):
            log.info(f"  Shipment {i}/{len(shipment_ids)} | id={shipment_id}")
            try:
                costs               = self.fetch_costs(shipment_id)
                costs["shipment_id"] = shipment_id
                results.append(costs)
            except Exception as e:
                log.error(f"  Erro no shipment {shipment_id}: {e}")
                continue
        return results


# ---------------------------------------------------------------------------
# Backfill principal
# ---------------------------------------------------------------------------

def run_backfill():
    log.info("▶ Iniciando backfill de shipments")

    # Autenticação
    auth = MeliAuth(
        client_id=MELI_CLIENT_ID,
        client_secret=MELI_CLIENT_SECRET,
        bucket=AWS_BUCKET,
        region=AWS_REGION,
    )
    auth.authenticate()

    client = MeliShipmentsClient(auth)
    writer = S3Writer(bucket=AWS_BUCKET, region=AWS_REGION, prefix=SHIPMENTS_PREFIX)
    s3     = boto3.client("s3", region_name=AWS_REGION)

    # Lista todos os arquivos da Bronze de orders
    order_files   = list_all_order_files(AWS_BUCKET, AWS_REGION)
    total_files   = len(order_files)
    total_saved   = 0

    for i, file_info in enumerate(order_files, start=1):
        key          = file_info["key"]
        execution_dt = file_info["execution_dt"]

        log.info(f"{'='*55}")
        log.info(f"Arquivo {i}/{total_files}: {key}")
        log.info(f"{'='*55}")

        # Verifica se já existe arquivo de shipments para essa partição
        partition = (
            f"{SHIPMENTS_PREFIX}/"
            f"year={execution_dt.year:04d}/"
            f"month={execution_dt.month:02d}/"
            f"day={execution_dt.day:02d}/"
        )
        existing = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=partition, MaxKeys=1)
        if existing.get("KeyCount", 0) > 0:
            log.info(f"  Partição já processada — pulando\n")
            continue

        # Extrai shipping_ids
        shipping_ids = get_shipping_ids(AWS_BUCKET, AWS_REGION, key)
        log.info(f"  {len(shipping_ids)} shipping_ids encontrados")

        if not shipping_ids:
            log.info("  Nenhum shipping_id — pulando\n")
            continue

        # Busca custos na API
        shipments = client.fetch_all(shipping_ids)

        if shipments:
            date_str = execution_dt.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
            s3_path  = writer.write(
                records=shipments,
                date_from=date_str,
                date_to=date_str,
                execution_dt=execution_dt,
                file_name="shipments",
            )
            total_saved += len(shipments)
            log.info(f"  ✅ {len(shipments)} shipments salvos → {s3_path}\n")
        else:
            log.info("  Nenhum shipment retornado\n")

    log.info("=" * 55)
    log.info(f"✅ Backfill concluído!")
    log.info(f"   Arquivos processados : {total_files}")
    log.info(f"   Total de shipments   : {total_saved}")
    log.info("=" * 55)


if __name__ == "__main__":
    run_backfill()
