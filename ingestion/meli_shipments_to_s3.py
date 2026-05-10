"""
meli_shipments_to_s3.py
=======================
Ingestão de custos de envio do Mercado Livre → S3 Bronze.

Estratégia:
  1. Lê o JSONL de orders do dia já salvo no S3
  2. Extrai os shipping_ids únicos
  3. Busca os custos de cada shipment na API
  4. Salva o resultado no S3

Caminho de destino:
  s3://tcc-uspesalq/bronze/mercado_livre/shipments/year=YYYY/month=MM/day=DD/

Pode ser rodado:
  - Localmente : python meli_shipments_to_s3.py
  - Airflow    : PythonOperator chamando a função run()
             (rodar sempre APÓS o meli_orders_to_s3)
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from botocore.exceptions import ClientError
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
# Lê os shipping_ids das orders do dia já salvas no S3
# ---------------------------------------------------------------------------

def get_shipping_ids_from_orders(bucket: str, region: str, execution_dt: datetime) -> list[int]:
    """
    Lê o JSONL de orders da partição do dia e extrai os shipping_ids únicos.
    """
    s3     = boto3.client("s3", region_name=region)
    prefix = (
        f"{ORDERS_PREFIX}/"
        f"year={execution_dt.year:04d}/"
        f"month={execution_dt.month:02d}/"
        f"day={execution_dt.day:02d}/"
    )

    # Lista os arquivos da partição do dia
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files    = response.get("Contents", [])

    if not files:
        log.warning(f"Nenhum arquivo de orders encontrado em {prefix}")
        return []

    shipping_ids = set()

    for obj in files:
        log.info(f"Lendo: {obj['Key']}")
        body   = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
        orders = [json.loads(line) for line in body.strip().splitlines()]

        for order in orders:
            shipping_id = order.get("shipping", {}).get("id")
            if shipping_id:
                shipping_ids.add(int(shipping_id))

    log.info(f"{len(shipping_ids)} shipping_ids únicos encontrados nas orders do dia")
    return list(shipping_ids)


# ---------------------------------------------------------------------------
# Cliente de Shipments
# ---------------------------------------------------------------------------

class MeliShipmentsClient:
    def __init__(self, auth: MeliAuth):
        self.auth    = auth
        self.session = requests.Session()

    def _get(self, url: str, headers_extra: dict = None) -> dict:
        """GET com retry automático em caso de 401."""
        headers = {**self.auth.headers(), **(headers_extra or {})}
        resp    = self.session.get(url, headers=headers, timeout=30)

        if resp.status_code == 401:
            log.warning("401 recebido — renovando token...")
            self.auth._refresh_tokens()
            headers = {**self.auth.headers(), **(headers_extra or {})}
            resp    = self.session.get(url, headers=headers, timeout=30)

        resp.raise_for_status()
        return resp.json()

    def fetch_shipment_costs(self, shipment_id: int) -> dict:
        """
        Busca os custos de um shipment.
        Retorna cost (pago pelo comprador) e list_cost (pago pelo vendedor).
        """
        return self._get(
            url=f"{MELI_BASE_URL}/shipments/{shipment_id}/costs",
            headers_extra={"x-format-new": "true"},
        )

    def fetch_all(self, shipment_ids: list[int]) -> list[dict]:
        """
        Busca os custos de cada shipment_id.
        """
        results = []

        for i, shipment_id in enumerate(shipment_ids, start=1):
            log.info(f"Shipment {i}/{len(shipment_ids)} | id={shipment_id}")
            try:
                costs = self.fetch_shipment_costs(shipment_id)
                costs["shipment_id"] = shipment_id
                results.append(costs)

            except Exception as e:
                log.error(f"Erro ao buscar custos do shipment {shipment_id}: {e}")
                continue

        return results


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def run(**kwargs) -> dict:
    """
    Ponto de entrada do pipeline de shipments.
    Deve rodar APÓS o meli_orders_to_s3 do mesmo dia.
    """
    now = datetime.now(timezone.utc)
    log.info(f"▶ Iniciando ingestão de shipments | execution_date={now.isoformat()}")

    # 1. Autenticação
    auth = MeliAuth(
        client_id=MELI_CLIENT_ID,
        client_secret=MELI_CLIENT_SECRET,
        bucket=AWS_BUCKET,
        region=AWS_REGION,
    )
    auth.authenticate()

    # 2. Extrai shipping_ids das orders do dia
    shipping_ids = get_shipping_ids_from_orders(AWS_BUCKET, AWS_REGION, now)

    if not shipping_ids:
        log.info("Nenhum shipping_id encontrado — nada a processar.")
        return {"shipments_count": 0, "s3_path": None}

    # 3. Busca os shipments na API
    client    = MeliShipmentsClient(auth)
    shipments = client.fetch_all(shipping_ids)

    # 4. Salva no S3
    s3_path = None
    if shipments:
        writer  = S3Writer(bucket=AWS_BUCKET, region=AWS_REGION, prefix=SHIPMENTS_PREFIX)
        date_str = now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
        s3_path  = writer.write(
            records=shipments,
            date_from=date_str,
            date_to=date_str,
            execution_dt=now,
            file_name="shipments",
        )
        log.info(f"✅ {len(shipments)} shipments salvos → {s3_path}")
    else:
        log.info("Nenhum shipment retornado pela API.")

    result = {
        "shipments_count": len(shipments),
        "s3_path":         s3_path,
    }
    log.info(f"✅ Concluído: {result}")
    return result


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
