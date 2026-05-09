"""
s3_helper.py
============
- S3StateManager : controle incremental (cursor de última execução)
- S3Writer       : escrita de JSONL particionado por data de execução (year/month/day)
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


class S3StateManager:
    """
    Persiste o timestamp da última execução bem-sucedida no S3.

    Exemplo de caminho:
        config/mercado_livre/orders_last_run.json
    """

    def __init__(self, bucket: str, region: str, state_key: str):
        self.bucket    = bucket
        self.state_key = state_key
        self.s3        = boto3.client("s3", region_name=region)

    def get_last_run(self) -> Optional[str]:
        """Retorna o timestamp da última execução ou None se for a primeira."""
        try:
            obj  = self.s3.get_object(Bucket=self.bucket, Key=self.state_key)
            data = json.loads(obj["Body"].read())
            ts   = data.get("last_run_end")
            log.info(f"Último run: {ts}")
            return ts
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                log.info("Nenhum estado anterior — primeira execução.")
                return None
            raise

    def save_last_run(self, end_ts: str):
        """Salva o timestamp do fim da janela de ingestão atual."""
        payload = {
            "last_run_end": end_ts,
            "updated_at":   datetime.now(timezone.utc).isoformat(),
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=self.state_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
        )
        log.info(f"Estado salvo → last_run_end: {end_ts}")


class S3Writer:
    """
    Escreve todos os registros em um único arquivo JSONL por execução,
    particionado pela data de execução (não pela data do pedido).

    Isso garante que cada execução escreve em uma partição isolada,
    nunca sobrescrevendo dados anteriores — mesmo que a API retorne
    pedidos antigos atualizados.

    Estrutura gerada:
        s3://<bucket>/<prefix>/year=YYYY/month=MM/day=DD/<file_name>_<date_from>_<date_to>.jsonl

    Exemplo:
        bronze/mercado_livre/orders/year=2026/month=04/day=26/orders_20260419_20260426.jsonl
    """

    def __init__(self, bucket: str, region: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.s3     = boto3.client("s3", region_name=region)

    def write(
        self,
        records:      list[dict],
        date_from:    str,
        date_to:      str,
        execution_dt: datetime = None,
        file_name:    str = "data",
    ) -> str:
        """
        Salva todos os registros em um único arquivo JSONL no S3.

        Parâmetros
        ----------
        records      : lista de registros a salvar
        date_from    : início da janela de ingestão — usado no nome do arquivo
        date_to      : fim da janela de ingestão   — usado no nome do arquivo
        execution_dt : datetime da execução        — determina a partição year/month/day
                       se None, usa datetime.now(UTC)
        file_name    : prefixo do nome do arquivo (ex: "orders")

        Retorna
        -------
        Caminho s3:// do arquivo gerado
        """
        execution_dt = execution_dt or datetime.now(timezone.utc)

        # Partição pela data de EXECUÇÃO
        partition = (
            f"year={execution_dt.year:04d}/"
            f"month={execution_dt.month:02d}/"
            f"day={execution_dt.day:02d}"
        )

        # Nome do arquivo pelo range da janela de ingestão
        dt_from = datetime.fromisoformat(date_from.replace("Z", "+00:00")).strftime("%Y%m%d")
        dt_to   = datetime.fromisoformat(date_to.replace("Z", "+00:00")).strftime("%Y%m%d")

        if dt_from == dt_to:
            file_key = f"{self.prefix}/{partition}/{file_name}_{dt_from}.jsonl"
        else:
            file_key = f"{self.prefix}/{partition}/{file_name}_{dt_from}_{dt_to}.jsonl"

        # Monta o JSONL
        ingested_at = execution_dt.isoformat()
        lines = [
            json.dumps({**r, "_ingested_at": ingested_at}, ensure_ascii=False)
            for r in records
        ]
        body = "\n".join(lines)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=file_key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

        s3_path = f"s3://{self.bucket}/{file_key}"
        log.info(f"Salvo: {s3_path} ({len(records)} registros)")
        return s3_path
