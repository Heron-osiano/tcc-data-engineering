"""
meli_auth.py
============
Fluxo do authenticate() a cada execução:
  1. Carrega os tokens do S3 (access_token, refresh_token, expires_at)
  2. Verifica se o access_token ainda é válido pelo expires_at
  3a. Válido   → usa direto, sem chamar API nenhuma
  3b. Expirado → renova via refresh_token, salva os dois + novo expires_at no S3
"""

import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import requests
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

MELI_TOKEN_URL    = "https://api.mercadolibre.com/oauth/token"
TOKEN_S3_KEY      = "config/mercado_livre/tokens.json"
EXPIRY_BUFFER_SEC = 300  # renova 5 minutos antes de expirar, por segurança


class MeliAuth:
    def __init__(self, client_id: str, client_secret: str, bucket: str, region: str):
        self.client_id     = client_id
        self.client_secret = client_secret
        self.bucket        = bucket
        self.s3            = boto3.client("s3", region_name=region)

        self.access_token  = None
        self.refresh_token = None
        self.expires_at    = None  # datetime UTC

    # ------------------------------------------------------------------
    # Carrega os tokens do S3
    # ------------------------------------------------------------------

    def _load_tokens(self):
        log.info(f"Carregando tokens de s3://{self.bucket}/{TOKEN_S3_KEY}")
        try:
            obj  = self.s3.get_object(Bucket=self.bucket, Key=TOKEN_S3_KEY)
            data = json.loads(obj["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"Tokens não encontrados em s3://{self.bucket}/{TOKEN_S3_KEY}\n"
                    "Execute meli_auth_setup.py primeiro."
                )
            raise

        self.access_token  = data.get("access_token")
        self.refresh_token = data.get("refresh_token")

        # Converte expires_at de string ISO para datetime UTC
        expires_at_str = data.get("expires_at")
        if expires_at_str:
            self.expires_at = datetime.fromisoformat(expires_at_str)
        else:
            # Arquivo antigo sem expires_at — força renovação
            self.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

        if not self.access_token or not self.refresh_token:
            raise ValueError("access_token ou refresh_token ausentes no arquivo do S3.")

        log.info(f"Tokens carregados | expira em: {self.expires_at.isoformat()}")

    # ------------------------------------------------------------------
    # Verifica se o access_token ainda é válido
    # ------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        now = datetime.now(timezone.utc)
        valid = self.expires_at > (now + timedelta(seconds=EXPIRY_BUFFER_SEC))
        if valid:
            remaining = (self.expires_at - now).seconds // 60
            log.info(f"access_token válido — expira em ~{remaining} minutos.")
        else:
            log.info("access_token expirado ou prestes a expirar — renovando...")
        return valid

    # ------------------------------------------------------------------
    # Renova os tokens via API do ML
    # ------------------------------------------------------------------

    def _refresh_tokens(self):
        resp = requests.post(
            MELI_TOKEN_URL,
            headers={
                "accept":       "application/json",
                "content-type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type":    "refresh_token",
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        self.access_token  = data.get("access_token")
        self.refresh_token = data.get("refresh_token")
        expires_in         = data.get("expires_in", 21600)  # padrão 6h

        # Calcula e salva o momento exato de expiração
        self.expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        if not self.access_token or not self.refresh_token:
            raise ValueError(f"Tokens não retornados pela API. Resposta: {data}")

        log.info(f"Tokens renovados | novo expires_at: {self.expires_at.isoformat()}")
        self._save_tokens()

    # ------------------------------------------------------------------
    # Salva access_token + refresh_token + expires_at no S3
    # ------------------------------------------------------------------

    def _save_tokens(self):
        payload = {
            "access_token":  self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at":    self.expires_at.isoformat(),
            "updated_at":    datetime.now(timezone.utc).isoformat(),
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=TOKEN_S3_KEY,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
        )
        log.info(f"Tokens salvos em s3://{self.bucket}/{TOKEN_S3_KEY}")

    # ------------------------------------------------------------------
    # Método público principal
    # ------------------------------------------------------------------

    def authenticate(self):
        """
        Garante um access_token válido antes de qualquer chamada.

        - Se o token do S3 ainda for válido → usa direto
        - Se estiver expirado               → renova e salva no S3

        Chame sempre no início de cada script/DAG.
        """
        self._load_tokens()

        if not self._is_token_valid():
            self._refresh_tokens()

    def headers(self) -> dict:
        """Header Authorization pronto para uso em requests."""
        if not self.access_token:
            raise RuntimeError("Chame authenticate() antes de usar headers().")
        return {"Authorization": f"Bearer {self.access_token}"}
