"""
meli_auth_setup.py
==================
Executa UMA VEZ para popular o S3 com os tokens iniciais.

Execute:
    python meli_auth_setup.py
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MELI_CLIENT_ID     = os.getenv("MELI_CLIENT_ID")
MELI_CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET")
AWS_BUCKET         = os.getenv("AWS_BUCKET", "tcc-uspesalq")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")
TOKEN_S3_KEY       = "config/mercado_livre/tokens.json"


def setup():
    print("\n" + "=" * 50)
    print("SETUP INICIAL — TOKENS MERCADO LIVRE")
    print("=" * 50)

    refresh_token = input("\nCole o refresh_token atual: ").strip()
    if not refresh_token:
        print("refresh_token não pode ser vazio.")
        return

    print("\nChamando API do Mercado Livre...")
    resp = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        headers={
            "accept":       "application/json",
            "content-type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type":    "refresh_token",
            "client_id":     MELI_CLIENT_ID,
            "client_secret": MELI_CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    access_token      = data.get("access_token")
    new_refresh_token = data.get("refresh_token")
    expires_in        = data.get("expires_in", 21600)
    expires_at        = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    print(f"\naccess_token  : {access_token[:40]}...")
    print(f"refresh_token : {new_refresh_token[:40]}...")
    print(f"expires_at    : {expires_at.isoformat()}")
    print(f"user_id       : {data.get('user_id')}")

    payload = {
        "access_token":  access_token,
        "refresh_token": new_refresh_token,
        "expires_at":    expires_at.isoformat(),
        "updated_at":    datetime.now(timezone.utc).isoformat(),
    }

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=TOKEN_S3_KEY,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json",
    )

    print(f"\n✅ Tokens salvos em s3://{AWS_BUCKET}/{TOKEN_S3_KEY}")
    print("Agora você pode rodar o pipeline normalmente.")
    print("=" * 50)


if __name__ == "__main__":
    setup()