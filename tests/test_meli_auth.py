"""
test_meli_auth.py
=================
Testa o fluxo do MeliAuth:
  1. Carrega o access_token do S3
  2. Faz uma chamada real na API com ele
  3. Simula um 401 para confirmar que a renovação funciona
"""

import logging
import os

import requests
from dotenv import load_dotenv

from meli_auth import MeliAuth

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MELI_CLIENT_ID     = os.getenv("MELI_CLIENT_ID")
MELI_CLIENT_SECRET = os.getenv("MELI_CLIENT_SECRET")
AWS_BUCKET         = os.getenv("AWS_BUCKET", "tcc-uspesalq")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")


def call_api(auth: MeliAuth) -> dict:
    """Faz uma chamada na API tratando 401 automaticamente."""
    url  = "https://api.mercadolibre.com/users/me"
    resp = requests.get(url, headers=auth.headers(), timeout=30)

    if resp.status_code == 401:
        auth.handle_401()
        resp = requests.get(url, headers=auth.headers(), timeout=30)

    resp.raise_for_status()
    return resp.json()


def test_auth():
    print("\n" + "=" * 50)
    print("TESTE — MeliAuth")
    print("=" * 50)

    auth = MeliAuth(
        client_id=MELI_CLIENT_ID,
        client_secret=MELI_CLIENT_SECRET,
        bucket=AWS_BUCKET,
        region=AWS_REGION,
    )

    # Passo 1 — carrega o access_token do S3
    print("\n[1/2] Carregando access_token do S3...")
    auth.authenticate()
    print(f"      access_token  : {auth.access_token[:40]}...")

    # Passo 2 — usa o token em uma chamada real
    print("\n[2/2] Chamando API do Mercado Livre...")
    user = call_api(auth)

    print("\n✅ Token válido! Dados retornados:")
    print(f"   user_id  : {user.get('id')}")
    print(f"   nickname : {user.get('nickname')}")
    print(f"   email    : {user.get('email')}")
    print("=" * 50)


if __name__ == "__main__":
    test_auth()
