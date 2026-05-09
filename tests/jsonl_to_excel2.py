import json
import pandas as pd

USER_ID = 128571198

# lê JSONL
records = []
with open("shipments_20260502.jsonl", encoding="utf-8") as f:
    for line in f:
        records.append(json.loads(line))

rows = []

for rec in records:
    shipment_id = rec.get("shipment_id")
    ingested_at = rec.get("_ingested_at")

    senders = rec.get("senders", [])

    # pega o sender correto
    sender_match = next(
        (s for s in senders if s.get("user_id") == USER_ID),
        None
    )

    cost = sender_match.get("cost") if sender_match else None

    save = sender_match.get("save") if sender_match else None


    rows.append({
        "shipment_id": shipment_id,
        "_ingested_at": ingested_at,
        "cost": cost,
        "save": save
    })

df = pd.DataFrame(rows)

df.to_excel("shipments_tratado.xlsx", index=False)