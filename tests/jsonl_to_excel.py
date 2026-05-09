


import json
import pandas as pd

with open("orders_20260428_20260429.jsonl", encoding="utf-8") as f:
    orders = [json.loads(line) for line in f]

# mantém somente payments aprovados
for order in orders:
    pagamentos_aprovados = [
        p for p in order.get("payments", [])
        if p.get("status") == "approved"
    ]

    # pega o primeiro aprovado
    order["payment"] = pagamentos_aprovados[0] if pagamentos_aprovados else None

# normaliza order
df = pd.json_normalize(orders)

# explode order_items
df = df.explode("order_items").reset_index(drop=True)

# normaliza order_items
items_df = pd.json_normalize(df["order_items"])
items_df.columns = [f"order_item_{c}" for c in items_df.columns]

# normaliza payment aprovado
payments_df = pd.json_normalize(df["payment"])
payments_df.columns = [f"payment_{c}" for c in payments_df.columns]

# junta tudo
df = pd.concat(
    [
        df.drop(columns=["order_items", "payment", "payments"]),
        items_df,
        payments_df
    ],
    axis=1
)

df.to_excel("testee.xlsx", index=False)



