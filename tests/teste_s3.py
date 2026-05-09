import boto3
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client("s3")

bucket = "tcc-uspesalq"
prefix = "bronze/mercado_livre/orders/"

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

arquivos = response.get("Contents", [])

if not arquivos:
    print("Nenhum arquivo encontrado nesse caminho.")
else:
    print(f"{len(arquivos)} arquivo(s) encontrado(s):\n")
    for obj in arquivos:
        print(f"  {obj['Key']}  ({obj['Size']} bytes)")