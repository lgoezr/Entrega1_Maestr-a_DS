import boto3
from collections import Counter

ddb = boto3.resource("dynamodb", region_name="us-east-1")

def leer(nombre):
    t = ddb.Table(nombre)
    items, kw = [], {}
    while True:
        r = t.scan(**kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return items

guardados = leer("eventos")
descartados = leer("eventos_descartados")

print("===== CONTEO =====")
print("Guardados en eventos:", len(guardados))
print("Descartados:", len(descartados))
print("Suma:", len(guardados) + len(descartados))

print("\n===== MOTIVOS DE DESCARTE =====")
motivos = Counter(
    d["motivo"].split(":")[0] for d in descartados
)
for m, c in motivos.items():
    print(f"  {m}: {c}")

delays = [int(i["delay_ms"]) for i in guardados]
print("\n===== RETRASO (envío -> DynamoDB) =====")
print(f"Promedio: {sum(delays)/len(delays):.0f} ms")
print(f"Máximo:   {max(delays)} ms")
print(f"Mínimo:   {min(delays)} ms")