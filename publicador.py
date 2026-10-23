import boto3, json, time, uuid, random
from botocore.exceptions import ClientError

REGION = "us-east-1"
TOPIC = "churn/eventos"
TOTAL = 10_000
PROB_MALO = 0.02          # ~2 % de eventos defectuosos
TIPOS = ["login", "view", "purchase", "support"]

iot = boto3.client("iot-data", region_name=REGION)

contadores = {"buenos": 0, "sin_customer_id": 0, "value_texto": 0}
fallidos = 0
t_inicio = time.time()

for i in range(TOTAL):
    ev = {
        "event_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1, 5000):06d}",
        "event_type": random.choice(TIPOS),
        "value": round(random.uniform(1, 100), 2),
        "sent_ts": int(time.time() * 1000),
    }

    # Eventos defectuosos a propósito
    if random.random() < PROB_MALO:
        if random.random() < 0.5:
            ev.pop("customer_id")
            tipo = "sin_customer_id"
        else:
            ev["value"] = "abc"
            tipo = "value_texto"
    else:
        tipo = "buenos"

    # Publicar con reintentos
    for intento in range(5):
        try:
            iot.publish(topic=TOPIC, qos=1, payload=json.dumps(ev))
            contadores[tipo] += 1
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                time.sleep(0.5 * (intento + 1))
            else:
                raise
    else:
        fallidos += 1

    if (i + 1) % 500 == 0:
        print(f"{i + 1} procesados...", flush=True)

publicados = sum(contadores.values())
print("\n===== RESUMEN =====")
print("Publicados correctamente:", publicados)
print("  buenos:", contadores["buenos"])
print("  sin customer_id:", contadores["sin_customer_id"])
print("  value en texto:", contadores["value_texto"])
print("Fallidos al publicar:", fallidos)
print(f"Tiempo total: {time.time() - t_inicio:.1f} s")