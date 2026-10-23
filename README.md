# Trabajo 1: Capas Medallion y Streaming en AWS

**Profesor:** Edwin Montoya
**Integrante:** Lorena Goez
**Video sustentación:**: 
 
---
 
## Contenido
 
1. [Parte A: Arquitectura Medallion con Athena](#parte-a-arquitectura-medallion-con-athena)
2. [Parte B: Streaming con IoT Core, SQS, Lambda y DynamoDB](#parte-b-streaming-iot-core--sqs--lambda--dynamodb)
3. [Limitaciones y errores conocidos](#limitaciones-y-errores-conocidos)
4. [Respuestas a las preguntas finales](#respuestas-a-las-preguntas-finales)
5. [Cumplimiento de las condiciones](#cumplimiento-de-las-condiciones)
Todos los recursos se crearon **manualmente en la consola de AWS** (región `us-east-1`), sin infraestructura como código. En el video se muestra la consola de CloudFormation sin stacks.
 
---
 
## Parte A: Arquitectura Medallion con Athena
 
### 1. Descripción del conjunto de datos
 
- **Qué es:** *Customer Behavior & Churn Prediction Dataset V2*, un dataset relacional **sintético** para predicción de churn, analítica y práctica de SQL.
- **De dónde proviene:** Kaggle.
- **Tamaño total del dataset:** 9 archivos CSV, 95 columnas, 265.81 MB.
- **Tabla usada:** `orders.csv`, con **1.000.000 de filas**, [Y] MB y 17 columnas. No fue necesario aumentar los datos.
- **Tipos de datos de `orders`:**
  - Identificadores: `order_id`, `customer_id`
  - Fecha: `order_date`
  - Texto/categorías: `product_category`, `product_name`, `payment_method`, `order_status`, `return_reason`
  - Numéricos: `quantity`, `unit_price`, `discount_percent`, `order_amount`, `delivery_days`, `seller_rating`, `customer_rating`
  - Banderas Yes/No: `coupon_used`, `returned`
### 2. Bronce
 
- CSV cargado en S3: `s3://medallion-lorena-2026/bronze/orders/`
- Tabla externa en Athena `medallion.bronze_orders`, con **las 17 columnas como `string`** (para conservar el dato crudo y poder medir sus defectos), usando `OpenCSVSerde`.
- Filas en bronce: **1.000.000**.
### 3. Hallazgos de calidad
 
Tabla `medallion.hallazgos`, construida con SQL sobre bronce:
 
| Hallazgo | Cantidad |
|---|---:|
| total_filas | 1.000.000 |
| returned_sin_motivo | **66.792** |
| order_id_duplicado | 0 |
| customer_id_vacio | 0 |
| fecha_invalida | 0 |
| fecha_futura | 0 |
| order_amount_invalido | 0 |
| order_amount_negativo | 0 |
| quantity_invalida_o_menor_igual_0 | 0 |
| unit_price_invalido_o_negativo | 0 |
| discount_fuera_de_0_100 | 0 |
| delivery_days_negativo | 0 |
| seller_rating_fuera_1_5 | 0 |
| customer_rating_fuera_1_5 | 0 |
 
El dataset es sintético y viene casi limpio. El único defecto detectado son **66.792 pedidos marcados como devueltos (`returned = Yes`) sin motivo de devolución**, cerca del 20 % de las devoluciones (333.008 pedidos devueltos en total).
 
### 4. Plata
 
Una vista (`medallion.v_clasificada`) aplica **6 reglas de calidad** en un `CASE`. Cada fila recibe **un único motivo de rechazo** (el primero que falle), por lo que los conteos cuadran por construcción.
 
| # | Regla | Motivo |
|---|---|---|
| 1 | `order_id` no repetido | `duplicado` |
| 2 | `customer_id` no vacío | `customer_vacio` |
| 3 | `order_date` es fecha válida y no futura | `fecha_invalida` |
| 4 | `order_amount` numérico y mayor o igual a 0 | `monto_invalido` |
| 5 | `quantity` entera y mayor que 0 | `cantidad_invalida` |
| 6 | `discount_percent` numérico entre 0 y 100 | `descuento_invalido` |
 
Tablas Iceberg:
 
- `medallion.silver_orders_validas`: filas válidas con los tipos de dato correctos.
- `medallion.silver_orders_rechazadas`: filas rechazadas con el texto original y su motivo.
**Conteos:**
 
| Bronce | Válidas | Rechazadas | Diferencia |
|---:|---:|---:|---:|
| 1.000.000 | 1.000.000 | 0 | 0 |
 
Se cumple `bronce = válidas + rechazadas`. Ver [Limitaciones](#limitaciones-y-errores-conocidos) sobre por qué las rechazadas son 0.
 
### 5. Oro
 
Tabla Iceberg `medallion.gold_categoria_mensual` (445 filas), agregada por mes y categoría de producto.
 
**Pregunta de negocio:** ¿qué categoría y qué mes generan más ingreso, y con qué tasa de devolución y calificación del cliente? Sirve para identificar categorías que venden mucho pero se devuelven mucho, lo que es un riesgo de churn.
 
Columnas: `mes`, `product_category`, `pedidos`, `ingreso_total`, `ticket_promedio`, `tasa_devolucion`, `calificacion_promedio`.
 
### 6. Comparación entre capas
 
Misma consulta (ingreso total por categoría de producto) en las tres capas, con "Volver a utilizar resultados" desactivado:
 
| Capa | Formato | Filas leídas | Bytes escaneados | Tiempo |
|---|---|---:|---:|---:|
| Bronce | CSV | 1.000.000 | 23.83 MB | 1.5 s |
| Plata | Iceberg / Parquet | 1.000.000 | 12.41 MB | 1.2 s |
| Oro | Iceberg / Parquet (agregado) | 445 | 6.02 KB | 807 ms |
 
**Interpretación:**
- Bronce → plata (≈ 48 % menos bytes): el CSV es un formato por filas, sin compresión ni tipos, y obliga a leer el archivo completo. Parquet es columnar y comprimido, y solo lee las dos columnas necesarias.
- Plata → oro (≈ 2.000 veces menos bytes): oro ya está pre-agregado, así que la consulta lee 445 filas en lugar de un millón.
- El tiempo mejora menos que los bytes porque, con un volumen tan pequeño, domina el tiempo fijo de arranque de Athena (cola, planeación). Con más datos la diferencia de tiempo crecería.
- Como Athena cobra por bytes escaneados, oro es órdenes de magnitud más barato de consultar.
---
 
## Parte B: Streaming (IoT Core → SQS → Lambda → DynamoDB)
 
### Flujo
 
```
Script Python ──► IoT Core (tema churn/eventos) ──► Regla regla_a_sqs ──► SQS cola-eventos
                                                                              │
                                                                              ▼
                                         DynamoDB eventos  ◄──  Lambda procesa_eventos  ──►  DynamoDB eventos_descartados
```
 
### Esquema de los eventos
 
```json
{
  "event_id": "7f9c2d1e-3b4a-4c8e-9d2f-1a2b3c4d5e6f",
  "customer_id": "C000123",
  "event_type": "login",
  "value": 12.5,
  "sent_ts": 1760000000123
}
```
 
| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | string (UUID) | Identificador único del evento |
| `customer_id` | string | Cliente que genera el evento |
| `event_type` | string | `login`, `view`, `purchase`, `support` |
| `value` | número | Valor asociado al evento |
| `sent_ts` | entero | Instante de envío en milisegundos desde epoch |
 
Datos sintéticos generados por el script publicador.
 
### Recursos creados
 
| Recurso | Configuración |
|---|---|
| Regla de IoT Core `regla_a_sqs` | `SELECT *, timestamp() AS iot_ts FROM 'churn/eventos'`, acción: enviar a SQS |
| Cola SQS `cola-eventos` | Estándar, visibilidad 60 s |
| Lambda `procesa_eventos` | Python, timeout 10 s, disparada por la cola con lote de 10 |
| DynamoDB `eventos` | Clave de partición `customer_id`, clave de ordenación `event_id`, capacidad bajo demanda |
| DynamoDB `eventos_descartados` | Clave de partición `event_id`, capacidad bajo demanda |
 
### Llave de DynamoDB y justificación
 
- **Partición `customer_id`:** agrupa los eventos por cliente (la consulta natural es "los eventos de este cliente") y reparte la carga entre muchas particiones.
- **Ordenación `event_id`:** es único en cada evento, así dos eventos del mismo cliente nunca se sobrescriben y los conteos cuadran.
### Adaptación de la función Lambda al esquema
 
- Valida que el mensaje sea JSON y que lleguen los 5 campos obligatorios.
- Valida que `value` sea numérico (no texto ni booleano).
- Convierte los decimales a `Decimal`, porque DynamoDB no acepta `float`.
- Agrega `arrival_ts` (hora de llegada) y `delay_ms` (retraso) a cada evento guardado.
- Usa `ConditionExpression="attribute_not_exists(event_id)"` para detectar entregas repetidas de SQS.
- Los eventos que no cumplen se guardan en `eventos_descartados` con su motivo.
Código: [`streaming/lambda_function.py`](streaming/lambda_function.py)
 
### Publicación de eventos
 
El script [`streaming/publicador.py`](streaming/publicador.py) publicó **10.000 eventos** con la API `iot-data` de IoT Core (`publish`). Se enviaron a propósito eventos defectuosos (~2 %):
 
| Tipo | Cantidad |
|---|---:|
| Buenos | 9.807 |
| Sin `customer_id` | 90 |
| `value` en texto | 103 |
| **Total publicados** | **10.000** |
| Fallidos al publicar | 0 |
 
### Conteos y retraso
 
Medidos con [`streaming/contar.py`](streaming/contar.py):
 
| Concepto | Cantidad |
|---|---:|
| Guardados en `eventos` | 9.807 |
| Descartados por `campos_faltantes` | 90 |
| Descartados por `value_no_numerico` | 103 |
| Descartados por `duplicado` | 1 |
 
**Cuadre:** 9.807 guardados + 193 descartados por esquema (90 + 103) = **10.000 publicados**.
 
La tabla de descartados tiene 194 filas porque SQS Standard entrega los mensajes *al menos una vez*: un evento llegó dos veces, la primera copia se guardó y la segunda se registró como `duplicado`. Ese evento se guardó una sola vez.
 
**Retraso (envío → DynamoDB):**
 
| Métrica | Valor |
|---|---:|
| Promedio | 149 ms |
| Máximo | 1.691 ms |
| Mínimo | 114 ms |
 
El retraso es aproximado: compara el reloj del computador que publica (`sent_ts`) con el de Lambda (`arrival_ts`).
 
---
 
## Limitaciones y errores conocidos
 
1. **La tabla de rechazadas está vacía.** Las 6 reglas de calidad no rechazan ninguna fila porque el dataset es sintético y casi todos los hallazgos dieron 0. Además, el único defecto real (66.792 devueltos sin motivo) **no se incluyó como regla** en la vista.
   *Solución prevista:* agregar a la vista la regla `devuelto_sin_motivo` (`returned = 'Yes'` y `return_reason` vacío) y recrear las tablas de plata y oro. Quedarían 933.208 filas válidas y 66.792 rechazadas.
2. **Entrega duplicada en SQS.** Un mensaje se procesó dos veces. La función lo detecta y lo registra como `duplicado`; por eso la tabla de descartados tiene 194 filas y no 193.
3. **Retraso aproximado.** Mezcla el reloj local con el de AWS.
4. **Volumen pequeño en la comparación.** Con ~24 MB, el tiempo fijo de Athena enmascara parte de la diferencia real entre capas.
---
 
## Respuestas a las preguntas finales
 
### Si los datos fueran mil veces mayores, ¿qué cambiaría?
 
- **Particionar** plata y oro por mes (en Iceberg, particionamiento oculto con `month(order_date)`) para escanear menos datos.
- **Compactar archivos pequeños** con `OPTIMIZE` y `VACUUM` de Iceberg.
- **Guardar bronce en Parquet** (o ORC) comprimido en lugar de CSV.
- **Mover las transformaciones a Glue/Spark** con cargas incrementales (`MERGE INTO`), en vez de recrear todo con CTAS en Athena.
- Orquestar el pipeline y monitorear la calidad; evaluar Redshift si hay muchas consultas concurrentes.
### ¿A partir de qué volumen dejarían esta forma de streaming y cuál usarían?
 
Esta arquitectura funciona bien con cientos a pocos miles de eventos por segundo. La dejaríamos al llegar a **decenas de miles de eventos por segundo sostenidos**, o antes si se necesita orden estricto, reprocesamiento o varios consumidores. SQS con Lambda procesa pocos mensajes por invocación, no garantiza el orden, entrega al menos una vez (como se vio con el duplicado) y no permite releer mensajes ya consumidos.
 
En su lugar usaríamos **Amazon Kinesis Data Streams** (o Kafka/MSK): mayor rendimiento por costo, orden por llave de partición, retención con reproducción de eventos y múltiples consumidores sobre el mismo flujo, con **Firehose** para volcar los datos a S3.
 
---
 
## Cumplimiento de las condiciones
 
- [x] Recursos creados manualmente en la consola de AWS (sin IaC).
- [x] CloudFormation sin stacks, mostrado en el video.
- [x] Parte A con 1.000.000 de filas (sin aumento de datos).
- [x] Parte B con 10.000 eventos publicados mediante la API de IoT Core.
- [x] Video en YouTube (no listado) de máximo 10 minutos.
- [x] Sin credenciales de AWS ni datos sensibles en el repositorio.
 
