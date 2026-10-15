# Benchmark de volumen (Silver y Gold)

Mide cuánto tardan `silver_job.py` y `gold_job.py` con millones de filas, y comprueba que Gold sigue dando las cifras correctas a esa escala.

## Qué mide y qué no

| Mide | No mide |
|---|---|
| El tiempo de **Silver** (abrir el JSON de Debezium, quedarse con el último evento de cada fila, convertir tipos) | Kafka ni Debezium: aquí los eventos se escriben directo en Bronze |
| El tiempo de **Gold** (joins, agregaciones, variación contra el mes anterior) | MySQL ni Railway: no se les manda ningún dato |
| Que Gold **cuadre** con un cálculo independiente en Python puro | La carga a Cloud Storage y BigQuery, ni Airflow |

Los jobs son **los mismos que usa el lago**, sin modificar: solo se les cambian las carpetas con las variables de entorno `BRONZE_PATH`, `SILVER_PATH` y `GOLD_PATH`. Todo se escribe en `data/benchmark/`; el lago real (`data/bronze`, `data/silver`, `data/gold`) no se toca.

## Cómo funciona

1. **`generar_bronze.py`** crea eventos con la forma de los de Debezium (`op`, `before`, `after`) y las columnas de `bronze_job.py`. Cada fila de `egresos` e `ingresos` nace con un evento de creación y algunas sufren después una actualización, un borrado lógico (`eliminado_en`) o un `DELETE`, para que Silver tenga que resolver el estado vigente de verdad.
2. **`silver_job.py` y `gold_job.py`** se corren tal cual y se mide cada uno.
3. **`verificar.py`** calcula, sin Spark, qué debe tener Gold y lo compara. Los datos no son aleatorios: cada valor sale de una fórmula sobre el id de la fila (ver `parametros.py`), así que el resultado esperado se puede calcular por separado. Se probó que detecta errores: con parámetros equivocados dice `NO CUADRA`.

`egresos` tiene el número de filas que pidas e `ingresos` una quinta parte. Hay 1,000 usuarios y 21 meses de datos. Se generan además `usuarios` y los catálogos (`destinos`, `tipos_egreso`...).

## Cómo correrlo

Con el contenedor `mw-spark` arriba (`docker ps` debe mostrarlo) y desde la raíz del repo:

```
python spark\benchmark\correr_benchmark.py --filas 100000
python spark\benchmark\correr_benchmark.py
```

La primera es una prueba rápida. La segunda corre los tamaños por defecto (1 y 5 millones de filas). Opciones:

| Opción | Para qué |
|---|---|
| `--filas 1000000 5000000 10000000` | Los tamaños a probar, en filas de `egresos` |
| `--driver-memory 6g` | Memoria de Spark (por defecto `4g`). Súbela si ves un error de memoria |
| `--sin-verificar` | Salta la comprobación de Gold (la verificación en Python puro tarda más con 10 millones) |
| `--conservar` | No borra `data/benchmark/<filas>` al terminar |

Al terminar imprime la tabla de resultados y guarda `data/benchmark/resultados.json`. Necesita `pyarrow` (viene en `requirements-dev.txt`). Espacio en disco: Bronze pesa unos 65 MB por millón de filas de `egresos`, y se borra al terminar cada tamaño.

## Cómo leer los resultados

- **Los tiempos incluyen el arranque de Spark** (decenas de segundos fijos por cada `spark-submit`) y los `count()` que los jobs hacen al terminar de escribir. Con pocas filas ese costo fijo domina; por eso se comparan tamaños grandes entre sí.
- **Eventos/s** es `eventos en Bronze / (Silver + Gold)`. Solo es comparable entre tamaños grandes.
- Es **un solo nodo**, dentro de tu contenedor de Docker, con los CPUs y la RAM que muestra la salida. En un clúster real los números serían otros.
- Los datos son sintéticos y de distribución uniforme: los datos reales tienen sesgos que pueden cambiar los tiempos.

## Resultados

Medidos el 2026-10-14 en un contenedor de Docker Desktop (Windows, WSL2) con **12 CPUs y 11.6 GB de RAM**, un solo nodo, Spark con `--driver-memory 4g`. Gold cuadró contra el cálculo independiente en todos los tamaños.

| Egresos | Eventos en Bronze | Silver | Gold | Total | Eventos/s | Bronze (MB) | Gold cuadra |
|---:|---:|---:|---:|---:|---:|---:|:---:|
| 1,000,000 | 1,405,029 | 76 s | 35 s | 111 s | 12,656 | 64 | sí |
| 5,000,000 | 7,021,029 | 104 s | 43 s | 148 s | 47,611 | 319 | sí |

Con 5 millones de filas de `egresos` (más 1 millón de `ingresos`), Silver y Gold procesaron **7 millones de eventos en 148 segundos**.

- **Gran parte de cada corrida es costo fijo.** De 1 a 5 millones el volumen se multiplicó por 5 y el tiempo total solo por 1.3 (111 s a 148 s). Con 100,000 filas (141,429 eventos) el total fue de 101 s. Por eso "Eventos/s" crece con el tamaño: no es una velocidad constante.
- **4 GB de memoria bastan.** Se repitió con `--driver-memory 6g` y no mejoró (112 s y 157 s en total, dentro del ruido de una corrida a otra).
- Docker Desktop en Windows lee y escribe más lento en disco que Linux nativo, y eso está incluido en estos tiempos.

## Archivos

| Archivo | Qué es |
|---|---|
| `correr_benchmark.py` | Orquesta todo y mide |
| `generar_bronze.py` | Genera el Bronze sintético (corre dentro de Spark) |
| `verificar.py` | Comprueba Gold contra el cálculo independiente |
| `parametros.py` | Las fórmulas de los datos, compartidas por el generador y el verificador |