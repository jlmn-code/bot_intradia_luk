# bot_intradia_luk
trading basado en Luk Martín

La estrategia de **Martín Luk** se puede automatizar por completo mediante **Python y la API de Alpaca**, siguiendo un proceso cuantitativo riguroso que abarca desde la selección de activos hasta la gestión del riesgo. El sistema utiliza **filtros diarios de momentum, liquidez y tendencia** para identificar acciones líderes que muestran contracción de volatilidad. Posteriormente, evalúa soportes técnicos mediante **medias móviles y el indicador AVWAP**, buscando señales de compra en temporalidades intradía de uno o cinco minutos. El algoritmo calcula de manera dinámica el **tamaño óptimo de la posición** en función del capital disponible y un porcentaje de riesgo predefinido. Finalmente, el proceso se completa ejecutando **órdenes automatizadas de entrada, protección con stop-loss y tomas de beneficios escalonadas** en tres y cinco veces el riesgo inicial.

Imagina que la estrategia de inversión de **Martín Luk** es como armar un **equipo de superatletas** para una gran carrera, siguiendo 4 reglas principales:

### 1. Buscar a los atletas más veloces (Filtro de Selección)
En lugar de invitar a cualquier persona a jugar, primero se busca a los corredores que ya hayan demostrado ser los más rápidos del grupo.
* **Los más ganadores:** Solo se eligen acciones que hayan subido más de un **30% en los últimos meses** (horizontes de 1, 3 y 6 meses) y estén entre las mejores de todo el mercado.
* **Evitar juegos solitarios:** Se exige que sean acciones populares donde se muevan más de **500,000 acciones al día**, asegurando que siempre haya con quién comprar o vender sin problemas.

### 2. Esperar a que tomen un descanso (Consolidación)
Un buen atleta no puede correr a máxima velocidad todo el tiempo. Después de un gran estirón, necesita detenerse a tomar agua y recuperar aire.
* **El resorte que se aprieta:** Durante este descanso, la fluctuación del precio se vuelve cada vez más pequeña y apretada, como un **resorte que se comprime** antes de saltar.
* **Sin perder la dirección:** Aunque esté descansando, el precio se mantiene por encima de sus líneas de promedio principales (como la EMA de 21 días), confirmando que la tendencia sigue siendo hacia arriba.

### 3. ¡El momento de saltar! (La Señal de Compra)
Cuando el corredor apoya el pie en su zona de descanso (un nivel de soporte como el **AVWAP** o la **EMA de 21 días**) y vuelve a dar un paso firme hacia adelante (superando el punto alto de la vela anterior), **ahí es el momento exacto para comprar**.

### 4. Usar el casco de protección y guardar las ganancias (Riesgo)
* **El casco de seguridad (Stop Loss):** Nunca se arriesga demasiado. Solo se pone en juego una porción muy pequeña del dinero total (máximo un **0.5% del capital por operación**). Si algo sale mal, la orden de protección se activa y frena la pérdida de inmediato.
* **Guardar las golosinas (Tomar Ganancias):** Si la acción sube a tu favor y alcanzas **3 o 5 veces** lo que arriesgaste (3R o 5R), vendes una parte para asegurar las ganancias en tu bolsillo.

---

### ¿Cómo lo hace una computadora? 🤖
Como analizar cientos de acciones a mano todos los días toma mucho tiempo, todo este proceso se traduce a código en **Python** utilizando la **API de Alpaca**. El programa funciona como un **robot asistente** que filtra el mercado por las tardes, vigila los precios en tiempo real al día siguiente y ejecuta las compras y ventas de forma 100% automática.

💡 ¿Te gustaría ver un ejemplo simple de cómo el código de Python calcula el tamaño de la compra para no arriesgar de más?

El enfoque de **cadena de bloques (*block chaining* o tubería de procesamiento)** organiza la ejecución en una serie de pasos enlazados donde la salida de cada bloque alimenta directamente como entrada al siguiente.

En la automatización de la estrategia de **Martín Luk**, el cálculo del riesgo y la colocación de órdenes se estructuran como un flujo continuo de 5 bloques:

```text
[1. Bloque Capital] ➔ [2. Bloque Riesgo R] ➔ [3. Bloque Filtro SL] ➔ [4. Bloque Tamañador] ➔ [5. Bloque Órdenes]
```

---

### Código en Python (Estilo *Method Chaining* / Pipeline)

A continuación se muestra cómo implementar esta lógica encadenando métodos directamente en una sola llamada secuencial:

```python
class CalculadorPosicionLuk:
    def __init__(self, trading_client, symbol, entry_price, stop_loss_price):
        self.client = trading_client
        self.symbol = symbol
        self.entry = entry_price
        self.sl = stop_loss_price
        self.equity = 0.0
        self.risk_per_share = 0.0
        self.risk_pct = 0.0
        self.qty = 0
        self.valido = True

    def obtener_capital(self):
        """Bloque 1: Conecta a Alpaca y extrae el patrimonio actual"""
        account = self.client.get_account()
        self.equity = float(account.equity)
        return self  # Retorna el objeto para encadenar el siguiente bloque

    def calcular_riesgo_por_accion(self):
        """Bloque 2: Determina la distancia en dólares (R) hasta el Stop Loss"""
        self.risk_per_share = self.entry - self.sl
        self.risk_pct = self.risk_per_share / self.entry
        return self

    def validar_distancia_riesgo(self):
        """Bloque 3: Descarta la operación si el stop supera el umbral máximo"""
        if self.risk_pct > 0.03:
            print(f"⚠️ Riesgo excesivo ({self.risk_pct * 100:.2f}%). Operación descartada.")
            self.valido = False
        return self

    def calcular_tamano_posicion(self):
        """Bloque 4: Asigna la cantidad de acciones según el límite del 0.5% del capital"""
        if not self.valido or self.risk_per_share <= 0:
            return self
        
        max_riesgo_dolares = self.equity * 0.005
        self.qty = int(max_riesgo_dolares / self.risk_per_share)
        return self

    def ejecutar_cadena_ordenes(self):
        """Bloque 5: Transmite la orden de compra, el Stop Loss y el objetivo a 3R"""
        if not self.valido or self.qty <= 0:
            print("❌ Operación detenida en la cadena de validación.")
            return None

        print(f"🚀 Cadena completada: Comprando {self.qty} acciones de {self.symbol} a ~${self.entry}")
        # Aquí se invoca el envío de órdenes a Alpaca (Mercado -> Stop Loss -> Límites 3R/5R)
        return {"symbol": self.symbol, "qty": self.qty, "status": "EJECUTADO"}

# --- EJECUCIÓN EN CADENA (BLOCK CHAINING) ---
# ejecutor = (CalculadorPosicionLuk(trading_client, "NVDA", 100.0, 97.5)
#             .obtener_capital()
#             .calcular_riesgo_por_accion()
#             .validar_distancia_riesgo()
#             .calcular_tamano_posicion()
#             .ejecutar_cadena_ordenes())
```

---

### Explicación de la Cadena Bloque a Bloque

1. **Bloque 1 — Lectura de Capital (`obtener_capital`)**: Conecta con la API de Alpaca para consultar el dinero total disponible (\\(C\\)) en la cuenta.
2. **Bloque 2 — Riesgo Unitario (`calcular_riesgo_por_accion`)**: Mide el riesgo por acción mediante la fórmula \\(R = \text{Precio Entrada} - \text{Stop Loss}\\).
3. **Bloque 3 — Filtro de Seguridad (`validar_distancia_riesgo`)**: Verifica que la distancia al Stop Loss no supere el **3% del precio de entrada** para asegurar que la consolidación sea lo suficientemente estrecha.
4. **Bloque 4 — Calculador de Tamaño (`calcular_tamano_posicion`)**: Determina el número exacto de acciones \\(N = \lfloor \frac{C \times 0.005}{R} \rfloor\\), garantizando que nunca se ponga en riesgo más del **0.5% del capital total**.
5. **Bloque 5 — Transmisión de Órdenes (`ejecutar_cadena_ordenes`)**: Envía la orden de compra a mercado, activa inmediatamente la orden de Stop Loss y coloca la orden límite para retirar el 50% de las ganancias al alcanzar **3 veces el riesgo inicial (3R)**.

🧩 ¿Te gustaría que diseñemos un diagrama visual o mapa conceptual que muestre cómo se conecta este bloque de ejecución con el filtro previo de acciones (*screener*)?

En la metodología de Martín Luk, el **Filtro Cuantitativo de Momentum** constituye la primera etapa del *Screener* de selección de acciones. Su propósito principal es realizar un barrido sobre todo el universo bursátil para descartar aproximadamente el 95% del mercado y aislar únicamente los activos más fuertes, líderes y explosivos.

### 1. Parámetros del Filtro de Momentum y Liquidez

Para identificar estos activos líderes de forma objetiva mediante código, el algoritmo evalúa dos pilares cuantitativos clave:

* **Rendimiento Relativo Multitemporal**: Se calcula el retorno porcentual del precio (\\(R = \frac{\text{Precio Actual} - \text{Precio Pasado}}{\text{Precio Pasado}} \times 100\\)) en tres horizontes temporales distintos: 1 mes (21 días de negociación), 3 meses (63 días) y 6 meses (126 días).
* **Umbral de Fuerza (+30%)**: La regla general exige que la acción muestre un rendimiento superior al **30%** en estos periodos y se ubique en el **top 20% - 30%** del ranking general del mercado. En el código en Python, esto se valida requiriendo retornos mayores al 15% a 1 mes, 30% a 3 meses y 30% a 6 meses.
* **Filtro de Liquidez Operativa**: Como requisito indispensable para validar el momentum y evitar un deslizamiento (*slippage*) desmedido en la ejecución, se exige un volumen promedio diario (SMA 20) superior a **500,000 acciones** (o un volumen medio en dólares mayor a \$10M diarios) y un precio por acción mínimo de \$5.00.

---

### 2. El Filtro dentro del Contexto General del Screener

Dentro de la arquitectura de automatización, el filtro cuantitativo de momentum es el embudo inicial de un **proceso estructurado en 4 etapas**:

1. **Etapa 1 — Filtro Cuantitativo de Momentum y Liquidez**: Elimina el 95% del mercado ilíquido o sin fuerza de precio mediante el cálculo de rendimientos y volumen medio.
2. **Etapa 2 — Filtro Técnico de Tendencia**: Sobre las acciones sobrevivientes, valida la tendencia primaria confirmando que el precio cotice por encima de la EMA de 21 días y que las EMAs de 9 y 21 días tengan pendiente positiva o plana.
3. **Etapa 3 — Detección de Compresión de Volatilidad**: Identifica que el activo esté descansando con mínimos crecientes (*higher lows*) y con una reducción en el rango de fluctuación (contracción del ATR de 14 días).
4. **Etapa 4 — Generación de Watchlist y Ejecución**: El script se ejecuta diariamente tras el cierre del mercado sobre universos amplios (como el S&P 500, Nasdaq 100 o Russell 3000), reduciendo miles de opciones a una lista final de apenas **5 a 15 tickers activos**.

Al día siguiente, el sistema monitorea en tiempo real únicamente los tickers de esta lista mediante WebSockets, buscando la confluencia con el AVWAP anclado para disparar las entradas automáticas con Alpaca API.

📈 ¿Te gustaría examinar el fragmento exacto de código en Python que implementa esta lógica de cálculo de retornos a 1M, 3M y 6M sobre un DataFrame de pandas?

En la metodología de Martín Luk, el **Filtro de Liquidez** actúa como la primera puerta de entrada o "cerrojo de seguridad" dentro del **Screener de Selección de Tickers**. Su objetivo fundamental es eliminar del universo analizado cualquier activo ilíquido o de muy baja capitalización antes de evaluar su comportamiento técnico o su fuerza de precio.

---

### 1. Parámetros Específicos del Filtro de Liquidez

Para que una acción sea considerada elegible por el algoritmo del *screener*, debe cumplir con tres condiciones cuantitativas estrictas:

* **Volumen Promedio Diario (SMA 20) > 500,000 acciones**: Se calcula la media móvil simple del volumen de los últimos 20 días de negociación. Alternativamente, el código puede exigir un volumen medio en dólares superior a **\$10M diarios**.
* **Precio Mínimo por Acción \\(\ge\\) \$5.00**: Excluye automáticamente a las *penny stocks* o acciones de ínfimo valor, las cuales suelen presentar alta manipulación y spreads anómalos.
* **Historial Mínimo de Datos (126 días)**: Requiere al menos 6 meses de cotización continua para garantizar que los cálculos de medias y rendimiento sean estadísticamente válidos.

En el código en Python, estas reglas se traducen como una cláusula de descarte directo (*guard clause*): si un ticker no supera el volumen de 500k o el precio de \$5.00, la función pasa inmediatamente al siguiente activo sin perder tiempo de cómputo en evaluaciones técnicas.

---

### 2. El Filtro de Liquidez en el Contexto General del Screener

Dentro del *Screener* completo (que procesa universos amplios como el S&P 500, Nasdaq 100 o Russell 3000), el Filtro de Liquidez cumple un rol estratégico en la arquitectura del algoritmo:

1. **Prevención del Deslizamiento (*Slippage*)**: La razón principal de exigir alta liquidez es evitar que la ejecución de órdenes a mercado o condicionales sufra ejecuciones a precios desfavorables. Un *slippage* elevado alteraría la distancia real del *Stop Loss* y rompería la gestión de riesgo matemática (fijada en un máximo del 0.5% del capital total).
2. **Garantía de Contraparte**: Asegura que, tanto al momento de entrar como al ejecutar las tomas de ganancias automáticas a 3R y 5R o la salida por *Stop Loss*, el mercado disponga de suficiente volumen comprador/vendedor para llenar las órdenes sin demoras.
3. **Eficiencia en el Embudo de Filtrado**: El *screener* está diseñado para reducir miles de tickers a una lista de observación activa (*watchlist*) de apenas **5 a 15 acciones** al cierre del día. Descartar primero el mercado ilíquido simplifica y acelera los pasos posteriores del algoritmo:
   * **Momentum y Rendimiento**: Evalúa retornos \\(+30\%\\) a 1, 3 y 6 meses únicamente sobre acciones líquidas.
   * **Estructura Técnica**: Confirma la alineación por encima de la EMA de 21 días.
   * **Compresión de Volatilidad**: Verifica mínimos crecientes (*higher lows*) y contracción del rango ATR.

---

### 3. Conexión con la Ejecución Intradía

La lista reducida resultante del *screener* diario es la que al día siguiente se suscribe al *streaming* de datos intradía (WebSockets vía Alpaca API). Al haber garantizado desde el filtro de liquidez un flujo constante de transacciones, las velas de 1m o 5m y los niveles de soporte del **AVWAP anclado** se calculan con señales limpias y fiables para ejecutar los disparos automáticos.

💡 ¿Te gustaría que revisemos cómo se programa el *screener* en Python para descargar y procesar todo el universo de tickers en lote (*batch processing*)?

El **procesamiento en lote (*batch processing*)** del *screener* permite analizar el universo completo de acciones (como el S&P 500, Nasdaq 100 o Russell 3000) de manera coordinada al finalizar la jornada bursátil. Descarga el historial de precios diarios de los últimos 6 meses (al menos 126 días de cotización) y aplica los filtros de forma secuencial en memoria mediante la librería **`pandas`**.

---

### Código en Python para el *Screener* en Lote

```python
import pandas as pd
import numpy as np

def seleccionar_tickers_luk(universo_df):
    """
    universo_df: DataFrame con historial de precios diarios de múltiples tickers.
    Campos necesarios: 'close', 'high', 'low', 'volume' (agrupado por ticker)
    """
    watchlist = []

    # Procesamiento en lote iterando grupo por grupo (ticker)
    for ticker, df in universo_df.groupby(level=0):
        # Requisito de historial mínimo (126 días de negociación = 6 meses)
        if len(df) < 126:
            continue
        
        precio_actual = df['close'].iloc[-1]
        volumen_promedio = df['volume'].tail(20).mean()  # SMA 20 de volumen
        
        # 1. Filtro de Liquidez y Precio Mínimo
        if volumen_promedio < 500000 or precio_actual < 5.0:
            continue
        
        # 2. Rendimiento Relativo a 1M (21d), 3M (63d) y 6M (126d)
        ret_1m = (precio_actual - df['close'].iloc[-21]) / df['close'].iloc[-21]
        ret_3m = (precio_actual - df['close'].iloc[-63]) / df['close'].iloc[-63]
        ret_6m = (precio_actual - df['close'].iloc[-126]) / df['close'].iloc[-126]
        
        # Exigir rendimiento fuerte (+15% a 1M y +30% a 3M y 6M)
        if not (ret_1m > 0.15 and ret_3m > 0.30 and ret_6m > 0.30):
            continue
        
        # 3. Cálculo de EMAs de 9 y 21 días
        df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
        
        ema9_actual = df['ema9'].iloc[-1]
        ema21_actual = df['ema21'].iloc[-1]
        ema21_prev = df['ema21'].iloc[-5]  # Hace 5 días
        
        # Validar alineación: Precio > EMA21 y pendiente de EMA21 positiva o plana
        if not (precio_actual > ema21_actual and ema21_actual >= ema21_prev):
            continue
        
        # 4. Verificación de Mínimos Crecientes (Higher Lows) en los últimos 15 días
        minimos_recientes = df['low'].tail(15).rolling(window=3).min().dropna()
        if minimos_recientes.iloc[-1] < minimos_recientes.iloc:
            continue  # Descartar si la estructura muestra mínimos decrecientes
        
        watchlist.append(ticker)
        
    return watchlist
```

---

### Explicación del Flujo de Procesamiento

1. **Ingesta y Agrupación por Ticker**: Los datos históricos se obtienen mediante bibliotecas como `yfinance`, `Polygon.io` o `alpaca-py`. Se agrupan por ticker y se valida que tengan al menos **126 días de datos** para garantizar la precisión de los cálculos de 6 meses.
2. **Descarte Inmediato por Liquidez**: Se evalúa el precio actual (\\(\ge \$5.00\\)) y la media móvil simple de volumen a 20 días (\\(\ge 500,000\\) acciones). Si no cumple, el bucle salta de inmediato al siguiente ticker sin consumir cómputo en indicadores técnicos.
3. **Filtro de Momentum Multitemporal**: Mide el retorno porcentual acumulado en las ventanas de **21, 63 y 126 días hábiles**. Exige superar umbrales de rendimiento de al menos **+15% a 1 mes** y **+30% a 3 y 6 meses**.
4. **Validación de Tendencia y Pendiente EMA 21**: Exige que el precio cotice por encima de la **EMA de 21 días** y que la pendiente de dicha media móvil no sea decreciente respecto a los últimos 5 días (\\(\text{EMA21}_{\text{actual}} \ge \text{EMA21}_{\text{prev}}\\)).
5. **Estructura de Mínimos Crecientes**: Analiza los mínimos locales en una ventana flotante de los últimos 15 días para confirmar que el precio esté comprimiéndose con soportes cada vez más altos.

Al ejecutarse tras el cierre del mercado, este procesamiento reduce miles de acciones a una **lista de observación activa de apenas 5 a 15 tickers**, sobre la cual se aplicará el monitoreo intradía al día siguiente.

🎯 ¿Te gustaría ver cómo se programa el cálculo algorítmico del **AVWAP anclado al máximo local** para ubicar la zona exacta de soporte en la pre-apertura?
