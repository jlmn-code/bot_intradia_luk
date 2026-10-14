#!/usr/bin/env python3
"""
bot_intradia_luk.py  -  Estrategia de Martín Luk automatizada sobre Alpaca (paper)

Cuatro modos (variable MODO o primer argumento):
  screener : tras el cierre. Filtra el mercado (liquidez -> momentum -> tendencia ->
             compresión de volatilidad) y guarda datos/watchlist_luk.json (5-15 tickers).
  monitor  : durante la apertura. Para cada ticker de la watchlist calcula AVWAP anclado
             al máximo local + EMA21 diaria como soporte y vigila velas de 5 min. Si toca el
             soporte y la vela rompe el máximo de la anterior -> compra con bracket
             (stop + toma de beneficios 50 % en 3R y 50 % en 5R).
  cierre   : antes del cierre liquida las posiciones abiertas por el bot (salvo MANTENER_NOCHE).
  resumen  : imprime cuenta, posiciones y operaciones del día.

Seguridad: DRY_RUN=true por defecto (no envía órdenes). Solo cuenta paper.
Todo se configura con variables de entorno (ver bloque CONFIG).
"""
import json
import math
import os
import re
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (AssetClass, AssetStatus, OrderClass, OrderSide,
                                  QueryOrderStatus, TimeInForce)
from alpaca.trading.requests import (GetAssetsRequest, GetCalendarRequest, GetOrdersRequest,
                                     MarketOrderRequest, StopLossRequest, TakeProfitRequest)

NY = ZoneInfo("America/New_York")


# ----------------------------------------------------------------------------- CONFIG
def _f(n, d): return float(os.getenv(n, d))
def _i(n, d): return int(float(os.getenv(n, d)))
def _b(n, d): return os.getenv(n, str(d)).strip().lower() in ("1", "true", "yes", "si", "sí")
def _t(n, d):
    h, m = os.getenv(n, d).split(":")
    return dtime(int(h), int(m))


DRY_RUN = _b("DRY_RUN", True)
if os.getenv("DRY_RUN_MANUAL", "").strip().lower() in ("true", "false"):
    DRY_RUN = os.getenv("DRY_RUN_MANUAL").strip().lower() == "true"
PAPER = _b("PAPER", True)
PROGRAMADO = _b("PROGRAMADO", False)            # True = lanzado por cron (se exigen franjas horarias)

# Screener (valores del documento de Luk)
PRECIO_MIN = _f("PRECIO_MIN", 5.0)
VOL_MIN = _f("VOL_MIN", 500_000)                 # SMA20 de volumen
MIN_HISTORIAL = _i("MIN_HISTORIAL", 127)         # 126 sesiones + 1
RET_1M_MIN = _f("RET_1M_MIN", 0.15)
RET_3M_MIN = _f("RET_3M_MIN", 0.30)
RET_6M_MIN = _f("RET_6M_MIN", 0.30)
RANK_TOP = _f("RANK_TOP", 0.30)                  # top 30 % de retorno a 3M entre acciones líquidas
ATR_CONTRACCION = _f("ATR_CONTRACCION", 0.95)    # ATR% actual <= 0.95 * ATR% de hace 10 sesiones
MAX_WATCHLIST = _i("MAX_WATCHLIST", 15)
MAX_EXT_EMA21 = _f("MAX_EXT_EMA21", 0.08)        # precio <= EMA21 * 1.08 (no comprar valores muy extendidos)
ANCLA_MIN_SESIONES = _i("ANCLA_MIN_SESIONES", 5) # el máximo ancla debe tener >= 5 sesiones (hay consolidación)
MAX_CORR = _f("MAX_CORR", 0.80)                  # correlación (60 sesiones) para considerar "mismo tema"
MAX_POR_GRUPO = _i("MAX_POR_GRUPO", 2)           # máx. valores correlacionados en la watchlist
EXCLUIR_ETF = _b("EXCLUIR_ETF", True)            # fuera ETFs/ETNs y apalancados (por nombre)
ANCLA_DIAS = _i("ANCLA_DIAS", 20)                # el AVWAP se ancla al máximo de las últimas N sesiones
FEED_DIARIO = DataFeed(os.getenv("FEED_DIARIO", "sip").lower())
FEED_INTRA = DataFeed(os.getenv("FEED_INTRA", "iex").lower())

# Señal intradía (velas de 5 min)
TOL_TOQUE = _f("TOL_TOQUE", 0.003)               # "toca" soporte si el mínimo llega a nivel*(1+0.3 %)
LOOKBACK_VELAS = _i("LOOKBACK_VELAS", 3)         # velas previas donde buscar el toque
BUFFER_STOP = _f("BUFFER_STOP", 0.001)           # stop = mínimo del toque - 0.1 %
MAX_EXTENSION = _f("MAX_EXTENSION", 0.02)        # no comprar si precio > nivel + 2 %
ENTRADA_DESDE = _t("ENTRADA_DESDE", "09:40")
ENTRADA_HASTA = _t("ENTRADA_HASTA", "11:00")
CIERRE_DESDE = _t("CIERRE_DESDE", "15:25")
CIERRE_HASTA = _t("CIERRE_HASTA", "15:59")
POLL_SEG = _i("POLL_SEG", 30)

# Riesgo
RIESGO_PCT = _f("RIESGO_PCT", 0.005)             # 0.5 % del capital por operación
MAX_RIESGO_DIST = _f("MAX_RIESGO_DIST", 0.03)    # stop a más del 3 % -> descartar
MIN_RIESGO_DIST = _f("MIN_RIESGO_DIST", 0.002)   # stop a menos del 0.2 % -> ruido, descartar
MAX_POSICION_PCT = _f("MAX_POSICION_PCT", 0.25)  # máx. 25 % del capital en una acción
MAX_POSICIONES = _i("MAX_POSICIONES", 5)
R_TP1 = _f("R_TP1", 3.0)
R_TP2 = _f("R_TP2", 5.0)
MANTENER_NOCHE = _b("MANTENER_NOCHE", False)     # False = intradía puro (orden DAY + cierre)

DATOS = Path(os.getenv("DATOS_DIR", "datos"))
WATCHLIST_F = DATOS / "watchlist_luk.json"
ESTADO_F = DATOS / "estado_luk.json"
OPS_F = DATOS / "operaciones_luk.csv"


# ----------------------------------------------------------------------------- utilidades
def ahora_ny() -> datetime:
    return datetime.now(NY)


def log(*a):
    print(f"[{ahora_ny():%Y-%m-%d %H:%M:%S} NY]", *a, flush=True)


def resumen_md(texto: str):
    f = os.getenv("GITHUB_STEP_SUMMARY")
    if f:
        with open(f, "a", encoding="utf-8") as fh:
            fh.write(texto + "\n")


def reintentar(fn, n=3, espera=2):
    for i in range(n):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            s = str(e).lower()
            if i == n - 1 or "401" in s or "403" in s or "unauthorized" in s:
                raise
            log(f"⚠️ reintento {i + 1}/{n - 1}: {e}")
            time.sleep(espera * (i + 1))


def conectar():
    key, sec = os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY")
    if not key or not sec:
        sys.exit("❌ Faltan APCA_API_KEY_ID / APCA_API_SECRET_KEY (secretos del repo).")
    if not PAPER:
        sys.exit("❌ PAPER=false: este bot solo está preparado para cuenta paper.")
    trading = TradingClient(key, sec, paper=True)
    data = StockHistoricalDataClient(key, sec)
    try:
        cuenta = trading.get_account()
    except Exception as e:  # noqa: BLE001
        if "401" in str(e) or "unauthorized" in str(e).lower():
            sys.exit("❌ 401 Unauthorized: Alpaca rechaza las credenciales. "
                     "Renueva los secretos del repo (API_KEY_LUK / SECRET_KEY_LUK).")
        raise
    log(f"Conectado (paper). Equity={float(cuenta.equity):,.2f}  DRY_RUN={DRY_RUN}")
    return trading, data


def es_dia_de_mercado(trading, dia: date) -> bool:
    cal = reintentar(lambda: trading.get_calendar(GetCalendarRequest(start=dia, end=dia)))
    return len(cal) > 0


def barras(data, symbols, tf, start, end, feed, intra=False, chunk=200):
    """Devuelve {symbol: DataFrame(open, high, low, close, volume)}.
    Diarias: índice = fecha (naive). Intradía: índice = datetime con zona NY."""
    out = {}
    symbols = list(symbols)
    for i in range(0, len(symbols), chunk):
        lote = symbols[i:i + chunk]
        req = StockBarsRequest(symbol_or_symbols=lote, timeframe=tf, start=start, end=end,
                               feed=feed, adjustment=Adjustment.SPLIT)
        df = reintentar(lambda: data.get_stock_bars(req).df)
        if df is None or len(df) == 0:
            continue
        for s, g in df.groupby(level=0):
            g = g.droplevel(0)[["open", "high", "low", "close", "volume"]].copy()
            idx = pd.DatetimeIndex(g.index).tz_convert(NY)
            g.index = idx if intra else idx.tz_localize(None).normalize()
            out[s] = g.sort_index()
        if len(symbols) > chunk:
            time.sleep(0.4)
    return out


# ----------------------------------------------------------------------------- estado / ficheros
def cargar_estado(hoy: date) -> dict:
    if ESTADO_F.exists():
        try:
            e = json.loads(ESTADO_F.read_text())
            if e.get("fecha") == hoy.isoformat():
                return e
        except Exception:  # noqa: BLE001
            pass
    return {"fecha": hoy.isoformat(), "entradas": {}, "monitor_done": False}


def guardar_estado(e: dict):
    DATOS.mkdir(parents=True, exist_ok=True)
    ESTADO_F.write_text(json.dumps(e, indent=2, ensure_ascii=False))


def anotar_operacion(fila: dict):
    DATOS.mkdir(parents=True, exist_ok=True)
    nuevo = not OPS_F.exists()
    pd.DataFrame([fila]).to_csv(OPS_F, mode="a", header=nuevo, index=False)


# ============================================================================ 1) SCREENER
def universo(trading) -> list:
    f = DATOS / "universo_luk.txt"          # opcional: lista propia (un ticker por línea)
    if f.exists():
        s = [x.strip().upper() for x in f.read_text().splitlines()
             if x.strip() and not x.startswith("#")]
        log(f"Universo desde {f}: {len(s)} tickers")
        return s
    activos = reintentar(lambda: trading.get_all_assets(
        GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)))
    bolsas = {"NASDAQ", "NYSE", "AMEX", "ARCA"}
    etf = re.compile(r"\b(ETF|ETN|2X|3X|DAILY|LEVERAGED|INVERSE|FUND)\b", re.I)
    s = []
    for a in activos:
        ex = getattr(a.exchange, "value", a.exchange)
        if EXCLUIR_ETF and etf.search(a.name or ""):
            continue
        if a.tradable and str(ex) in bolsas and a.symbol.isalpha() and len(a.symbol) <= 5:
            s.append(a.symbol)
    log(f"Universo desde Alpaca: {len(s)} tickers")
    return sorted(s)


def metricas(df: pd.DataFrame):
    """Filtro 1 (liquidez) + cálculo de métricas. None si no pasa liquidez/historial."""
    if len(df) < MIN_HISTORIAL:
        return None
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    precio = float(c.iloc[-1])
    vol20 = float(v.tail(20).mean())
    if precio < PRECIO_MIN or vol20 < VOL_MIN:
        return None
    ret = lambda n: precio / float(c.iloc[-1 - n]) - 1          # noqa: E731
    ema9 = c.ewm(span=9, adjust=False).mean()
    ema21 = c.ewm(span=21, adjust=False).mean()
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    # mínimos crecientes: 3 bloques de 5 sesiones, cada uno con mínimo >= anterior (tol. 1 %)
    m3, m2, m1 = float(l.iloc[-5:].min()), float(l.iloc[-10:-5].min()), float(l.iloc[-15:-10].min())
    ventana = df.iloc[-ANCLA_DIAS:]
    return {
        "precio": precio, "vol20": vol20,
        "ret_1m": ret(21), "ret_3m": ret(63), "ret_6m": ret(126),
        "ema9": float(ema9.iloc[-1]), "ema9_prev": float(ema9.iloc[-4]),
        "ema21": float(ema21.iloc[-1]), "ema21_prev": float(ema21.iloc[-6]),
        "atr14": float(atr.iloc[-1]),
        "atr_pct": float(atr.iloc[-1] / c.iloc[-1]),
        "atr_pct_prev": float(atr.iloc[-11] / c.iloc[-11]),
        "higher_lows": bool((m3 >= m2 * 0.99) and (m2 >= m1 * 0.99)),
        "ancla_fecha": ventana["high"].idxmax().date().isoformat(),
        "ancla_sesiones": int(len(ventana) - 1 - ventana.index.get_loc(ventana["high"].idxmax())),
        "dist_ema21": float(precio / ema21.iloc[-1] - 1),
        "ancla_max": float(ventana["high"].max()),
        "ultima_fecha": df.index[-1].date().isoformat(),
    }


def filtrar_watchlist(met: pd.DataFrame) -> pd.DataFrame:
    """Filtros 2-4 sobre el DataFrame de métricas (index = ticker)."""
    if met.empty:
        return met
    met = met.copy()
    met["rank_3m"] = met["ret_3m"].rank(pct=True)               # entre acciones líquidas
    ok = (
        (met.ret_1m > RET_1M_MIN) & (met.ret_3m > RET_3M_MIN) & (met.ret_6m > RET_6M_MIN)
        & (met.rank_3m >= 1 - RANK_TOP)                                 # momentum + ranking
        & (met.precio > met.ema21) & (met.precio <= met.ema21 * (1 + MAX_EXT_EMA21))
        & (met.ema21 >= met.ema21_prev * 0.999)
        & (met.ema9 >= met.ema9_prev * 0.995)                           # tendencia
        & met.higher_lows.astype(bool)
        & (met.atr_pct <= met.atr_pct_prev * ATR_CONTRACCION)           # compresión de volatilidad
        & (met.ancla_sesiones >= ANCLA_MIN_SESIONES)                    # máximo previo => consolidación
    )
    return met[ok].sort_values("ret_3m", ascending=False)


def diversificar(cand: pd.DataFrame, diarias: dict) -> pd.DataFrame:
    """Recorre los candidatos por ret_3m y descarta los que se mueven igual (corr > MAX_CORR)
    que MAX_POR_GRUPO valores ya elegidos (mismo tema: refinerías, PLTR y sus ETFs...)."""
    if cand.empty:
        return cand
    rets = pd.DataFrame({s: diarias[s]["close"].pct_change().tail(60) for s in cand.index})
    corr = rets.corr()
    elegidos = []
    for s in cand.index:
        similares = [e for e in elegidos if corr.loc[s, e] > MAX_CORR]
        if len(similares) >= MAX_POR_GRUPO:
            log(f"↪️ {s} descartado por tema repetido (corr>{MAX_CORR} con {similares})")
            continue
        elegidos.append(s)
        if len(elegidos) >= MAX_WATCHLIST:
            break
    return cand.loc[elegidos]


def modo_screener(trading, data):
    ahora = ahora_ny()
    hoy = ahora.date()
    if PROGRAMADO and (not es_dia_de_mercado(trading, hoy) or ahora.time() < dtime(16, 20)):
        return log("Screener programado fuera de franja (antes de 16:20 NY o día sin mercado). Salgo.")
    sesion_hoy_cerrada = ahora.time() >= dtime(16, 20)
    syms = universo(trading)
    end = datetime.now(NY) - timedelta(minutes=16)          # SIP gratis: datos con 15 min de retraso
    start = datetime.now(NY) - timedelta(days=210)
    log(f"Descargando velas diarias ({FEED_DIARIO.value}) de {len(syms)} tickers…")
    diarias = barras(data, syms, TimeFrame.Day, start, end, FEED_DIARIO)
    filas = {}
    for s, df in diarias.items():
        if not sesion_hoy_cerrada:                           # no usar una vela de hoy incompleta
            df = df[df.index.date < hoy]
        m = metricas(df) if len(df) else None
        if m:
            filas[s] = m
    met = pd.DataFrame.from_dict(filas, orient="index")
    log(f"Pasan liquidez: {len(met)} de {len(diarias)} con datos")
    wl = diversificar(filtrar_watchlist(met), diarias)
    items = []
    for s, r in wl.iterrows():
        d = {"symbol": s}
        for k, v in r.items():
            d[k] = bool(v) if isinstance(v, (np.bool_, bool)) else \
                   float(v) if isinstance(v, (np.floating, float)) else v
        items.append(d)
    DATOS.mkdir(parents=True, exist_ok=True)
    WATCHLIST_F.write_text(json.dumps({
        "generada": ahora.isoformat(timespec="seconds"),
        "ultima_sesion": max((m["ultima_fecha"] for m in filas.values()), default=None),
        "items": items}, indent=2, ensure_ascii=False))
    log(f"✅ Watchlist ({len(items)}): {[i['symbol'] for i in items]}")
    tabla = "\n".join(f"| {i['symbol']} | {i['ret_1m']:.0%} | {i['ret_3m']:.0%} | {i['ret_6m']:.0%} | "
                      f"{i['dist_ema21']:.1%} | {i['ancla_fecha']} |" for i in items)
    resumen_md("### Watchlist Luk\n| Ticker | 1M | 3M | 6M | Sobre EMA21 | Ancla AVWAP |\n"
               "|---|---|---|---|---|---|\n" + tabla)


# ============================================================================ 2) MONITOR
def cargar_watchlist(trading, data):
    if WATCHLIST_F.exists():
        wl = json.loads(WATCHLIST_F.read_text())
        edad = (ahora_ny() - datetime.fromisoformat(wl["generada"])).days
        if edad <= 4:                      # una lista vacía reciente también vale: hoy no hay setups
            return wl["items"]
        log(f"Watchlist con {edad} días.")
    log("Regenero la watchlist antes de operar…")
    modo_screener(trading, data)
    if WATCHLIST_F.exists():
        return json.loads(WATCHLIST_F.read_text())["items"]
    return []


def precalcular_base(data, item, hoy: date):
    """AVWAP anclado: acumulados (precio típico*volumen, volumen) desde la sesión ancla hasta ayer."""
    ancla = date.fromisoformat(item["ancla_fecha"])
    ini = datetime.combine(ancla, dtime(9, 30), tzinfo=NY)
    fin = datetime.combine(hoy, dtime(9, 30), tzinfo=NY)
    tf5 = TimeFrame(5, TimeFrameUnit.Minute)
    d = barras(data, [item["symbol"]], tf5, ini, fin, FEED_INTRA, intra=True).get(item["symbol"])
    if d is None or d.empty:
        return {"pv0": 0.0, "v0": 0.0}
    d = d.between_time("09:30", "15:59")
    d = d[d.index.date < hoy]
    tp = (d.high + d.low + d.close) / 3
    return {"pv0": float((tp * d.volume).sum()), "v0": float(d.volume.sum())}


def evaluar_senal(item, base, hoy_df, ahora):
    """Última vela 5m completada: ¿tocó AVWAP/EMA21 y rompe el máximo de la vela anterior?"""
    comp = hoy_df[hoy_df.index + pd.Timedelta(minutes=5) <= ahora]
    if len(comp) < 2:
        return None
    tp = (comp.high + comp.low + comp.close) / 3
    vv = base["v0"] + comp.volume.cumsum()
    avwap = ((base["pv0"] + (tp * comp.volume).cumsum()) / vv).where(vv > 0)
    a = 2 / 22
    ema21 = item["ema21"] * (1 - a) + float(comp.close.iloc[-1]) * a
    last, prev = comp.iloc[-1], comp.iloc[-2]
    if not (last.close > prev.high and last.close > last.open):      # vela de ruptura alcista
        return None
    if last.close < ema21:                                           # tendencia intacta
        return None
    vent = comp.iloc[-(LOOKBACK_VELAS + 1):]
    for nombre, serie in (("AVWAP", avwap.loc[vent.index]),
                          ("EMA21", pd.Series(ema21, index=vent.index))):
        if serie.isna().all():
            continue
        if (vent.low <= serie * (1 + TOL_TOQUE)).any():
            nivel = float(serie.iloc[-1])
            if last.close <= nivel or (last.close - nivel) / nivel > MAX_EXTENSION:
                continue
            return {"nivel_nombre": nombre, "nivel": nivel, "ref": float(last.close),
                    "stop": float(vent.low.min()) * (1 - BUFFER_STOP),
                    "barra": comp.index[-1].strftime("%H:%M")}
    return None


class PosicionLuk:
    """Cadena de bloques: capital -> riesgo R -> filtro SL -> tamaño -> órdenes."""

    def __init__(self, trading, symbol, entrada, stop):
        self.t, self.symbol, self.entry, self.sl = trading, symbol, entrada, stop
        self.equity = self.bp = self.r = 0.0
        self.qty = 0
        self.motivo = None
        self.legs = []

    def obtener_capital(self):
        c = reintentar(self.t.get_account)
        self.equity, self.bp = float(c.equity), float(c.buying_power)
        return self

    def calcular_riesgo(self):
        self.r = self.entry - self.sl
        return self

    def validar_riesgo(self):
        if self.r <= 0:
            self.motivo = "stop por encima de la entrada"
            return self
        dist = self.r / self.entry
        if dist > MAX_RIESGO_DIST:
            self.motivo = f"stop demasiado lejos ({dist:.2%} > {MAX_RIESGO_DIST:.0%})"
        elif dist < MIN_RIESGO_DIST:
            self.motivo = f"stop demasiado cerca ({dist:.2%})"
        return self

    def calcular_tamano(self):
        if self.motivo:
            return self
        por_riesgo = math.floor(self.equity * RIESGO_PCT / self.r)
        tope = math.floor(min(self.equity * MAX_POSICION_PCT, self.bp * 0.95) / self.entry)
        self.qty = max(0, min(por_riesgo, tope))
        if self.qty < 1:
            self.motivo = "tamaño 0 (capital/riesgo insuficiente)"
        return self

    def construir_ordenes(self):
        """50 % de las acciones con TP a 3R y el resto con TP a 5R (dos brackets con el mismo stop)."""
        if self.motivo:
            return self
        q1 = max(1, self.qty // 2)
        q2 = self.qty - q1
        self.legs = [(q1, self.entry + R_TP1 * self.r)]
        if q2 > 0:
            self.legs.append((q2, self.entry + R_TP2 * self.r))
        return self

    def ejecutar(self):
        if self.motivo:
            log(f"❌ {self.symbol}: descartada — {self.motivo}")
            return None
        ids = []
        tif = TimeInForce.GTC if MANTENER_NOCHE else TimeInForce.DAY
        for q, tp in self.legs:
            desc = (f"{self.symbol} BUY {q} @~{self.entry:.2f} SL {self.sl:.2f} TP {tp:.2f} "
                    f"(riesgo {self.r * q:.0f}$)")
            if DRY_RUN:
                log(f"🧪 SIMULADO: {desc}")
                ids.append("simulado")
                continue
            req = MarketOrderRequest(
                symbol=self.symbol, qty=q, side=OrderSide.BUY, time_in_force=tif,
                order_class=OrderClass.BRACKET,
                take_profit=TakeProfitRequest(limit_price=round(tp, 2)),
                stop_loss=StopLossRequest(stop_price=round(self.sl, 2)))
            o = reintentar(lambda: self.t.submit_order(req))
            log(f"🚀 ENVIADA: {desc}  id={o.id}")
            ids.append(str(o.id))
        return ids


def simbolos_ocupados(trading) -> set:
    pos = {p.symbol for p in reintentar(trading.get_all_positions)}
    ords = reintentar(lambda: trading.get_orders(
        GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500)))
    return pos | {o.symbol for o in ords}


def modo_monitor(trading, data):
    ahora = ahora_ny()
    hoy = ahora.date()
    if PROGRAMADO:
        if not es_dia_de_mercado(trading, hoy):
            return log("Hoy no hay sesión. Salgo.")
        if ahora.time() < dtime(9, 30) or ahora.time() >= ENTRADA_HASTA:
            return log(f"Monitor programado fuera de franja ({ahora:%H:%M} NY). Salgo.")
    estado = cargar_estado(hoy)
    if PROGRAMADO and estado.get("monitor_done"):
        return log("El monitor de hoy ya terminó. Salgo.")
    items = cargar_watchlist(trading, data)
    if not items:
        return log("Watchlist vacía: ningún ticker cumple los filtros. Nada que vigilar.")
    log(f"Vigilando {len(items)}: {[i['symbol'] for i in items]}")
    bases = {}
    for it in items:
        try:
            bases[it["symbol"]] = precalcular_base(data, it, hoy)
        except Exception as e:  # noqa: BLE001
            log(f"⚠️ {it['symbol']}: sin base AVWAP ({e}); solo EMA21")
            bases[it["symbol"]] = {"pv0": 0.0, "v0": 0.0}
    ultima_barra = {}
    tf5 = TimeFrame(5, TimeFrameUnit.Minute)
    # lanzado a mano fuera de la franja: una sola pasada, sin órdenes reales
    manual_fuera = (not PROGRAMADO) and not (ENTRADA_DESDE <= ahora.time() < ENTRADA_HASTA)
    while True:
        ahora = ahora_ny()
        if ahora.time() >= ENTRADA_HASTA and not manual_fuera:
            break
        if ahora.time() >= ENTRADA_DESDE or manual_fuera:
            pendientes = [i for i in items if i["symbol"] not in estado["entradas"]]
            if not pendientes or len(estado["entradas"]) >= MAX_POSICIONES:
                log("Sin tickers pendientes o máximo de posiciones alcanzado.")
                break
            ini = datetime.combine(hoy, dtime(9, 30), tzinfo=NY)
            try:
                hoy_bars = barras(data, [i["symbol"] for i in pendientes], tf5, ini, ahora,
                                  FEED_INTRA, intra=True)
            except Exception as e:  # noqa: BLE001
                log(f"⚠️ error descargando velas: {e}")
                hoy_bars = {}
            for it in pendientes:
                s = it["symbol"]
                df = hoy_bars.get(s)
                if df is None or df.empty:
                    continue
                df = df.between_time("09:30", "15:59")
                comp = df[df.index + pd.Timedelta(minutes=5) <= ahora]
                if len(comp) < 2 or ultima_barra.get(s) == comp.index[-1]:
                    continue
                ultima_barra[s] = comp.index[-1]       # solo se evalúa la última vela completada, una vez
                sen = evaluar_senal(it, bases[s], df, ahora)
                if not sen:
                    continue
                log(f"📈 {s}: señal en vela {sen['barra']} (toque {sen['nivel_nombre']} "
                    f"{sen['nivel']:.2f}, ref {sen['ref']:.2f}, stop {sen['stop']:.2f})")
                if s in simbolos_ocupados(trading):
                    log(f"↪️ {s}: ya hay posición u orden abierta, omito")
                    continue
                pos = (PosicionLuk(trading, s, sen["ref"], sen["stop"])
                       .obtener_capital().calcular_riesgo().validar_riesgo()
                       .calcular_tamano().construir_ordenes())
                if manual_fuera and not DRY_RUN:
                    pos.motivo = pos.motivo or "ejecución manual fuera de franja: sin órdenes reales"
                ids = pos.ejecutar()
                if ids:
                    estado["entradas"][s] = {"hora": ahora.strftime("%H:%M:%S"), "qty": pos.qty,
                                             "entrada": sen["ref"], "stop": round(pos.sl, 2),
                                             "simulada": DRY_RUN, "ids": ids}
                    guardar_estado(estado)
                    anotar_operacion({"fecha": hoy.isoformat(), "hora": ahora.strftime("%H:%M:%S"),
                                      "symbol": s, "qty": pos.qty, "entrada": round(sen["ref"], 2),
                                      "stop": round(pos.sl, 2), "r": round(pos.r, 4),
                                      "nivel": sen["nivel_nombre"], "simulada": DRY_RUN})
                    resumen_md(f"- {'🧪' if DRY_RUN else '🚀'} {s}: {pos.qty} acc. "
                               f"@ {sen['ref']:.2f}, SL {pos.sl:.2f}")
        if manual_fuera:
            break
        time.sleep(POLL_SEG)
    if PROGRAMADO:
        estado["monitor_done"] = True
    guardar_estado(estado)
    log(f"Monitor terminado. Entradas de hoy: {list(estado['entradas'])}")


# ============================================================================ 3) CIERRE
def modo_cierre(trading, data):
    ahora = ahora_ny()
    hoy = ahora.date()
    if MANTENER_NOCHE:
        return log("MANTENER_NOCHE=true: no se liquida nada.")
    if PROGRAMADO and not (CIERRE_DESDE <= ahora.time() <= CIERRE_HASTA
                           and es_dia_de_mercado(trading, hoy)):
        return log(f"Cierre programado fuera de franja ({ahora:%H:%M} NY). Salgo.")
    estado = cargar_estado(hoy)
    mias = [s for s, v in estado["entradas"].items() if not v.get("simulada")]
    if not mias:
        return log("No hay entradas reales de hoy que liquidar.")
    posiciones = {p.symbol for p in reintentar(trading.get_all_positions)}
    for s in mias:
        abiertas = reintentar(lambda: trading.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[s], limit=100)))
        if DRY_RUN:
            log(f"🧪 SIMULADO: cancelar {len(abiertas)} órdenes y cerrar {s}")
            continue
        for o in abiertas:
            try:
                trading.cancel_order_by_id(o.id)
            except Exception as e:  # noqa: BLE001
                log(f"⚠️ no se pudo cancelar {o.id}: {e}")
        time.sleep(1.5)
        if s in posiciones:
            try:
                trading.close_position(s)
                log(f"🔚 {s}: posición liquidada")
            except Exception as e:  # noqa: BLE001
                log(f"⚠️ {s}: no se pudo cerrar ({e})")
        else:
            log(f"{s}: sin posición (ya cerrada por stop/TP)")


# ============================================================================ 4) RESUMEN
def modo_resumen(trading, data):
    c = reintentar(trading.get_account)
    log(f"Equity {float(c.equity):,.2f} | efectivo {float(c.cash):,.2f} | BP {float(c.buying_power):,.2f}")
    for p in reintentar(trading.get_all_positions):
        log(f"  {p.symbol}: {p.qty} acc. entrada {float(p.avg_entry_price):.2f} "
            f"P&L {float(p.unrealized_pl):+.2f} ({float(p.unrealized_plpc):+.2%})")
    e = cargar_estado(ahora_ny().date())
    log("Entradas de hoy:", json.dumps(e["entradas"], ensure_ascii=False))
    resumen_md(f"### Resumen\nEquity **{float(c.equity):,.2f}** · entradas hoy: {list(e['entradas'])}")


MODOS = {"screener": modo_screener, "monitor": modo_monitor,
         "cierre": modo_cierre, "resumen": modo_resumen}


def main():
    modo = (sys.argv[1] if len(sys.argv) > 1 else os.getenv("MODO", "")).strip().lower()
    if modo not in MODOS:
        sys.exit(f"Modo inválido '{modo}'. Usa: {', '.join(MODOS)}")
    log(f"=== bot_intradia_luk · modo={modo} · programado={PROGRAMADO} · DRY_RUN={DRY_RUN} ===")
    trading, data = conectar()
    MODOS[modo](trading, data)


if __name__ == "__main__":
    main()
