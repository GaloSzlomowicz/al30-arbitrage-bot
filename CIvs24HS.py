# -*- coding: utf-8 -*-
# =============================================================================
# OMS - Motor de Desarbitraje AL30 CI vs 24hs (Primary API / Matba Rofex)
# =============================================================================
# Todo en ARS: especie AL30. Solo 2 tickers: AL30 - CI, AL30 - 24hs (Order Book L2).
#
# LÓGICA: Comprar CI y vender 24hs si la TNA implícita es mayor a la caución. Fin.
# - TNA implícita = (px_bid_24hs / px_offer_ci - 1) × (365 / días); días = 1 (CI → 24hs).
# - Condición: TNA_implícita > caución_tomadora → comprar CI, vender 24hs (2 órdenes FOK/IOC).
# - Caución 1 día EN PESOS: Primary (CAAP1D) o config. Ejecución: FOK/IOC.
# =============================================================================

import os
import sys
import logging
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple, Any
from dotenv import load_dotenv
import pyRofex

# =============================================================================
# LOGGING Y .ENV (mismo patrón que bot_ggal_futures_primary)
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('bot_canje_mep.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()
downloads_dir = os.path.join(os.path.expanduser("~"), "Downloads")
env_paths = [
    os.path.join(downloads_dir, '.env'),
    os.path.join(downloads_dir, '.envGGAL'),
    os.path.join(script_dir, '.env'),
    os.path.join(os.path.dirname(script_dir), '.env'),
    os.path.join(os.getcwd(), '.env'),
]
env_loaded = False
for env_path in env_paths:
    if os.path.exists(env_path):
        env_loaded = load_dotenv(env_path, override=True)
        if env_loaded:
            print(f"[OK] .env cargado desde: {env_path}")
            break
if not env_loaded:
    load_dotenv()

# =============================================================================
# COMISIONES ECO VALORES (tarifario Bonos/Letras/ONs)
# =============================================================================
# Fuente: ecovalores.com.ar/tarifario_detallado - Bonos, Letras y ONs
# - Sin bonificación intraday: 0.49% (sin promoción) / 0.19% (con promoción Club)
# - Derechos de mercado: 0.01%, IVA exento
# Se aplica por pata; round-trip = 2 patas. La cotización límite se ajusta para
# que solo se dispare cuando el tipo implícito deja margen después de comisiones.
# =============================================================================

COMISION_BONO_SIN_PROMO_PCT = 0.0049   # 0.49%
COMISION_BONO_CON_PROMO_PCT = 0.0019   # 0.19%
COMISION_DERECHOS_PCT = 0.0001         # 0.01%


def _byma_ticker(symbol: str) -> str:
    """Formato Primary BYMA: MERV - XMEV - SYMBOL."""
    if not symbol or not isinstance(symbol, str):
        return symbol or ""
    s = symbol.strip()
    if s.upper().startswith("MERV - XMEV - "):
        return s
    return f"MERV - XMEV - {s}"


def _short_label(ticker: str) -> str:
    """Etiqueta corta para log: 'MERV - XMEV - GD30 - CI' -> 'GD30 - CI'."""
    if not ticker:
        return ticker or ""
    s = ticker.strip()
    if s.upper().startswith("MERV - XMEV - "):
        return s[14:].strip()  # quitar "MERV - XMEV - "
    return s


# Market MERV (BYMA): instrumentos como AL30 CI/24hs cotizan en MERV. Primary puede requerir market=MERV para recibir BI/OF.
class _MarketMERV:
    """Objeto compatible con pyRofex: .value para market_data_subscription (marketId MERV)."""
    value = "MERV"


def _build_arbitrage_tickers(instrument: str, suffix_ci: str = " - CI", suffix_24: str = " - 24hs") -> Tuple[str, str]:
    """Construye los 2 tickers para arbitraje CI vs 24hs: solo especie en ARS (AL30 - CI, AL30 - 24hs). Sin D."""
    base = (instrument or "AL30").strip().upper()
    if base != "AL30":
        base = "AL30"
    if base.endswith("D"):
        base = base[:-1]
    return (
        _byma_ticker(base + suffix_ci),
        _byma_ticker(base + suffix_24),
    )


def _is_argentina_market_hours() -> bool:
    """Aproximación: Lun-Vie 10:00-18:00 Argentina (hora local del sistema)."""
    try:
        now = datetime.now()
        if now.weekday() >= 5:  # sábado=5, domingo=6
            return False
        return 10 <= now.hour < 18
    except Exception:
        return True


def _no_data_error_message(con_subscripcion_ok: bool = True) -> str:
    """Mensaje de error personalizado cuando no hay datos de mercado."""
    if _is_argentina_market_hours():
        return (
            "Sin datos de mercado en horario de operación. "
            "Posibles causas: suscripción incorrecta, ticker mal formado (usar formato MERV - XMEV - SYMBOL - CI), "
            "o la API no está devolviendo book para ese instrumento."
        )
    return (
        "Sin datos de mercado. El mercado BYMA suele estar cerrado (Lun-Vie 10:00-18:00 Argentina). "
        "Si está en horario de mercado, revisar suscripción y formato de tickers."
    )


def _parse_md_bid_offer(
    message: Dict,
    depth: int = 5,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], List[Tuple[float, float]], List[Tuple[float, float]]]:
    """
    Extrae bid, offer, bid_size, offer_size (top) y listas del book.
    Primary WebSocket: marketData.BI / marketData.OF (o marketDataResponse).
    Acepta también mensaje con datos en raíz o entries (listas con type BI/OF).
    """
    md = message.get('marketData', {}) or message.get('marketDataResponse', {}) or message.get('md', {}) or message
    # BI/OF pueden venir en marketData o en la raíz del mensaje (Primary)
    bids = md.get('BI') or md.get('bids') or message.get('BI') or message.get('bids') or []
    offers = md.get('OF') or md.get('offers') or message.get('OF') or message.get('offers') or []
    # Primary a veces envía entries: [ {type: 'BI', price, size}, ... ]
    if not bids and not offers and isinstance(md.get('entries'), list):
        for e in md.get('entries', []):
            if not isinstance(e, dict):
                continue
            t = (e.get('type') or e.get('entryType') or '').upper()
            px = e.get('price')
            sz = e.get('size', 0)
            if t in ('BI', 'BID') and px is not None:
                bids.append({'price': float(px), 'size': float(sz)})
            elif t in ('OF', 'OFFER', 'OFER') and px is not None:
                offers.append({'price': float(px), 'size': float(sz)})
    if not isinstance(bids, list):
        bids = []
    if not isinstance(offers, list):
        offers = []
    bid_px = float(bids[0]['price']) if len(bids) > 0 and isinstance(bids[0], dict) and bids[0].get('price') is not None else None
    offer_px = float(offers[0]['price']) if len(offers) > 0 and isinstance(offers[0], dict) and offers[0].get('price') is not None else None
    bid_sz = float(bids[0].get('size', 0)) if len(bids) > 0 and isinstance(bids[0], dict) else 0.0
    offer_sz = float(offers[0].get('size', 0)) if len(offers) > 0 and isinstance(offers[0], dict) else 0.0
    bids_list = []
    for i in range(min(depth, len(bids))):
        if isinstance(bids[i], dict) and bids[i].get('price') is not None:
            bids_list.append((float(bids[i]['price']), float(bids[i].get('size', 0))))
    offers_list = []
    for i in range(min(depth, len(offers))):
        if isinstance(offers[i], dict) and offers[i].get('price') is not None:
            offers_list.append((float(offers[i]['price']), float(offers[i].get('size', 0))))
    # Si no hay top pero sí listas, tomar de listas
    if bid_px is None and bids_list:
        bid_px, bid_sz = bids_list[0][0], bids_list[0][1]
    if offer_px is None and offers_list:
        offer_px, offer_sz = offers_list[0][0], offers_list[0][1]
    # Fallback: depth.bid / depth.offer (algunas APIs)
    if (bid_px is None or offer_px is None) and isinstance(md.get('depth'), dict):
        depth = md['depth']
        db = depth.get('bid') or depth.get('bids')
        do = depth.get('offer') or depth.get('offers')
        if isinstance(db, list) and len(db) > 0 and isinstance(db[0], dict) and db[0].get('price') is not None:
            bid_px = float(db[0]['price'])
        if isinstance(do, list) and len(do) > 0 and isinstance(do[0], dict) and do[0].get('price') is not None:
            offer_px = float(do[0]['price'])
    return bid_px, offer_px, bid_sz, offer_sz, bids_list, offers_list


# =============================================================================
# CLASE BOT CANJE MEP
# =============================================================================

class CanjeMEPPrimary:
    """
    Bot restringido a AL30 CI y AL30 24hs. Todo en ARS (especie en ARS).
    Desarbitraje entre plazos: busca discrepancias 24hs vs CI por valor tiempo vía CAUCIÓN 1 DÍA.
    Calcula el teórico en ARS del bono llevado a 24hs: Precio_AL30_24hs_teorico_ARS = Precio_AL30_CI_ARS × (1 + tasa×1/365).
    Pares: (AL30/AL30D - CI), (AL30/AL30D - 24hs). Condición: (TC_24hs - TC_CI) > fricción. FOK/IOC.
    """

    def __init__(self, config: Dict):
        self.config = config
        # Motor de arbitraje plazos: solo 2 tickers en ARS (AL30 - CI, AL30 - 24hs). Sin especie D.
        self.modo_arbitraje_plazos = config.get('modo_arbitraje_plazos', True)
        self.instrument = 'AL30'
        suffix_ci = config.get('suffix_ci', ' - CI')
        suffix_24 = config.get('suffix_24hs', ' - 24hs')
        self.ars_ci, self.ars_24hs = _build_arbitrage_tickers(self.instrument, suffix_ci, suffix_24)
        self.usd_ci, self.usd_24hs = None, None  # Solo ARS, no se suscribe a D
        # Pares: 2 patas en ARS (CI, 24hs); cada una es (ticker_ars, None)
        pairs_raw = config.get('pairs', [])
        if not pairs_raw and self.modo_arbitraje_plazos:
            pairs_raw = [(self.ars_ci, None), (self.ars_24hs, None)]
        elif not pairs_raw:
            pairs_raw = [(_byma_ticker('AL30 - CI'), None), (_byma_ticker('AL30 - 24hs'), None)]
        self.pairs = [(_byma_ticker(ars) if ars else None, usd and _byma_ticker(usd) or None) for ars, usd in pairs_raw]
        self.accion = config.get('accion', 'compra')  # compra | venta (arbitraje: compra MEP CI, vende MEP 24hs)
        self.cotizacion = float(config.get('cotizacion', 1300))
        self.efectivo = float(config.get('efectivo', 100000))
        self.stock = float(config.get('stock', 1000))
        self.nominales_maximo = int(config.get('nominales_maximo', 50))
        self.porcentaje_efectivo = float(config.get('porcentaje_efectivo', 0.9))
        self.porcentaje_stock = float(config.get('porcentaje_stock', 0.8))
        self.tiempo_espera = float(config.get('tiempo_espera', 2.0))

        # Comisiones Eco Valores (tarifario bonos)
        self.use_promocion = config.get('use_promocion_eco', False)
        comision_bono = COMISION_BONO_CON_PROMO_PCT if self.use_promocion else COMISION_BONO_SIN_PROMO_PCT
        self.comision_por_pata_pct = comision_bono + COMISION_DERECHOS_PCT
        self.comision_round_trip_pct = 2.0 * self.comision_por_pata_pct
        # Cotización límite: solo operar si el tipo implícito deja margen después de comisiones
        # Compra MEP: cotizacion_mercado <= cotizacion / (1 + comision_rt) para que tipo efectivo <= cotizacion
        self.cotizacion_limite_compra = self.cotizacion / (1.0 + self.comision_round_trip_pct)
        # Venta MEP: cotizacion_mercado >= cotizacion * (1 + comision_rt)
        self.cotizacion_limite_venta = self.cotizacion * (1.0 + self.comision_round_trip_pct)

        # Fricción financiera (arbitraje plazos): caución + comisiones + slippage
        # Tasa caución: tomadora (pedir prestado) / colocadora (prestar); 1 día entre CI y 24hs
        self.tasa_caucion_tomadora_pct_anual = float(config.get('tasa_caucion_tomadora_pct_anual', 0.0))  # ej. 50
        self.tasa_caucion_colocadora_pct_anual = float(config.get('tasa_caucion_colocadora_pct_anual', 0.0))
        self.dias_entre_plazos = float(config.get('dias_entre_plazos', 1.0))
        self.costo_caucion_pct_dia = (self.tasa_caucion_tomadora_pct_anual / 100.0 / 365.0) * self.dias_entre_plazos
        self.slippage_estimado_pct = float(config.get('slippage_estimado_pct', 0.05))  # 0.05% por pata
        # Fricción total (en %): Costo_Caución + Comisiones (2 patas) + Slippage
        self.friccion_arbitraje_pct = (
            self.costo_caucion_pct_dia * 100.0
            + 2.0 * self.comision_round_trip_pct * 100.0
            + 4.0 * self.slippage_estimado_pct
        )
        # Ejecución atómica: FOK o IOC para evitar pierna coja
        self.time_in_force_arbitrage = (config.get('time_in_force_arbitrage') or 'FOK').upper()
        if self.time_in_force_arbitrage not in ('FOK', 'IOC'):
            self.time_in_force_arbitrage = 'FOK'

        # Caución a 1 día desde Primary: monitoreo constante (tasa tomadora/colocadora)
        self.use_caucion_primary = config.get('use_caucion_primary', True)
        self.caucion_ticker_1d = config.get('caucion_ticker_1d')  # opcional; si no, se resuelve CAAP1D (caución en pesos 1d)
        self.caucion_refresh_seconds = float(config.get('caucion_refresh_seconds', 30.0))
        self._ticker_caucion_1d_resolved: Optional[str] = None
        self._last_caucion_fetch = 0.0
        self._cached_tasa_tomadora_pct_anual: Optional[float] = None
        self._cached_tasa_colocadora_pct_anual: Optional[float] = None

        # Por par: size_tick, price_size (USD), contract_multiplier
        self.pair_params = config.get('pair_params', {})
        self.default_size_tick = int(config.get('size_tick', 1))
        self.default_price_size_usd = float(config.get('price_size_usd', 100.0))
        self.default_contract_multiplier = int(config.get('contract_multiplier', 1))

        self.operado = 0.0
        self.stock_operado = 0.0
        self.ultimo_envio = None
        self.running = False

        # Filtros: profundidad y instantaneidad (no quedarse colgado)
        self.min_profundidad = int(config.get('min_profundidad', 5))   # mínimo size en book para operar
        self.max_profundidad = config.get('max_profundidad')           # opcional: cap nominales por liquidez
        self.max_data_age_seconds = float(config.get('max_data_age_seconds', 15.0))  # datos frescos
        self.max_spread_pct = config.get('max_spread_pct')             # opcional: no operar si spread % > este valor
        # Solo AL30: sin referencia a otros bonos (GD30, etc.)
        self.tickers_referencia_dolar = [
            _byma_ticker(s.strip()) for s in config.get('tickers_referencia_dolar', [])
            if isinstance(s, str) and s.strip()
        ]
        # Par referencia MEP para este bot: AL30/AL30D (opcional; desarbitraje plazos no lo usa por defecto)
        par_ref = config.get('par_referencia_mep')
        if isinstance(par_ref, (list, tuple)) and len(par_ref) >= 2:
            self.par_referencia_mep = (_byma_ticker(str(par_ref[0])), _byma_ticker(str(par_ref[1])))
        else:
            self.par_referencia_mep = (_byma_ticker('AL30 - CI'), _byma_ticker('AL30D - CI'))
        self.comparar_con_referencia_mep = config.get('comparar_con_referencia_mep', False)
        self.par_referencia_ccl = None
        self.comparar_con_referencia_ccl = False
        # Cada cuánto analiza (imprime resumen) y cada cuánto evalúa para ejecutar
        self.analisis_interval_seconds = float(config.get('analisis_interval_seconds', config.get('log_resumen_interval_seconds', 99.0)))  # ej. 99s resumen
        self.ejecucion_interval_seconds = float(config.get('ejecucion_interval_seconds', 1.0))  # ej. 1s evaluar y eventualmente enviar orden
        self.log_resumen_interval = self.analisis_interval_seconds  # usado por _log_resumen_dolar
        self._last_log_resumen = 0.0
        self._last_ejecucion_time = 0.0
        self._last_no_data_error_log = 0.0
        self.no_data_error_throttle_seconds = float(config.get('no_data_error_throttle_seconds', 60.0))  # no repetir error sin datos
        self.log_skip_resumen_only = config.get('log_skip_resumen_only', False)  # si True, no logear cada SKIP individual
        self.market_data_depth = int(config.get('market_data_depth', 5))  # niveles del book (depth)

        # Chequeo de balance en cuenta (Primary: get_account_report + get_account_position)
        self.check_balance_cuenta = config.get('check_balance_cuenta', True)
        self.balance_check_interval = float(config.get('balance_check_interval_seconds', 60.0))
        self._account_id = None
        self._last_balance_fetch = 0.0
        self._cached_account_report = None
        self._cached_positions = None
        self._positions_by_symbol = {}

        # Market data por ticker: { ticker: {'bid', 'offer', 'bid_size', 'offer_size', 'timestamp'} }
        self.market_data_by_ticker = {}
        self._subscription_sent = False
        self._symbols_received_logged = set()
        # Verificación BID/OFFER por ticker: log throttled cada N segundos
        self._last_bid_offer_log_time: Dict[str, float] = {}
        self.bid_offer_log_interval_seconds = float(config.get('bid_offer_log_interval_seconds', 30.0))
        # Diagnóstico: log una vez por ticker cuando llega Md pero sin BI/OF (mercado cerrado o estructura distinta)
        self._md_sin_bid_offer_logged: set = set()
        # Órdenes pendientes por par: ordenes_data_ars[i][cl_ord_id] = {..., 'timestamp': ts}
        self.ordenes_data_ars = {i: {} for i in range(len(self.pairs))}
        self.ordenes_data_usd = {i: {} for i in range(len(self.pairs))}
        # Map cl_ord_id -> (pair_idx, 'ars'|'usd') para notify
        self.order_to_pair_leg = {}
        # Estado por orden (para log ORDER_STATUS: old -> new)
        self.order_states: Dict[str, str] = {}
        # Última operación enviada por par (para loguear ganancia cuando ambas patas fill)
        self._last_operation_by_par: Dict[int, Dict] = {}
        self.proprietary = config.get('proprietary', 'api') or 'api'
        self.log_ultra_detallado = config.get('log_ultra_detallado', True)

        # Timeout y cancelación (como bot_ggal_futures_primary): si orden queda pendiente, cancelar
        self.order_timeout_seconds = float(config.get('order_timeout_seconds', 60.0))
        self.order_cancel_pending = set()

        # WebSocket: reconexión si error o sin datos
        self.websocket_connected = True
        self.last_market_data_time = None
        self.websocket_timeout_seconds = float(config.get('websocket_timeout_seconds', 90.0))
        self.max_reconnect_attempts = int(config.get('max_reconnect_attempts', 5))
        self.websocket_reconnect_attempts = 0
        self._arbitrage_two_legs_ready_logged = False

    def _get_pair_params(self, i: int) -> Tuple[int, float, int]:
        """size_tick, price_size_usd, contract_multiplier para el par i."""
        p = self.pair_params.get(i, {})
        return (
            int(p.get('size_tick', self.default_size_tick)),
            float(p.get('price_size_usd', self.default_price_size_usd)),
            int(p.get('contract_multiplier', self.default_contract_multiplier))
        )

    def initialize(self) -> bool:
        """Conexión Primary: mismo flujo que bot_ggal_futures_primary (auth + ws + suscripciones)."""
        def clean_env(v):
            if not v:
                return None
            v = str(v).strip()
            if v.startswith('{') and v.endswith('}'):
                v = v[1:-1].strip()
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1].strip()
            return v or None

        username = clean_env(os.getenv('PRIMARY_USERNAME') or os.getenv('PRIMARY_USER'))
        password = clean_env(os.getenv('PRIMARY_PASSWORD') or os.getenv('PRIMARY_PASS'))
        account = clean_env(os.getenv('PRIMARY_ACCOUNT') or os.getenv('PRIMARY_ACC'))
        if not username or not password:
            logger.error("PRIMARY_USERNAME y PRIMARY_PASSWORD requeridos en .env")
            return False
        account = account or username

        custom_api_url = clean_env(os.getenv('PRIMARY_API_URL') or os.getenv('MATRIZ_API_URL'))
        custom_ws_url = clean_env(os.getenv('PRIMARY_WS_URL') or os.getenv('MATRIZ_WS_URL'))
        is_eco = (
            'eco' in (username or '').lower() or
            os.getenv('USE_ECO_URLS', '').lower() == 'true' or
            (account and '347751' in str(account))
        )
        if not custom_api_url and not custom_ws_url and is_eco:
            custom_api_url = 'https://api.eco.xoms.com.ar/'
            custom_ws_url = 'wss://api.eco.xoms.com.ar/'
        logger.info("[PIPELINE] INICIO DEL PIPELINE DE CONEXIÓN")
        logger.info("[PIPELINE] Orden: 1) Configurar URLs → 2) Autenticar → 3) WebSocket → 4) Suscripciones")

        if custom_api_url:
            try:
                pyRofex._set_environment_parameter('url', custom_api_url, pyRofex.Environment.LIVE)
                logger.info(f"[CONFIG] API URL configurada: {custom_api_url}")
            except Exception:
                pass
        if custom_ws_url:
            try:
                pyRofex._set_environment_parameter('ws', custom_ws_url, pyRofex.Environment.LIVE)
                logger.info(f"[CONFIG] WebSocket URL configurada: {custom_ws_url}")
            except Exception:
                pass
        if not custom_api_url and not custom_ws_url:
            logger.info("[CONFIG] Usando URLs por defecto de pyRofex para ambiente LIVE")
        logger.info("[OK] Configuración de URLs completada")

        logger.info("[PIPELINE] Paso 1: AUTENTICACIÓN")
        logger.info("[AUTH] Intentando autenticación con Primary API...")
        logger.info(f"[AUTH] Usuario: {username}")
        logger.info(f"[AUTH] Account: {account or '(usar username)'}")
        logger.info("[AUTH] Ambiente: LIVE")

        try:
            pyRofex.initialize(
                user=username,
                password=password,
                account=account,
                environment=pyRofex.Environment.LIVE
            )
        except Exception as e1:
            try:
                pyRofex.initialize(user=username, password=password, environment=pyRofex.Environment.LIVE)
                account = None
            except Exception as e2:
                logger.error(f"Auth falló: {e1}; {e2}")
                return False

        self._account_id = account if account is not None else clean_env(os.getenv('PRIMARY_ACCOUNT') or os.getenv('PRIMARY_ACC')) or username
        logger.info("[OK] Autenticado con Primary" + (f" (cuenta {self._account_id})" if self._account_id else ""))
        logger.info("[AUTH] [OK] INICIALIZADO en ambiente: LIVE (PRODUCCIÓN)")

        logger.info("[PIPELINE] Paso 2: CONECTANDO WEBSOCKET")
        logger.info("[WS] Conectando WebSocket...")
        try:
            pyRofex.init_websocket_connection(
                market_data_handler=self._market_data_handler,
                order_report_handler=self._order_report_handler,
                error_handler=self._error_handler,
                exception_handler=self._exception_handler
            )
        except Exception as e:
            logger.error(f"WebSocket falló: {e}")
            return False
        logger.info("[OK] WebSocket inicializado correctamente")
        self.running = True

        time.sleep(2)

        # Resolver ticker caución 1 día EN PESOS antes de suscribir (CAAP1D)
        if self.modo_arbitraje_plazos and self.use_caucion_primary:
            ticker_cau = self._resolve_ticker_caucion_1d()
            self._ticker_caucion_1d_resolved = ticker_cau
            if ticker_cau:
                logger.info(f"[CAUCIÓN PESOS] Ticker caución 1d en pesos: {_short_label(ticker_cau)} → se suscribirá por WebSocket")
            else:
                logger.warning(
                    "[CAUCIÓN PESOS] No se encontró CAAP1D en Primary; se usará tasa de config (tasa_caucion_tomadora_pct_anual) "
                    "para teórico 24hs y fricción. Configure en config si desea un valor distinto de 0."
                )

        logger.info("[PIPELINE] Paso 3: SUSCRIBIENDO A MARKET DATA (BID/ASK + LAST)")
        all_tickers = self._build_all_tickers()
        all_tickers = [t for t in all_tickers if t and isinstance(t, str) and t.strip()]
        if not all_tickers:
            logger.error("[SUBSCRIPTION] Lista de tickers vacía; no se puede suscribir a market data")
        else:
            logger.info(
                f"[SUBSCRIPTION] Suscribiendo a market data para {len(all_tickers)} tickers | "
                f"entries: BIDS, OFFERS, LAST (bid/ask + último) | depth={max(1, self.market_data_depth)}"
            )
            for t in all_tickers:
                logger.info(f"[SUBSCRIPTION]   - {_short_label(t) or t}")
        try:
            if all_tickers:
                # Eco/Primary rechaza "MERV - XMEV - AL30 - CI:MERV don't exist"; suscribir sin market (default)
                pyRofex.market_data_subscription(
                    tickers=all_tickers,
                    entries=[
                        pyRofex.MarketDataEntry.BIDS,
                        pyRofex.MarketDataEntry.OFFERS,
                        pyRofex.MarketDataEntry.LAST
                    ],
                    depth=max(1, self.market_data_depth)
                )
                logger.info("[SUBSCRIPTION] market_data_subscription enviada (sin market; default para Eco/Primary)")
                self._subscription_sent = True
        except Exception as e:
            logger.warning(f"[SUBSCRIPTION] market_data_subscription: {e}")
        time.sleep(1)
        logger.info(
            "[OK] Suscripción a market data enviada (BID/ASK incorporados). "
            f"Fricción arbitraje incluye: comisiones round-trip {self.comision_round_trip_pct*100:.2f}%, caución, slippage."
        )

        logger.info("[PIPELINE] Paso 4: SUSCRIBIENDO A ORDER REPORTS")
        pyRofex.order_report_subscription()
        logger.info("[OK] Suscripción a order reports enviada")

        logger.info("=" * 60)
        logger.info("[OK] PIPELINE DE CONEXIÓN COMPLETADO")
        logger.info("=" * 60)
        logger.info("[OK] WebSocket conectado y suscripciones activas")
        logger.info("[OK] Bot listo para recibir datos de mercado y reportes de órdenes")

        if self.log_ultra_detallado:
            logger.info("=" * 60)
            logger.info("CONFIGURACIÓN DE ESTRATEGIA")
            logger.info("=" * 60)
            if self.modo_arbitraje_plazos:
                logger.info("[ARB] MOTOR DESARBITRAJE PLAZOS (AL30 CI vs 24hs) — todo en ARS, valor tiempo vía CAUCIÓN 1 DÍA:")
                logger.info(f"   - Especie en ARS: instrumento fijo {self.instrument} (solo AL30 CI y AL30 24hs)")
                logger.info(f"   - 2 tickers L2 (solo ARS): {_short_label(self.ars_ci)}, {_short_label(self.ars_24hs)}")
                logger.info(f"   - Condición: comprar CI y vender 24hs si TNA implícita > caución (tomadora)")
                logger.info(f"   - TNA implícita = (px_bid_24hs/px_offer_ci - 1)×(365/1); caución 1d Primary (CAAP1D) o config")
                logger.info(f"   - Caución 1 día EN PESOS (CAAP1D): use_caucion_primary={self.use_caucion_primary} (tomadora/colocadora en vivo)")
                logger.info(f"   - Ejecución atómica: {self.time_in_force_arbitrage} (evitar pierna coja)")
            logger.info("[ENTRY] ENTRADA:")
            logger.info(f"   - Acción: {self.accion.upper()}")
            logger.info(f"   - Pares: {[(_short_label(a).replace(' - CI','').replace(' - 24hs',''), _short_label(b).replace(' - CI','').replace(' - 24hs','')) for a, b in self.pairs]}")
            logger.info(f"   - Cotización objetivo: {self.cotizacion:.2f} | Límite compra: {self.cotizacion_limite_compra:.2f} | Límite venta: {self.cotizacion_limite_venta:.2f}")
            logger.info(f"   - Efectivo: {self.efectivo:,.0f} (usar {self.porcentaje_efectivo*100:.0f}%) | Stock: {self.stock:,.0f} (usar {self.porcentaje_stock*100:.0f}%)")
            logger.info("   - Requisito compra MEP: tener posición en bono USD (ej. AL30D) para vender; 'stock' es ese nominal disponible.")
            logger.info(f"   - Nominales máximo por operación: {self.nominales_maximo}")
            logger.info("[REFERENCIA DÓLAR MEP] Contra qué se compara:")
            if self.par_referencia_mep:
                t_ars, t_usd = self.par_referencia_mep
                lbl_ars = _short_label(t_ars).replace(" - CI", "")
                lbl_usd = _short_label(t_usd).replace(" - CI", "")
                logger.info(f"   - Cotización dólar MEP (referencia): par {lbl_ars}/{lbl_usd}")
                logger.info(f"   - Fórmula: offer_{lbl_ars} / bid_{lbl_usd} = tipo implícito compra MEP (lo que publican como 'dólar MEP')")
                logger.info(f"   - El bot opera con sus pares (ej. AL30/AL30D) y solo ejecuta si su tipo está al nivel o mejor que esta referencia MEP")
                logger.info(f"   - Comparar con referencia MEP: {self.comparar_con_referencia_mep} | CCL: {self.comparar_con_referencia_ccl}")
            else:
                logger.info("   - No hay par_referencia_mep configurado → no se compara contra cotización dólar MEP")
            logger.info("[FILTER] FILTRO:")
            logger.info(f"   - Profundidad mínima en book: {self.min_profundidad} | Máx: {self.max_profundidad}")
            logger.info(f"   - Edad máxima datos: {self.max_data_age_seconds}s | Spread máx %: {self.max_spread_pct}")
            logger.info("[TARGET] EJECUCIÓN:")
            logger.info(f"   - Tiempo entre envíos: {self.tiempo_espera}s | Order timeout: {self.order_timeout_seconds}s")
            logger.info("[SYSTEM] SISTEMA:")
            logger.info(f"   - Análisis (resumen) cada: {self.analisis_interval_seconds:.0f}s | Evaluar cada: {self.ejecucion_interval_seconds:.1f}s")
            logger.info(f"   - Market data depth: {self.market_data_depth} niveles | Check balance: {self.check_balance_cuenta}")
            logger.info("[FLUJO Y GANANCIA]")
            if self.accion == 'compra':
                logger.info("   - Compra MEP: comprás bono ARS + vendés bono USD → en un solo paso convertís pesos en dólares.")
                logger.info("   - Al liquidar tenés USD en la cuenta. No hay que vender después ni arbitrar.")
                logger.info("   - Ganancia: conseguís dólares al tipo que fijaste (cotizacion) o mejor; el bot solo ejecuta si el mercado te da ese precio o más barato.")
            else:
                logger.info("   - Venta MEP: vendés bono ARS + comprás bono USD → convertís dólares en pesos en un solo paso.")
                logger.info("   - Al liquidar tenés ARS en la cuenta. No hay segunda pata ni arbitraje.")
                logger.info("   - Ganancia: vendés USD al tipo que fijaste o mejor.")
            logger.info("=" * 60)

        logger.info(f"[OK] Suscrito a {all_tickers}")
        if self.modo_arbitraje_plazos:
            logger.info("[SUSCRIPCIÓN] Solo ARS: %s | %s" % (_short_label(self.ars_ci), _short_label(self.ars_24hs)))
            logger.info("[SUSCRIPCIÓN] BI/OF se reciben en horario BYMA (Lun-Vie 10:00-18:00 Argentina). Fuera de horario = sin book.")
        if self.tickers_referencia_dolar:
            logger.info(f"[INFO] Cotizaciones referencia dólar: {self.tickers_referencia_dolar}")
        if self.modo_arbitraje_plazos and self.use_caucion_primary and self._ticker_caucion_1d_resolved:
            logger.info(f"[CAUCIÓN PESOS] Caución 1d en pesos suscrita por WebSocket; actualización en vivo. Fallback: get_market_data cada {self.caucion_refresh_seconds}s")
        return True

    def _error_handler(self, message):
        logger.warning(f"[WS ERROR] {message}")
        err = str(message).lower()
        if any(k in err for k in ('connection', 'timeout', 'disconnected', 'closed', 'broken', 'io error', 'network')):
            logger.warning("[WS] Error crítico detectado - intentando reconectar...")
            self.websocket_connected = False
            self._reconnect_websocket()

    def _exception_handler(self, e):
        logger.error(f"[WS EXCEPTION] {e}")
        err = str(e).lower()
        if any(k in err for k in ('connection', 'timeout', 'broken', 'io', 'network', 'socket')):
            logger.warning("[WS] Excepción crítica - intentando reconectar...")
            self.websocket_connected = False
            self._reconnect_websocket()

    def _normalize_symbol_to_canonical(self, raw: Optional[str]) -> Optional[str]:
        """
        Mapea el símbolo que envía la API al ticker canónico (ars_ci o ars_24hs).
        Así se actualiza siempre el cache bajo la misma clave que usa _get_md() y la edad se refresca.
        """
        if not raw or not isinstance(raw, str):
            return None
        s = (raw or "").strip().upper()
        if not s:
            return None
        if "AL30" in s and ("CI" in s or s.rstrip().endswith("CI")):
            return self.ars_ci
        if "AL30" in s and ("24" in s or "24HS" in s):
            return self.ars_24hs
        return None

    def _market_data_handler(self, message):
        try:
            msg_type = (message.get('type') or '').strip().upper()
            if msg_type != 'MD':
                return
            inst = message.get('instrumentId') or {}
            symbol = inst.get('symbol') or inst.get('ticker') or message.get('symbol') or message.get('ticker')
            if not symbol:
                return
            symbol = str(symbol).strip()
            if symbol not in self._symbols_received_logged:
                self._symbols_received_logged.add(symbol)
                logger.info(f"[MD] Recibiendo datos: {_short_label(symbol)} (symbol={symbol!r})")
            bid_px, offer_px, bid_sz, offer_sz, bids_list, offers_list = _parse_md_bid_offer(message, self.market_data_depth)
            now_ts = time.time()
            data = {
                'bid': bid_px,
                'offer': offer_px,
                'bid_size': bid_sz,
                'offer_size': offer_sz,
                'bids': bids_list,
                'offers': offers_list,
                'timestamp': now_ts
            }
            # Merge con datos existentes: Primary puede enviar actualizaciones incrementales (solo BI o solo OF)
            existing = self.market_data_by_ticker.get(symbol) or {}
            canonical = self._normalize_symbol_to_canonical(symbol)
            if canonical:
                existing = existing or self.market_data_by_ticker.get(canonical) or {}
            if existing and (bid_px is None or offer_px is None):
                if bid_px is None and existing.get('bid') is not None:
                    data['bid'] = existing['bid']
                    data['bid_size'] = existing.get('bid_size', 0)
                    data['bids'] = data['bids'] or existing.get('bids') or []
                if offer_px is None and existing.get('offer') is not None:
                    data['offer'] = existing['offer']
                    data['offer_size'] = existing.get('offer_size', 0)
                    data['offers'] = data['offers'] or existing.get('offers') or []
            self.market_data_by_ticker[symbol] = data
            if canonical:
                self.market_data_by_ticker[canonical] = data
                bid_val = data.get('bid')
                offer_val = data.get('offer')
                if bid_val is not None and offer_val is not None:
                    # Verificación: log cuando tenemos BID y OFFER de cada ticker (throttled)
                    last_log = self._last_bid_offer_log_time.get(canonical, 0)
                    if now_ts - last_log >= self.bid_offer_log_interval_seconds:
                        self._last_bid_offer_log_time[canonical] = now_ts
                        label = 'AL30 CI' if canonical == self.ars_ci else 'AL30 24hs'
                        logger.info(f"[BID/OF] {label}: bid={bid_val:.2f} offer={offer_val:.2f} (recibidos OK)")
                else:
                    # Diagnóstico: Md recibido pero sin BI/OF (mercado cerrado o book vacío)
                    if canonical not in self._md_sin_bid_offer_logged:
                        self._md_sin_bid_offer_logged.add(canonical)
                        label = 'AL30 CI' if canonical == self.ars_ci else 'AL30 24hs'
                        md_keys = list((message.get('marketData') or message).keys())[:15]
                        logger.info(
                            f"[MD DIAG] {label}: mensaje recibido sin BI/OF. "
                            f"Horario BYMA: Lun-Vie 10:00-18:00 Argentina. Fuera de horario = book vacío (normal). "
                            f"Claves en mensaje: {md_keys}"
                        )
            ticker_normalized = _byma_ticker(symbol)
            if ticker_normalized != symbol:
                self.market_data_by_ticker[ticker_normalized] = data
            short = _short_label(symbol)
            if short and short != symbol:
                self.market_data_by_ticker[short] = data
            self.last_market_data_time = now_ts
            # Si es el ticker de caución 1d, actualizar tasas en vivo (offer=tomadora, bid=colocadora)
            ticker_cau = getattr(self, '_ticker_caucion_1d_resolved', None)
            if ticker_cau and self.use_caucion_primary:
                sym_match = (symbol == ticker_cau or symbol == _short_label(ticker_cau) or
                             _short_label(symbol) == _short_label(ticker_cau))
                if sym_match and (bid_px is not None or offer_px is not None):
                    if offer_px is not None:
                        px_f = float(offer_px)
                        self._cached_tasa_tomadora_pct_anual = px_f * 100.0 if px_f <= 2.0 else px_f
                    if bid_px is not None:
                        px_f = float(bid_px)
                        self._cached_tasa_colocadora_pct_anual = px_f * 100.0 if px_f <= 2.0 else px_f
                    if self.log_ultra_detallado:
                        logger.info(f"[CAUCIÓN PESOS WS] {_short_label(symbol)} Tomadora={self._cached_tasa_tomadora_pct_anual}% Colocadora={self._cached_tasa_colocadora_pct_anual}%")
            if self._subscription_sent:
                now = time.time()
                if now - self._last_ejecucion_time >= self.ejecucion_interval_seconds:
                    self._last_ejecucion_time = now
                    self._evaluate_pairs()
                if self.running:
                    self._check_order_timeout()
        except Exception as e:
            logger.error(f"[MD] {e}")

    def _fetch_balance_and_positions(self) -> Dict[str, Any]:
        """Obtiene balance (account report) y posiciones (account position) de la cuenta. Cache por balance_check_interval."""
        now = time.time()
        if not self.check_balance_cuenta or not self._account_id:
            return {}
        if now - self._last_balance_fetch < self.balance_check_interval:
            return {
                'report': self._cached_account_report,
                'positions': self._cached_positions,
                'positions_by_symbol': getattr(self, '_positions_by_symbol', {}),
            }
        self._last_balance_fetch = now
        out = {'report': None, 'positions': None, 'positions_by_symbol': {}}
        try:
            rep = pyRofex.get_account_report(account=self._account_id)
            if rep and rep.get('status') == 'OK':
                self._cached_account_report = rep
                out['report'] = rep
        except Exception as e:
            logger.warning(f"[BALANCE] Error get_account_report: {e}")
        try:
            pos = pyRofex.get_account_position(account=self._account_id)
            if pos and pos.get('status') == 'OK':
                self._cached_positions = pos.get('positions', [])
                by_sym = {}
                for p in self._cached_positions:
                    sym = (p.get('instrumentId') or {}).get('symbol') or (p.get('instrumentId') or {}).get('ticker')
                    if sym:
                        by_sym[sym] = int(p.get('netQuantity', 0))
                self._positions_by_symbol = by_sym
                out['positions'] = self._cached_positions
                out['positions_by_symbol'] = by_sym
        except Exception as e:
            logger.warning(f"[BALANCE] Error get_account_position: {e}")
        return out

    def _balance_ars_from_report(self, report: Dict) -> Optional[float]:
        """Extrae saldo ARS del account report (Primary API puede devolver cash/balance por moneda)."""
        if not report:
            return None
        ar = report.get('accountReport') or report.get('report') or report
        if isinstance(ar, list):
            for item in ar:
                if (item.get('currency') or item.get('currencyId') or '').upper() in ('ARS', '032'):
                    return float(item.get('balance', item.get('amount', 0)) or 0)
            return None
        cash = ar.get('cash') or ar.get('cashBalance') or {}
        if isinstance(cash, dict):
            return float(cash.get('ARS', cash.get('032', cash.get('available', 0))) or 0)
        if isinstance(cash, (int, float)):
            return float(cash)
        bal = ar.get('balance') or ar.get('availableBalance') or ar.get('totalBalance')
        if bal is not None:
            return float(bal)
        return None

    def _balance_usd_from_report(self, report: Dict) -> Optional[float]:
        """Extrae saldo USD del account report si existe."""
        if not report:
            return None
        ar = report.get('accountReport') or report.get('report') or report
        if isinstance(ar, list):
            for item in ar:
                if (item.get('currency') or item.get('currencyId') or '').upper() in ('USD', '840'):
                    return float(item.get('balance', item.get('amount', 0)) or 0)
            return None
        cash = ar.get('cash') or ar.get('cashBalance') or {}
        if isinstance(cash, dict):
            return float(cash.get('USD', cash.get('840', 0))) or 0
        return None

    def _get_referencia_mep(self) -> Optional[float]:
        """Calcula cotización implícita MEP de referencia desde par_referencia_mep (offer_ars/bid_usd = lado compra MEP)."""
        if not self.par_referencia_mep:
            return None
        t_ars, t_usd = self.par_referencia_mep
        md_ars = self.market_data_by_ticker.get(t_ars)
        md_usd = self.market_data_by_ticker.get(t_usd)
        if not md_ars or not md_usd:
            return None
        offer_ars = md_ars.get('offer')
        bid_usd = md_usd.get('bid')
        if offer_ars is not None and bid_usd is not None and bid_usd > 0:
            return offer_ars / bid_usd
        return None

    def _get_referencia_ccl(self) -> Optional[float]:
        """Calcula cotización implícita CCL de referencia desde par_referencia_ccl (offer_ars/bid_usd)."""
        if not self.par_referencia_ccl:
            return None
        t_ars, t_usd = self.par_referencia_ccl
        md_ars = self.market_data_by_ticker.get(t_ars)
        md_usd = self.market_data_by_ticker.get(t_usd)
        if not md_ars or not md_usd:
            return None
        offer_ars = md_ars.get('offer')
        bid_usd = md_usd.get('bid')
        if offer_ars is not None and bid_usd is not None and bid_usd > 0:
            return offer_ars / bid_usd
        return None

    def _get_md(self, ticker: str) -> Optional[Dict]:
        """Market data por ticker; acepta formato completo (MERV - XMEV - X) o corto (X - CI)."""
        return self.market_data_by_ticker.get(ticker) or self.market_data_by_ticker.get(_short_label(ticker))

    def _get_tc_ci(self) -> Optional[float]:
        """TC implícito en Contado Inmediato: offer_ARS_CI / bid_USD_CI (costo compra MEP en CI)."""
        md_ars = self._get_md(self.ars_ci)
        md_usd = self._get_md(self.usd_ci)
        if not md_ars or not md_usd:
            return None
        offer_ars = md_ars.get('offer')
        bid_usd = md_usd.get('bid')
        if offer_ars is not None and bid_usd is not None and bid_usd > 0:
            return offer_ars / bid_usd
        return None

    def _get_tc_24hs(self) -> Optional[float]:
        """TC implícito en 24hs: offer_ARS_24hs / bid_USD_24hs (costo compra MEP en 24hs)."""
        md_ars = self._get_md(self.ars_24hs)
        md_usd = self._get_md(self.usd_24hs)
        if not md_ars or not md_usd:
            return None
        offer_ars = md_ars.get('offer')
        bid_usd = md_usd.get('bid')
        if offer_ars is not None and bid_usd is not None and bid_usd > 0:
            return offer_ars / bid_usd
        return None

    def _get_tc_24hs_teorico(self) -> Optional[float]:
        """
        TC teórico a 24hs: llevar el activo de CI a 24hs más la tasa (caución).
        Fórmula: TC_24hs_teorico = TC_CI * (1 + tasa_tomadora_anual * dias_entre_plazos / 365).
        """
        tc_ci = self._get_tc_ci()
        if tc_ci is None or tc_ci <= 0:
            return None
        tasa = self._get_tasa_caucion_tomadora_actual()  # % anual
        factor = 1.0 + (tasa / 100.0) * (self.dias_entre_plazos / 365.0)
        return tc_ci * factor

    def _get_precio_al30_ci_ars(self) -> Optional[float]:
        """Precio del bono AL30 en CI en ARS (offer = punta vendedor). Especie en ARS."""
        md = self._get_md(self.ars_ci)
        if not md:
            return None
        return md.get('offer')

    def _get_precio_al30_24hs_ars(self) -> Optional[float]:
        """Precio del bono AL30 en 24hs en ARS (offer). Especie en ARS."""
        md = self._get_md(self.ars_24hs)
        if not md:
            return None
        return md.get('offer')

    def _get_precio_al30_24hs_teorico_ars(self) -> Optional[float]:
        """
        Teórico en ARS del bono AL30 llevado a 24hs: precio CI en ARS más costo de financiar 1 día (caución).
        Fórmula: Precio_AL30_24hs_teorico_ARS = Precio_AL30_CI_ARS × (1 + tasa_tomadora × dias/365).
        Todo en ARS (especie en ARS).
        """
        precio_ci = self._get_precio_al30_ci_ars()
        if precio_ci is None or precio_ci <= 0:
            return None
        tasa = self._get_tasa_caucion_tomadora_actual()  # % anual
        factor = 1.0 + (tasa / 100.0) * (self.dias_entre_plazos / 365.0)
        return precio_ci * factor

    def _resolve_ticker_caucion_1d(self) -> Optional[str]:
        """Resuelve el ticker de caución a 1 día EN PESOS vía Primary (CAAP1D = caución en pesos 1 día)."""
        if self.caucion_ticker_1d:
            return _byma_ticker(self.caucion_ticker_1d) if not str(self.caucion_ticker_1d).upper().startswith("MERV") else self.caucion_ticker_1d
        try:
            instruments = pyRofex.get_detailed_instruments()
            if not instruments or 'instruments' not in instruments:
                return None
            inst_list = instruments.get('instruments') or []

            def _sym(inst):
                iid = inst.get('instrumentId') or inst
                return (iid.get('symbol') or iid.get('ticker') or inst.get('symbol') or inst.get('ticker') or '').strip()

            # Prioridad 1: CAAP1D = caución en PESOS 1 día
            for inst in inst_list:
                sym = _sym(inst)
                sym_u = sym.upper()
                if 'CAAP' in sym_u and ('1D' in sym_u or '1d' in sym_u):
                    out = _byma_ticker(sym) if not sym_u.startswith("MERV") else sym
                    logger.info("[CAUCIÓN] Usando caución en PESOS 1 día: %s" % _short_label(out))
                    return out
            for inst in inst_list:
                sym = _sym(inst)
                if 'CAAP1D' in sym.upper():
                    out = _byma_ticker(sym) if not sym.upper().startswith("MERV") else sym
                    logger.info("[CAUCIÓN] Usando caución en PESOS 1 día: %s" % _short_label(out))
                    return out
            # Fallback: cualquier caución 1 día
            for inst in inst_list:
                sym = _sym(inst)
                sym_u = sym.upper()
                if ('1D' in sym_u or '1d' in sym_u) and ('CAA' in sym_u or 'CAU' in sym_u):
                    out = _byma_ticker(sym) if not sym_u.startswith("MERV") else sym
                    logger.warning("[CAUCIÓN] CAAP1D no encontrado; usando %s (verificar que sea en pesos)" % _short_label(out))
                    return out
            # Debug: mostrar algunos símbolos que contengan CAA/CAU para Eco
            samples = [s for s in [_sym(i) for i in inst_list[:200]] if s and ('CAA' in s.upper() or 'CAU' in s.upper())][:10]
            if samples:
                logger.debug("[CAUCIÓN] Símbolos tipo caución en Primary: %s" % samples)
            return None
        except Exception as e:
            logger.warning(f"[CAUCIÓN] Error resolviendo ticker caución 1d en pesos: {e}")
            return None

    def _fetch_tasa_caucion_1dia(self) -> Tuple[Optional[float], Optional[float]]:
        """
        Pide a Primary la caución a 1 día EN PESOS (CAAP1D).
        Retorna (tasa_tomadora_pct_anual, tasa_colocadora_pct_anual).
        En el book: offer = tasa tomadora (pedir prestado), bid = tasa colocadora (prestar).
        Precio en API suele ser decimal (0.75 = 75% TNA) o ya en %; se normaliza a % anual.
        """
        now = time.time()
        if now - self._last_caucion_fetch < self.caucion_refresh_seconds:
            return self._cached_tasa_tomadora_pct_anual, self._cached_tasa_colocadora_pct_anual
        if not self.use_caucion_primary:
            return self.tasa_caucion_tomadora_pct_anual or None, self.tasa_caucion_colocadora_pct_anual or None
        self._last_caucion_fetch = now
        try:
            ticker = self._ticker_caucion_1d_resolved
            if not ticker:
                ticker = self._resolve_ticker_caucion_1d()
                self._ticker_caucion_1d_resolved = ticker
            if not ticker:
                # Sin CAAP1D en Primary: usar tasa de config para teórico y fricción
                if self.tasa_caucion_tomadora_pct_anual is not None or self.tasa_caucion_colocadora_pct_anual is not None:
                    self._cached_tasa_tomadora_pct_anual = self.tasa_caucion_tomadora_pct_anual
                    self._cached_tasa_colocadora_pct_anual = self.tasa_caucion_colocadora_pct_anual
                return self._cached_tasa_tomadora_pct_anual, self._cached_tasa_colocadora_pct_anual
            md = pyRofex.get_market_data(
                ticker=ticker,
                entries=[
                    pyRofex.MarketDataEntry.BIDS,
                    pyRofex.MarketDataEntry.OFFERS,
                    pyRofex.MarketDataEntry.LAST,
                ],
                depth=1
            )
            md_inner = (md or {}).get('marketData') or md or {}
            bids = md_inner.get('BI') or md_inner.get('bids') or []
            offers = md_inner.get('OF') or md_inner.get('offers') or []
            tasa_tomadora = None  # mejor offer (pedir prestado)
            tasa_colocadora = None  # mejor bid (prestar)
            if isinstance(offers, list) and len(offers) > 0 and isinstance(offers[0], dict):
                px = offers[0].get('price')
                if px is not None:
                    px_f = float(px)
                    tasa_tomadora = px_f * 100.0 if px_f <= 2.0 else px_f  # 0.75 -> 75%
            if isinstance(bids, list) and len(bids) > 0 and isinstance(bids[0], dict):
                px = bids[0].get('price')
                if px is not None:
                    px_f = float(px)
                    tasa_colocadora = px_f * 100.0 if px_f <= 2.0 else px_f
            if tasa_tomadora is not None or tasa_colocadora is not None:
                self._cached_tasa_tomadora_pct_anual = tasa_tomadora
                self._cached_tasa_colocadora_pct_anual = tasa_colocadora
                if self.log_ultra_detallado:
                    logger.info(f"[CAUCIÓN PESOS] Ticker={_short_label(ticker)} Tomadora={tasa_tomadora}% Colocadora={tasa_colocadora}%")
        except Exception as e:
            logger.warning(f"[CAUCIÓN PESOS] Error get_market_data caución 1d en pesos: {e}")
        return self._cached_tasa_tomadora_pct_anual, self._cached_tasa_colocadora_pct_anual

    def _get_tasa_caucion_tomadora_actual(self) -> float:
        """Tasa tomadora anual (%) para fricción: live desde Primary o fallback a config."""
        self._fetch_tasa_caucion_1dia()
        if self._cached_tasa_tomadora_pct_anual is not None:
            return self._cached_tasa_tomadora_pct_anual
        return self.tasa_caucion_tomadora_pct_anual if self.tasa_caucion_tomadora_pct_anual is not None else 0.0

    def _get_friccion_arbitraje_pct_actual(self) -> float:
        """Fricción actual usando caución en vivo (Primary) si está disponible."""
        tasa_tomadora = self._get_tasa_caucion_tomadora_actual()
        costo_caucion_pct_dia = (tasa_tomadora / 100.0 / 365.0) * self.dias_entre_plazos
        costo_caucion_pct = costo_caucion_pct_dia * 100.0
        return (
            costo_caucion_pct
            + 2.0 * self.comision_round_trip_pct * 100.0
            + 2.0 * self.slippage_estimado_pct  # 2 patas ARS
        )

    def _log_resumen_dolar(self):
        """Log periódico compacto: una línea con todos los pares MEP, ref dólar, MEP/CCL ref y cuenta; segunda línea solo posiciones si hay."""
        now = time.time()
        if now - self._last_log_resumen < self.log_resumen_interval:
            return
        self._last_log_resumen = now

        # Construir segmentos para una línea [RESUMEN]
        segs = []
        should_log_no_data_error = (now - self._last_no_data_error_log) >= self.no_data_error_throttle_seconds

        if self.modo_arbitraje_plazos:
            # Solo 2 tickers ARS: AL30 CI, AL30 24hs
            for label, ticker in [("AL30 CI", self.ars_ci), ("AL30 24hs", self.ars_24hs)]:
                md = self._get_md(ticker) if ticker else None
                if not md:
                    segs.append(f"{label}:--")
                    if should_log_no_data_error:
                        logger.error(f"[ERROR] {label}: sin datos. {_no_data_error_message()}")
                        self._last_no_data_error_log = now
                    continue
                bid, offer = md.get('bid'), md.get('offer')
                if bid is not None and offer is not None:
                    segs.append(f"{label}:{bid:.0f}/{offer:.0f}")
                else:
                    segs.append(f"{label}:?")
        else:
            for i, (ticker_ars, ticker_usd) in enumerate(self.pairs):
                md_ars = self._get_md(ticker_ars)
                md_usd = self._get_md(ticker_usd) if ticker_usd else None
                label_ars = _short_label(ticker_ars).replace(" - CI", "").strip()
                label_usd = _short_label(ticker_usd).replace(" - CI", "").strip() if ticker_usd else "?"
                short_pair = f"{label_ars}/{label_usd}"
                if not md_ars or not md_usd:
                    segs.append(f"{short_pair}:--")
                    if should_log_no_data_error and ticker_usd:
                        logger.error(
                            f"[ERROR] Par {i}: sin datos. ARS ({_short_label(ticker_ars)}): {'sin md' if not md_ars else 'sin bid/ask'} | "
                            f"USD ({_short_label(ticker_usd)}): {'sin md' if not md_usd else 'sin bid/ask'}. {_no_data_error_message()}"
                        )
                        self._last_no_data_error_log = now
                    continue
                bid_ars, offer_ars = md_ars.get('bid'), md_ars.get('offer')
                bid_usd, offer_usd = md_usd.get('bid'), md_usd.get('offer')
                if bid_ars and offer_ars and bid_usd and offer_usd and bid_usd > 0 and offer_usd > 0:
                    cot_bid = bid_ars / bid_usd
                    cot_offer = offer_ars / offer_usd
                    spread_pct = ((cot_offer - cot_bid) / cot_bid * 100) if cot_bid else 0
                    segs.append(f"{short_pair}:{cot_offer:.0f} {spread_pct:.0f}%")
                else:
                    segs.append(f"{short_pair}:?")
                    if should_log_no_data_error:
                        logger.error(
                            f"[ERROR] Par {i}: puntas incompletas. ARS bid={bid_ars} ask={offer_ars} | USD bid={bid_usd} ask={offer_usd}. {_no_data_error_message()}"
                        )
                        self._last_no_data_error_log = now

        # Ref dólar (muy compacto: R: ticker bid/ask)
        ref_str = ""
        if self.tickers_referencia_dolar:
            ref_parts = []
            for t in self.tickers_referencia_dolar:
                md = self.market_data_by_ticker.get(t)
                if md and md.get('bid') and md.get('offer'):
                    lbl = _short_label(t).replace(" - CI", "").strip()
                    ref_parts.append(f"{lbl}:{md['bid']:.0f}/{md['offer']:.0f}")
            if ref_parts:
                ref_str = " R:" + " ".join(ref_parts)

        mep_ref = self._get_referencia_mep()
        ccl_ref = self._get_referencia_ccl()
        ref_mep_str = f" M:{mep_ref:.0f}" if mep_ref is not None else ""
        ref_ccl_str = f" C:{ccl_ref:.0f}" if ccl_ref is not None else ""
        # Arbitraje plazos: solo 2 patas en ARS (AL30 CI, AL30 24hs)
        if self.modo_arbitraje_plazos:
            legs_ok, legs_missing = self._get_arbitrage_two_legs_status()
            if not legs_ok:
                logger.info(f"[2 PATAS ARS] Datos faltantes o viejos: {legs_missing} (máx edad {self.max_data_age_seconds}s)")
            self._fetch_tasa_caucion_1dia()
            # Siempre mostrar caución (Primary o config) y teórico 24hs
            t_tom = self._cached_tasa_tomadora_pct_anual
            t_col = self._cached_tasa_colocadora_pct_anual
            caucion_fuente = "config" if not getattr(self, "_ticker_caucion_1d_resolved", None) else "Primary"
            if t_tom is not None or t_col is not None:
                t_tom_str = f"{t_tom:.1f}" if t_tom is not None else "--"
                t_col_str = f"{t_col:.1f}" if t_col is not None else "--"
                logger.info(f"[CAUCIÓN PESOS] Tomadora={t_tom_str}% Colocadora={t_col_str}% ({caucion_fuente})")
            else:
                logger.info("[CAUCIÓN PESOS] Sin tasa (config tasa_caucion_tomadora_pct_anual para teórico y fricción)")
            tasa_para_teorico = self._get_tasa_caucion_tomadora_actual()
            logger.info(
                f"[TEÓRICO 24hs] Fórmula: Precio_24hs_teorico_ARS = Precio_AL30_CI_ARS × (1 + tasa×1/365). Tasa usada: {tasa_para_teorico:.2f}% ({caucion_fuente})"
            )
            friccion_actual = self._get_friccion_arbitraje_pct_actual()
            precio_ci_ars = self._get_precio_al30_ci_ars()
            precio_24hs_teorico_ars = self._get_precio_al30_24hs_teorico_ars()
            precio_24hs_real_ars = self._get_precio_al30_24hs_ars()
            bid_24hs = self._get_md(self.ars_24hs)
            bid_24hs_val = bid_24hs.get('bid') if bid_24hs else None
            if precio_ci_ars is not None and precio_24hs_teorico_ars is not None:
                logger.info(
                    f"[ARS] AL30 CI offer={precio_ci_ars:,.2f} ARS | AL30 24hs teórico ARS (bono llevado a 24hs) = {precio_24hs_teorico_ars:,.2f} ARS"
                )
                if precio_24hs_real_ars is not None:
                    diff_ars = precio_24hs_real_ars - precio_24hs_teorico_ars
                    diff_ars_pct = (diff_ars / precio_24hs_teorico_ars * 100.0) if precio_24hs_teorico_ars else 0
                    logger.info(
                        f"[ARS] AL30 24hs real offer={precio_24hs_real_ars:,.2f} ARS | diff (real - teórico) = {diff_ars:+,.2f} ARS ({diff_ars_pct:+.3f}%)"
                    )
                # Comprar CI y vender 24hs si TNA implícita > caución (tomadora)
                if bid_24hs_val is not None and precio_ci_ars is not None and precio_ci_ars > 0:
                    dias_plazo = 1
                    tasa_impl_resumen = ((bid_24hs_val / precio_ci_ars) - 1) * (365 / dias_plazo) * 100.0
                    caución_resumen = self._get_tasa_caucion_tomadora_actual()
                    logger.info(
                        f"[TNA] Implícita = {tasa_impl_resumen:.2f}% | Caución = {caución_resumen:.2f}% | "
                        f"ejecutar (TNA > caución) = {tasa_impl_resumen > caución_resumen}"
                    )
            else:
                logger.info("[TEÓRICO 24hs] Valor requiere precio AL30 CI (market data). Sin datos = sin teórico.")
            logger.info(
                f"[PUNTAS] Solo ARS. Comisiones round-trip: {self.comision_round_trip_pct*100:.2f}% | "
                f"Fricción = caución + comisiones + slippage (2 patas)."
            )

        # Cuenta: A= U=
        cuenta_str = ""
        if self.check_balance_cuenta and self._account_id:
            bal_data = self._fetch_balance_and_positions()
            rep = bal_data.get('report')
            pos_by_sym = bal_data.get('positions_by_symbol') or {}
            bal_ars = self._balance_ars_from_report(rep) if rep else None
            bal_usd = self._balance_usd_from_report(rep) if rep else None
            if bal_ars is not None or bal_usd is not None:
                cuenta_str = " A:" + (f"{bal_ars:,.0f}" if bal_ars is not None else "?")
                cuenta_str += " U:" + (f"{bal_usd:,.0f}" if bal_usd is not None else "?")
            logger.info("[R] " + " ".join(segs) + ref_str + ref_mep_str + ref_ccl_str + cuenta_str)
            if pos_by_sym:
                relevant = [f"{_short_label(s).replace(' - CI','')}:{q}" for s, q in pos_by_sym.items() if q != 0]
                if relevant:
                    logger.info("[POS] " + " ".join(relevant))
        else:
            logger.info("[R] " + " ".join(segs) + ref_str + ref_mep_str + ref_ccl_str)

    def _evaluate_arbitraje_plazos(self):
        """
        Motor de arbitraje CI vs 24hs. Solo ARS (2 tickers: AL30 CI, AL30 24hs).
        Condición: comprar CI y vender 24hs si TNA implícita > caución (tomadora). Fin.
        """
        now = time.time()
        if self.log_ultra_detallado:
            logger.info("[ARB] ========== EVALUACIÓN ARBITRAJE PLAZOS (solo ARS, 2 patas) ==========")
        # Órdenes pendientes en cualquiera de las 2 patas (solo ARS)
        for i in (0, 1):
            if (self.ordenes_data_ars.get(i) or {}):
                if not self.log_skip_resumen_only:
                    logger.info(f"[ARB] Órdenes pendientes en pata {i} (CI o 24hs) → SKIP")
                return
        if self.order_cancel_pending:
            return

        legs_ok, legs_missing = self._get_arbitrage_two_legs_status()
        if not legs_ok:
            if not self.log_skip_resumen_only:
                logger.info(f"[ARB] SKIP: 2 patas incompletas o viejas: {legs_missing}")
                if any("sin bid/offer" in str(m) for m in legs_missing) and not _is_argentina_market_hours():
                    logger.info("[ARB] Fuera de horario BYMA (Lun-Vie 10:00-18:00 Arg): es normal no recibir BI/OF.")
            return

        precio_ci_ars = self._get_precio_al30_ci_ars()
        precio_24hs_teorico_ars = self._get_precio_al30_24hs_teorico_ars()
        md_ci = self._get_md(self.ars_ci)
        md_24 = self._get_md(self.ars_24hs)
        if not md_ci or not md_24 or precio_ci_ars is None or precio_24hs_teorico_ars is None or precio_24hs_teorico_ars <= 0:
            if (now - self._last_no_data_error_log) >= self.no_data_error_throttle_seconds:
                logger.warning("[ARB] Sin precios ARS (AL30 CI o 24hs)")
                self._last_no_data_error_log = now
            return
        bid_24hs = md_24.get('bid')
        offer_ci = md_ci.get('offer')
        if bid_24hs is None or offer_ci is None:
            if not self.log_skip_resumen_only:
                logger.info("[ARB] Puntas incompletas (offer CI o bid 24hs) → SKIP")
            return

        # Condición: comprar CI y vender 24hs si TNA implícita > caución (tomadora)
        dias_plazo = 1  # CI → 24hs = 1 día
        tasa_implicita_anual_pct = ((bid_24hs / offer_ci) - 1) * (365 / dias_plazo) * 100.0 if offer_ci else None
        tasa_caucion_tomadora = self._get_tasa_caucion_tomadora_actual()
        if tasa_implicita_anual_pct is None:
            return
        if not self.log_skip_resumen_only:
            logger.info(
                f"[TNA] Implícita = {tasa_implicita_anual_pct:.2f}% | Caución tomadora = {tasa_caucion_tomadora:.2f}% | "
                f"cumple (TNA > caución) = {tasa_implicita_anual_pct > tasa_caucion_tomadora}"
            )
        if tasa_implicita_anual_pct <= tasa_caucion_tomadora:
            if not self.log_skip_resumen_only:
                logger.info(
                    f"[ARB] No ejecutar: TNA implícita {tasa_implicita_anual_pct:.2f}% <= caución {tasa_caucion_tomadora:.2f}%"
                )
            return

        if (now - md_ci.get('timestamp', 0)) > self.max_data_age_seconds or (now - md_24.get('timestamp', 0)) > self.max_data_age_seconds:
            if not self.log_skip_resumen_only:
                logger.warning("[ARB] Datos viejos → SKIP")
            return

        sz_ci = int(md_ci.get('offer_size', 0) or 0)
        sz_24 = int(md_24.get('bid_size', 0) or 0)
        size_tick_0, _, contract_mult_0 = self._get_pair_params(0)
        size_tick_1, _, contract_mult_1 = self._get_pair_params(1)
        qty_cap = min(sz_ci, sz_24)
        if qty_cap < self.min_profundidad:
            if not self.log_skip_resumen_only:
                logger.info(f"[ARB] Profundidad insuficiente (min={qty_cap}, mín={self.min_profundidad}) → SKIP")
            return

        efectivo_restante = self.efectivo * self.porcentaje_efectivo - self.operado
        qty_efectivo = int(efectivo_restante / offer_ci) if offer_ci else 0
        qty_orden = min(qty_cap, qty_efectivo, self.nominales_maximo)
        if self.max_profundidad is not None:
            qty_orden = min(qty_orden, int(self.max_profundidad))
        qty_final = (qty_orden // size_tick_0) * size_tick_0 if size_tick_0 else qty_orden
        qty_final = (qty_final // size_tick_1) * size_tick_1 if size_tick_1 else qty_final
        if qty_final < max(contract_mult_0, contract_mult_1):
            if not self.log_skip_resumen_only:
                logger.info(f"[ARB] Cantidad final insuficiente (qty={qty_final}) → SKIP")
            return

        tif = self.time_in_force_arbitrage
        logger.info(
            f"[ARB] Ejecutando: TNA implícita {tasa_implicita_anual_pct:.2f}% > caución {tasa_caucion_tomadora:.2f}% | "
            f"Comprar CI @ {offer_ci:.2f} | Vender 24hs @ {bid_24hs:.2f} | qty={qty_final} TIF={tif}"
        )
        # 2 órdenes en ARS: Buy AL30 CI @ offer_ci, Sell AL30 24hs @ bid_24hs
        o_ars_ci = self._place_order(self.ars_ci, 'BUY', offer_ci, qty_final, time_in_force=tif)
        o_ars_24 = self._place_order(self.ars_24hs, 'SELL', bid_24hs, qty_final, time_in_force=tif)

        if not o_ars_ci or not o_ars_24:
            logger.warning("[ARB] Falló envío de una o más órdenes (pierna coja riesgo)")
            return
        now_ts = time.time()
        for idx in (0, 1):
            self._last_operation_by_par[idx] = {
                'cotizacion_limite': precio_24hs_teorico_ars if idx == 1 else offer_ci,
                'qty': qty_final,
                'time': now_ts,
                'fills': {},
            }
        for cl_key, leg_tuple in [
            (o_ars_ci.get('order', {}).get('clientId'), (0, 'ars')),
            (o_ars_24.get('order', {}).get('clientId'), (1, 'ars')),
        ]:
            if cl_key:
                self.order_to_pair_leg[cl_key] = leg_tuple
                self.ordenes_data_ars[leg_tuple[0]][cl_key] = {'timestamp': now_ts}
        self.ultimo_envio = datetime.now()
        logger.info("[ARB] 2 órdenes enviadas (solo ARS): Compra AL30 CI | Venta AL30 24hs — " + tif)

    def _evaluate_pairs(self):
        """Para cada par, si cotización cumple, datos frescos, profundidad y spread OK, y no hay órdenes pendientes, envía las dos patas.
        Si modo_arbitraje_plazos: delega en _evaluate_arbitraje_plazos (TC_CI vs TC_24hs, FOK/IOC)."""
        now = time.time()
        if self.log_ultra_detallado:
            logger.info("[ANALISIS] ========== INICIO EVALUACIÓN DE PARES ==========")
            logger.info("[ANALISIS] _evaluate_pairs() LLAMADO")
        self._log_resumen_dolar()

        if self.modo_arbitraje_plazos:
            self._evaluate_arbitraje_plazos()
            return

        if self.ultimo_envio is not None:
            if (datetime.now() - self.ultimo_envio).total_seconds() < self.tiempo_espera:
                return
        efectivo_restante = self.efectivo * self.porcentaje_efectivo - self.operado
        stock_restante = self.stock * self.porcentaje_stock - self.stock_operado
        if efectivo_restante <= 0 or stock_restante <= 0:
            logger.info("[INFO] Objetivo completado (efectivo o stock); no se envían más órdenes")
            return
        if self.order_cancel_pending:
            return

        for i, (ticker_ars, ticker_usd) in enumerate(self.pairs):
            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] ---------- Par {i} ({_short_label(ticker_ars).replace(' - CI','')}/{_short_label(ticker_usd).replace(' - CI','')}) ----------")
            o_ars = self.ordenes_data_ars.get(i, {})
            o_usd = self.ordenes_data_usd.get(i, {})
            if len(o_ars) > 0 or len(o_usd) > 0:
                if self.log_ultra_detallado:
                    logger.info(f"[ANALISIS] Par {i}: órdenes pendientes ARS={len(o_ars)} USD={len(o_usd)} → SKIP (esperar confirmación)")
                if not self.log_skip_resumen_only:
                    logger.info(f"[SKIP] Par {i}: hay órdenes pendientes (ARS={len(o_ars)} USD={len(o_usd)})")
                continue
            md_ars = self._get_md(ticker_ars)
            md_usd = self._get_md(ticker_usd)
            if not md_ars or not md_usd:
                throttle_ok = (now - self._last_no_data_error_log) >= self.no_data_error_throttle_seconds
                if throttle_ok:
                    b_ars = (f"bid={md_ars.get('bid'):.2f} ask={md_ars.get('offer'):.2f}" if md_ars and md_ars.get('bid') is not None and md_ars.get('offer') is not None else "N/A")
                    b_usd = (f"bid={md_usd.get('bid'):.2f} ask={md_usd.get('offer'):.2f}" if md_usd and md_usd.get('bid') is not None and md_usd.get('offer') is not None else "N/A")
                    logger.error(
                        f"[ERROR] Par {i}: falta market data. "
                        f"{_short_label(ticker_ars)}: {b_ars} | {_short_label(ticker_usd)}: {b_usd}. "
                        f"{_no_data_error_message()}"
                    )
                    self._last_no_data_error_log = now
                if not self.log_skip_resumen_only:
                    logger.info(f"[SKIP] Par {i}: falta market data (ARS={bool(md_ars)} USD={bool(md_usd)})")
                continue

            # Instantaneidad: datos frescos para no quedarse colgado
            age_ars = now - md_ars.get('timestamp', 0)
            age_usd = now - md_usd.get('timestamp', 0)
            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] Par {i}: datos mercado | edad ARS={age_ars:.1f}s USD={age_usd:.1f}s (máx={self.max_data_age_seconds}s)")
            if age_ars > self.max_data_age_seconds or age_usd > self.max_data_age_seconds:
                if self.log_ultra_detallado:
                    logger.info(f"[ANALISIS] Par {i}: datos viejos → SKIP")
                if not self.log_skip_resumen_only:
                    logger.warning(
                        f"[SKIP] Par {i}: datos viejos (edad ARS={age_ars:.1f}s USD={age_usd:.1f}s, máx={self.max_data_age_seconds}s)"
                    )
                continue

            bid_ars = md_ars.get('bid')
            offer_ars = md_ars.get('offer')
            bid_usd = md_usd.get('bid')
            offer_usd = md_usd.get('offer')
            if self.accion == 'compra':
                px_ars = offer_ars
                px_usd = bid_usd
            else:
                px_ars = bid_ars
                px_usd = offer_usd
            if px_ars is None or px_usd is None or px_usd <= 0:
                if not self.log_skip_resumen_only:
                    logger.info(f"[SKIP] Par {i}: puntas incompletas (ARS {bid_ars}/{offer_ars} USD {bid_usd}/{offer_usd})")
                continue

            cotizacion_mercado = px_ars / px_usd
            # Spread entre puntas: bid/offer del tipo implícito MEP (offer_ars/bid_usd = mejor oferta MEP, bid_ars/offer_usd = mejor bid MEP)
            if bid_ars and offer_ars and bid_usd and offer_usd and bid_usd > 0 and offer_usd > 0:
                mep_offer = offer_ars / bid_usd
                mep_bid = bid_ars / offer_usd
                spread_impl_pct = ((mep_offer - mep_bid) / mep_bid * 100) if mep_bid else 0
            else:
                spread_impl_pct = 0
            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] Par {i}: cotización mercado={cotizacion_mercado:.2f} | spread implícito={spread_impl_pct:.2f}% (máx={self.max_spread_pct})")
            if self.max_spread_pct is not None and spread_impl_pct > float(self.max_spread_pct):
                if not self.log_skip_resumen_only:
                    logger.info(
                        f"[SKIP] Par {i}: spread entre puntas alto ({spread_impl_pct:.2f}% > máx {self.max_spread_pct}%)"
                    )
                continue

            if self.accion == 'compra':
                if cotizacion_mercado > self.cotizacion_limite_compra:
                    if not self.log_skip_resumen_only:
                        logger.info(
                            f"[SKIP] Par {i}: cotización no cumple compra (mercado={cotizacion_mercado:.2f} > límite={self.cotizacion_limite_compra:.2f})"
                        )
                    continue
                # Comparar contra dólar MEP de referencia: solo comprar si nuestro par está al nivel o mejor (<= MEP ref)
                if self.comparar_con_referencia_mep:
                    mep_ref = self._get_referencia_mep()
                    if mep_ref is not None and cotizacion_mercado > mep_ref:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: cotización por encima del MEP de referencia (nuestro={cotizacion_mercado:.2f} > MEP ref={mep_ref:.2f})"
                            )
                        continue
                if self.comparar_con_referencia_ccl:
                    ccl_ref = self._get_referencia_ccl()
                    if ccl_ref is not None and cotizacion_mercado > ccl_ref:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: cotización por encima del CCL de referencia (nuestro={cotizacion_mercado:.2f} > CCL ref={ccl_ref:.2f})"
                            )
                        continue
            else:
                if cotizacion_mercado < self.cotizacion_limite_venta:
                    if not self.log_skip_resumen_only:
                        logger.info(
                            f"[SKIP] Par {i}: cotización no cumple venta (mercado={cotizacion_mercado:.2f} < límite={self.cotizacion_limite_venta:.2f})"
                        )
                    continue
                # Comparar contra dólar MEP de referencia: solo vender si nuestro par está al nivel o mejor (>= MEP ref)
                if self.comparar_con_referencia_mep:
                    mep_ref = self._get_referencia_mep()
                    if mep_ref is not None and cotizacion_mercado < mep_ref:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: cotización por debajo del MEP de referencia (nuestro={cotizacion_mercado:.2f} < MEP ref={mep_ref:.2f})"
                            )
                        continue
                if self.comparar_con_referencia_ccl:
                    ccl_ref = self._get_referencia_ccl()
                    if ccl_ref is not None and cotizacion_mercado < ccl_ref:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: cotización por debajo del CCL de referencia (nuestro={cotizacion_mercado:.2f} < CCL ref={ccl_ref:.2f})"
                            )
                        continue

            size_tick, price_size_usd, contract_mult = self._get_pair_params(i)
            qty_ars = md_ars.get('offer_size', 0) if self.accion == 'compra' else md_ars.get('bid_size', 0)
            qty_usd = md_usd.get('bid_size', 0) if self.accion == 'compra' else md_usd.get('offer_size', 0)
            qty_ars_int = max(0, int(qty_ars))
            qty_usd_int = max(0, int(qty_usd))
            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] Par {i}: profundidad book ARS={qty_ars_int} USD={qty_usd_int} (mín={self.min_profundidad})")

            # Profundidad mínima: no operar si el book no tiene liquidez suficiente
            if qty_ars_int < self.min_profundidad or qty_usd_int < self.min_profundidad:
                if not self.log_skip_resumen_only:
                    logger.info(
                        f"[SKIP] Par {i}: profundidad insuficiente (ARS={qty_ars_int} USD={qty_usd_int}, mín={self.min_profundidad})"
                    )
                continue

            qty_efectivo = int(efectivo_restante / px_usd * price_size_usd) if px_usd else 0
            qty_orden = min(
                qty_ars_int,
                qty_usd_int,
                int(stock_restante),
                qty_efectivo,
                self.nominales_maximo
            )
            if self.max_profundidad is not None:
                qty_orden = min(qty_orden, int(self.max_profundidad))
            qty_final = (qty_orden // size_tick) * size_tick if size_tick else qty_orden
            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] Par {i}: cantidad calculada qty_final={qty_final} (size_tick={size_tick} mult={contract_mult})")
            if qty_final < contract_mult:
                if self.log_ultra_detallado:
                    logger.info(f"[ANALISIS] Par {i}: cantidad final insuficiente → SKIP")
                if not self.log_skip_resumen_only:
                    logger.info(f"[SKIP] Par {i}: cantidad final insuficiente (qty_final={qty_final} < mult={contract_mult})")
                continue

            # Chequeo de balance en cuenta: ARS y posiciones de bonos (solo si se obtuvo datos)
            if self.check_balance_cuenta and self._account_id:
                bal_data = self._fetch_balance_and_positions()
                rep = bal_data.get('report')
                pos_by_sym = bal_data.get('positions_by_symbol') or {}
                bal_ars = self._balance_ars_from_report(rep) if rep else None
                monto_ars_necesario = qty_final * px_ars
                if self.accion == 'compra':
                    pos_usd = pos_by_sym.get(ticker_usd, 0)
                    if bal_ars is not None and bal_ars < monto_ars_necesario:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: balance ARS insuficiente en cuenta (ARS={bal_ars:,.0f} < {monto_ars_necesario:,.0f})"
                            )
                        continue
                    if bal_data.get('positions') is not None and pos_usd < qty_final:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: posición insuficiente de bono USD en cuenta ({ticker_usd}={pos_usd} < {qty_final})"
                            )
                        continue
                else:
                    pos_ars = pos_by_sym.get(ticker_ars, 0)
                    if bal_data.get('positions') is not None and pos_ars < qty_final:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: posición insuficiente de bono ARS en cuenta ({ticker_ars}={pos_ars} < {qty_final})"
                            )
                        continue
                    if bal_ars is not None and bal_ars < qty_final * px_usd:
                        if not self.log_skip_resumen_only:
                            logger.info(
                                f"[SKIP] Par {i}: balance insuficiente en cuenta para pata USD (ARS={bal_ars:,.0f})"
                            )
                        continue

            if self.log_ultra_detallado:
                logger.info(f"[ANALISIS] Par {i}: [OK] TODAS LAS CONDICIONES CUMPLIDAS → ENVIAR ÓRDENES")
                logger.info("[ANALISIS] ========== FIN EVALUACIÓN (decisión: enviar) ==========")
            logger.info(
                f"[EVAL] Par {i} listo: cot={cotizacion_mercado:.2f} spread={spread_impl_pct:.2f}% prof ARS={qty_ars_int} USD={qty_usd_int} -> enviando {qty_final} nominales"
            )
            # Enviar dos órdenes
            if self.accion == 'compra':
                order_ars = self._place_order(ticker_ars, 'BUY', px_ars, qty_final)
                order_usd = self._place_order(ticker_usd, 'SELL', px_usd, qty_final)
            else:
                order_ars = self._place_order(ticker_ars, 'SELL', px_ars, qty_final)
                order_usd = self._place_order(ticker_usd, 'BUY', px_usd, qty_final)

            if order_ars and order_usd:
                cl_ars = order_ars.get('order', {}).get('clientId')
                cl_usd = order_usd.get('order', {}).get('clientId')
                now_ts = time.time()
                cot_lim = self.cotizacion_limite_compra if self.accion == 'compra' else self.cotizacion_limite_venta
                self._last_operation_by_par[i] = {
                    'cotizacion_limite': cot_lim,
                    'qty': qty_final,
                    'time': now_ts,
                    'fills': {},
                }
                if cl_ars:
                    self.ordenes_data_ars[i][cl_ars] = {**(order_ars or {}), 'timestamp': now_ts}
                    self.order_to_pair_leg[cl_ars] = (i, 'ars')
                if cl_usd:
                    self.ordenes_data_usd[i][cl_usd] = {**(order_usd or {}), 'timestamp': now_ts}
                    self.order_to_pair_leg[cl_usd] = (i, 'usd')
                self.ultimo_envio = datetime.now()
                logger.info(
                    f"[MEP] Par {i} órdenes enviadas: ARS {cl_ars} USD {cl_usd} | cot={cotizacion_mercado:.2f} spread={spread_impl_pct:.2f}% qty={qty_final}"
                )
                return  # un par por ciclo
            else:
                logger.warning(f"[MEP] Par {i}: falló envío de una o ambas órdenes")

        if self.log_ultra_detallado:
            logger.info("[ANALISIS] ========== FIN EVALUACIÓN (sin envío este ciclo) ==========")

    def _get_arbitrage_two_legs_status(self) -> Tuple[bool, List[str]]:
        """
        Verifica que las 2 patas en ARS (AL30 CI, AL30 24hs) tengan datos frescos.
        Solo especie en ARS; no se usa D. Retorna (ok, lista de patas faltantes o viejas).
        """
        if not self.modo_arbitraje_plazos:
            return True, []
        now = time.time()
        legs = [
            ('AL30 CI', self.ars_ci),
            ('AL30 24hs', self.ars_24hs),
        ]
        missing = []
        for label, ticker in legs:
            if not ticker:
                continue
            md = self._get_md(ticker)
            if not md:
                missing.append(f"{label}(sin md)")
                continue
            ts = md.get('timestamp', 0)
            age = now - ts
            if age > self.max_data_age_seconds:
                missing.append(f"{label}(edad {age:.0f}s)")
            elif md.get('bid') is None or md.get('offer') is None:
                missing.append(f"{label}(sin bid/offer)")
        ok = len(missing) == 0
        if ok and self.modo_arbitraje_plazos and not getattr(self, '_arbitrage_two_legs_ready_logged', False):
            self._arbitrage_two_legs_ready_logged = True
            logger.info("[MD] 2 patas AL30 CI + 24hs (solo ARS) con dato recibido - suscripción OK")
        return (ok, missing)

    def _build_all_tickers(self) -> List[str]:
        """Lista de tickers a suscribir: AL30 CI, AL30 24hs (ARS) + caución 1d si está resuelta."""
        all_tickers = []
        if self.modo_arbitraje_plazos:
            all_tickers.extend([self.ars_ci, self.ars_24hs])
            # Incluir caución 1 día en la suscripción para recibir tasas por WebSocket
            ticker_cau = getattr(self, '_ticker_caucion_1d_resolved', None)
            if ticker_cau and self.use_caucion_primary and ticker_cau not in all_tickers:
                all_tickers.append(ticker_cau)
        for t_ars, t_usd in self.pairs:
            if t_ars and t_ars not in all_tickers:
                all_tickers.append(t_ars)
            if t_usd and t_usd not in all_tickers:
                all_tickers.append(t_usd)
        return list(dict.fromkeys(all_tickers))

    def _cancel_order(self, cl_ord_id: str) -> bool:
        """Solicita cancelación de orden y la marca como pendiente de confirmación."""
        if cl_ord_id in self.order_cancel_pending:
            return True
        try:
            pyRofex.cancel_order(client_order_id=cl_ord_id, proprietary=self.proprietary)
            self.order_cancel_pending.add(cl_ord_id)
            logger.info(f"[CANCEL] Cancelación solicitada: {cl_ord_id}")
            return True
        except Exception as e:
            logger.warning(f"[CANCEL] Error cancelando {cl_ord_id}: {e}")
            return False

    def _cancel_other_legs_same_round(self, current_cl_ord_id: str, current_i: int, current_leg: str, now_ts: float):
        """
        En arbitraje 2 patas: si una pata fue CANCELLED/REJECTED, cancelar la otra
        de la misma ronda para evitar pierna coja.
        """
        round_window = 120.0  # órdenes enviadas en la misma ventana son misma ronda
        to_cancel = []
        for pi in range(len(self.pairs)):
            for cl_id, data in list((self.ordenes_data_ars.get(pi) or {}).items()):
                if cl_id == current_cl_ord_id:
                    continue
                ts = data.get('timestamp', 0)
                if ts and (now_ts - ts) <= round_window and cl_id not in self.order_cancel_pending:
                    to_cancel.append((cl_id, pi, 'ars'))
            for cl_id, data in list((self.ordenes_data_usd.get(pi) or {}).items()):
                if cl_id == current_cl_ord_id:
                    continue
                ts = data.get('timestamp', 0)
                if ts and (now_ts - ts) <= round_window and cl_id not in self.order_cancel_pending:
                    to_cancel.append((cl_id, pi, 'usd'))
        if to_cancel:
            logger.warning(f"[4 PATAS] Una pata {current_cl_ord_id} cancelada/rechazada → cancelando otras {len(to_cancel)} patas de la misma ronda")
        for cl_id, _pi, _leg in to_cancel:
            self._cancel_order(cl_id)

    def _check_order_timeout(self):
        """Si una orden lleva más de order_timeout_seconds pendiente, solicita cancelación (como bot_ggal_futures)."""
        if not hasattr(self, '_last_timeout_log'):
            self._last_timeout_log = 0
        now = time.time()
        to_cancel = []
        for i in range(len(self.pairs)):
            for cl_ord_id, data in list((self.ordenes_data_ars.get(i) or {}).items()):
                ts = data.get('timestamp', 0)
                if ts and (now - ts) > self.order_timeout_seconds and cl_ord_id not in self.order_cancel_pending:
                    to_cancel.append((cl_ord_id, 'ars', i, now - ts))
            for cl_ord_id, data in list((self.ordenes_data_usd.get(i) or {}).items()):
                ts = data.get('timestamp', 0)
                if ts and (now - ts) > self.order_timeout_seconds and cl_ord_id not in self.order_cancel_pending:
                    to_cancel.append((cl_ord_id, 'usd', i, now - ts))
        if to_cancel and self.log_ultra_detallado:
            logger.info("[TIMEOUT] Verificando órdenes pendientes: órdenes que exceden timeout")
        for item in to_cancel:
            cl_ord_id = item[0] if isinstance(item, (list, tuple)) else item
            leg = item[1] if isinstance(item, (list, tuple)) and len(item) > 1 else "?"
            par_i = item[2] if isinstance(item, (list, tuple)) and len(item) > 2 else "?"
            age = item[3] if isinstance(item, (list, tuple)) and len(item) > 3 else self.order_timeout_seconds
            if now - self._last_timeout_log >= 10:
                logger.warning(f"[TIMEOUT] Orden {cl_ord_id} (Par {par_i} {leg.upper()}) pendiente hace {age:.0f}s > máx {self.order_timeout_seconds}s - solicitando cancelación")
                self._last_timeout_log = now
            self._cancel_order(cl_ord_id)

    def _reconnect_websocket(self, delay: float = 5.0):
        """Reconectar WebSocket y re-suscribir (como bot_ggal_futures_primary)."""
        if self.websocket_reconnect_attempts >= self.max_reconnect_attempts:
            logger.error(f"[WS] Máximo de reconexiones alcanzado ({self.max_reconnect_attempts})")
            return
        self.websocket_reconnect_attempts += 1
        logger.info(f"[WS] Reconectando (intento {self.websocket_reconnect_attempts}/{self.max_reconnect_attempts})...")
        try:
            time.sleep(delay)
            try:
                pyRofex.close_websocket_connection()
                time.sleep(1)
            except Exception:
                pass
            pyRofex.init_websocket_connection(
                market_data_handler=self._market_data_handler,
                order_report_handler=self._order_report_handler,
                error_handler=self._error_handler,
                exception_handler=self._exception_handler
            )
            time.sleep(2)
            all_tickers = self._build_all_tickers()
            pyRofex.market_data_subscription(
                tickers=all_tickers,
                entries=[
                    pyRofex.MarketDataEntry.BIDS,
                    pyRofex.MarketDataEntry.OFFERS,
                    pyRofex.MarketDataEntry.LAST
                ],
                depth=max(1, self.market_data_depth)
            )
            self._subscription_sent = True
            time.sleep(1)
            pyRofex.order_report_subscription()
            self.websocket_connected = True
            self.last_market_data_time = time.time()
            self.websocket_reconnect_attempts = 0
            # Invalidar datos de las 2 patas ARS para no usar datos pre-reconexión
            if self.modo_arbitraje_plazos:
                for t in [self.ars_ci, self.ars_24hs]:
                    if t and t in self.market_data_by_ticker:
                        self.market_data_by_ticker[t] = {**self.market_data_by_ticker[t], 'timestamp': 0}
                self._arbitrage_two_legs_ready_logged = False
                logger.info("[WS] Reconectado y re-suscrito (solo ARS). Datos 2 patas invalidados hasta próximo MD.")
            else:
                logger.info("[WS] Reconectado y re-suscrito")
        except Exception as e:
            logger.error(f"[WS] Error reconectando: {e}")

    def _check_websocket_health(self):
        """Si no llega market data en websocket_timeout_seconds, reconectar."""
        if not self.websocket_connected or not self._subscription_sent:
            return
        if self.last_market_data_time is None:
            return
        if time.time() - self.last_market_data_time > self.websocket_timeout_seconds:
            logger.warning(f"[WS] Sin market data por {self.websocket_timeout_seconds}s - reconectando...")
            self.websocket_connected = False
            self._reconnect_websocket()

    def _place_order(
        self,
        ticker: str,
        side: str,
        price: float,
        size: int,
        time_in_force: Optional[str] = None,
    ) -> Optional[Dict]:
        """Envía una orden limit Primary. time_in_force: DAY | FOK | IOC (evitar pierna coja en arbitraje)."""
        tif = time_in_force or 'DAY'
        tif_map = {
            'DAY': pyRofex.TimeInForce.DAY,
            'IOC': getattr(pyRofex.TimeInForce, 'ImmediateOrCancel', pyRofex.TimeInForce.DAY),
            'FOK': getattr(pyRofex.TimeInForce, 'FillOrKill', pyRofex.TimeInForce.DAY),
        }
        py_tif = tif_map.get(tif.upper(), pyRofex.TimeInForce.DAY)
        if self.log_ultra_detallado:
            logger.info(f"[ORDEN] Enviando orden: ticker={_short_label(ticker)} side={side} size={size} price={price:.2f} TIF={tif}")
        try:
            order = pyRofex.send_order(
                ticker=ticker,
                side=pyRofex.Side.BUY if side == 'BUY' else pyRofex.Side.SELL,
                size=size,
                price=price,
                order_type=pyRofex.OrderType.LIMIT,
                time_in_force=py_tif
            )
            if order and order.get('status') == 'OK':
                ord_inner = order.get('order', order)
                cl_ord_id = ord_inner.get('clientId') or ord_inner.get('clOrdId')
                order_id = ord_inner.get('orderId')
                if self.log_ultra_detallado:
                    logger.info(f"[ORDEN] Respuesta: status=OK | orderId={order_id} clOrdId={cl_ord_id}")
                return order
        except Exception as e:
            logger.error(f"[ORDEN] Error: {ticker} {side} {size}@{price}: {e}")
        return None

    def _order_report_handler(self, message):
        now = time.time()
        if self.log_ultra_detallado:
            logger.info("[FUNC_CALL] [ORDER_REPORT] _order_report_handler() LLAMADO")
        try:
            if message.get('type') != 'OR':
                return
            report = message.get('orderReport', message)
            cl_ord_id = report.get('clOrdId') or report.get('clientId')
            order_id = report.get('orderId') or (report.get('order') or {}).get('orderId')
            status_raw = report.get('status') or report.get('order', {}).get('status')
            status = (status_raw or "").upper().replace("PARTIAL", "PARTIALLY_FILLED").replace("COMPLETED", "FILLED")
            if not status:
                status = status_raw or "UNKNOWN"
            text = report.get('text', '') or report.get('order', {}).get('text', '')
            if not cl_ord_id:
                if self.log_ultra_detallado:
                    logger.warning("[ORDER_REPORT] Mensaje sin clOrdId, ignorando")
                return
            pair_leg = self.order_to_pair_leg.get(cl_ord_id)
            if pair_leg is None:
                if self.log_ultra_detallado:
                    logger.debug(f"[ORDER_REPORT] clOrdId={cl_ord_id} no está en order_to_pair_leg, ignorando")
                if status in ('FILLED', 'CANCELLED', 'REJECTED'):
                    self.order_cancel_pending.discard(cl_ord_id)
                return
            i, leg = pair_leg
            old_status = self.order_states.get(cl_ord_id, 'UNKNOWN')
            self.order_states[cl_ord_id] = status
            order_id_str = f" orderId={order_id}" if order_id else ""
            logger.info(f"[ORDER] clOrdId={cl_ord_id}{order_id_str}: {old_status} -> {status} | {text or '-'}")
            if self.log_ultra_detallado:
                logger.info(f"[ORDER_STATUS] Procesando cambio de estado: {old_status} -> {status}")

            if status in ('FILLED', 'COMPLETED', 'PARTIALLY_FILLED') or (status_raw and 'Partial' in str(status_raw)):
                executed = report.get('lastQty') or report.get('cumQty') or report.get('order', {}).get('lastQty') or report.get('size', 0)
                price_exec = report.get('avgPx') or report.get('lastPx') or report.get('price') or report.get('order', {}).get('price')
                leaves_qty = report.get('leavesQty') or report.get('order', {}).get('leavesQty')
                if self.log_ultra_detallado:
                    logger.info(f"[EJECUCION] Ejecución detectada en orden {cl_ord_id} (Par {i} {leg.upper()}):")
                    logger.info(f"   - Cantidad ejecutada: {executed} | Precio: {price_exec} | Remanente (leavesQty): {leaves_qty}")
                if executed and price_exec is not None:
                    monto = abs(float(price_exec) * float(executed))
                    if leg == 'usd':
                        self.operado += monto
                    else:
                        self.stock_operado += abs(int(executed))
                    # Registrar fill para esta pata y, si ambas patas llenas, loguear ganancia realizada
                    op = self._last_operation_by_par.get(i)
                    if op and (now - op['time']) < 300:
                        op['fills'][leg] = (float(executed), float(price_exec))
                        if 'ars' in op['fills'] and 'usd' in op['fills']:
                            qty_ars, px_ars = op['fills']['ars']
                            qty_usd, px_usd = op['fills']['usd']
                            if qty_usd and px_usd and px_usd > 0:
                                implied_exec = px_ars / px_usd
                                cot_lim = op['cotizacion_limite']
                                qty = op['qty']
                                if self.accion == 'compra':
                                    ganancia_pesos = (cot_lim - implied_exec) * qty
                                else:
                                    ganancia_pesos = (implied_exec - cot_lim) * qty
                                logger.info(
                                    f"[GANANCIA] Par {i}: ejecutado a {implied_exec:.2f} (tope/piso={cot_lim:.2f}) "
                                    f"→ {'ahorro' if self.accion == 'compra' else 'extra'} {ganancia_pesos:,.0f} pesos por {qty} nominales"
                                )
                            self._last_operation_by_par.pop(i, None)
                logger.info(
                    f"[ORDER] {cl_ord_id} par {i} {leg.upper()}: {status} qty={executed} px={price_exec} | operado USD={self.operado:.0f} nominales={self.stock_operado:.0f}"
                )
                if status == 'FILLED' or status == 'COMPLETED':
                    if self.log_ultra_detallado:
                        logger.info(f"[ORDER_STATUS] [OK] Orden COMPLETADA (FILLED) - Par {i} {leg.upper()}")
            elif status == 'REJECTED':
                logger.error(f"[ORDER_STATUS] [ERROR] Orden rechazada por el mercado: {text}")
                if self.modo_arbitraje_plazos:
                    self._cancel_other_legs_same_round(cl_ord_id, i, leg, now)
                if self.log_ultra_detallado:
                    logger.error(f"[ORDER_STATUS] Acción: Limpiando orden de tracking y permitiendo nueva orden")
            elif status == 'CANCELLED':
                logger.info(f"[ORDER_STATUS] [CANCELLED] Orden cancelada | Motivo: {text or '-'}")
                if self.modo_arbitraje_plazos:
                    self._cancel_other_legs_same_round(cl_ord_id, i, leg, now)
                if self.log_ultra_detallado:
                    logger.info(f"[ORDER_STATUS] [OK] Confirmación de cancelación recibida - permitiendo nuevas órdenes")
            elif status == 'NEW' or status == 'PENDING_NEW':
                if self.log_ultra_detallado:
                    logger.info(f"[ORDER_STATUS] [{'PENDING_NEW' if status == 'PENDING_NEW' else 'NEW'}] Orden {'en cola' if status == 'PENDING_NEW' else 'aceptada'} en el mercado - esperando ejecución")

            if status in ('FILLED', 'COMPLETED', 'CANCELLED', 'REJECTED'):
                self.order_cancel_pending.discard(cl_ord_id)
                self.order_to_pair_leg.pop(cl_ord_id, None)
                self.order_states.pop(cl_ord_id, None)
                if leg == 'ars':
                    self.ordenes_data_ars.get(i, {}).pop(cl_ord_id, None)
                else:
                    self.ordenes_data_usd.get(i, {}).pop(cl_ord_id, None)
        except Exception as e:
            logger.error(f"[ORDER REPORT] {e}")

    def run(self, duration_seconds: Optional[float] = None):
        """Mantiene el proceso vivo. Evaluación periódica cada ejecucion_interval_seconds (no solo al recibir MD). Health check cada 30s."""
        logger.info("[RUN] Bot canje MEP corriendo (Ctrl+C para detener)")
        logger.info(f"[INTERVALOS] Análisis (resumen) cada {self.analisis_interval_seconds:.0f}s | Evaluar/ejecutar cada {self.ejecucion_interval_seconds:.1f}s (timer + MD)")
        last_health = time.time()
        last_eval = 0.0
        try:
            if duration_seconds and duration_seconds > 0:
                deadline = time.time() + duration_seconds
                while self.running and time.time() < deadline:
                    time.sleep(1)
                    now = time.time()
                    if now - last_eval >= self.ejecucion_interval_seconds:
                        last_eval = now
                        self._last_ejecucion_time = now
                        self._evaluate_pairs()
                        if self.running:
                            self._check_order_timeout()
                    if now - last_health >= 30:
                        self._check_websocket_health()
                        last_health = now
            else:
                while self.running:
                    time.sleep(1)
                    now = time.time()
                    if now - last_eval >= self.ejecucion_interval_seconds:
                        last_eval = now
                        self._last_ejecucion_time = now
                        self._evaluate_pairs()
                        if self.running:
                            self._check_order_timeout()
                    if now - last_health >= 30:
                        self._check_websocket_health()
                        last_health = now
        except KeyboardInterrupt:
            pass
        self.running = False
        logger.info("[STOP] Bot detenido")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*60)
    print("  OMS - Desarbitraje AL30 CI vs 24hs (Primary API)")
    print("  Todo en ARS (especie en ARS) | Teórico en ARS del bono llevado a 24hs | CAUCIÓN 1 DÍA")
    print("="*60)
    # Instrumento fijo AL30. 4 tickers: AL30/AL30D - CI y AL30/AL30D - 24hs.
    # Condición: (TC_24hs - TC_CI) > (Costo_Caución_1día + Comisiones + Slippage).
    config = {
        'instrument': 'AL30',  # Fijo: solo AL30 CI y AL30 24hs
        'modo_arbitraje_plazos': True,
        'suffix_ci': ' - CI',
        'suffix_24hs': ' - 24hs',
        'pairs': [],  # se construyen: (AL30/AL30D CI), (AL30/AL30D 24hs)
        'accion': 'compra',
        'cotizacion': 1300,
        'efectivo': 100000,
        'stock': 1000,
        'nominales_maximo': 50,
        'porcentaje_efectivo': 0.9,
        'porcentaje_stock': 0.8,
        'tiempo_espera': 2.0,
        'use_promocion_eco': False,
        'size_tick': 1,
        'price_size_usd': 100.0,
        'contract_multiplier': 1,
        # Valor tiempo del dinero: caución 1 día EN PESOS (CAAP1D) desde Primary (tomadora/colocadora)
        'use_caucion_primary': True,
        'caucion_refresh_seconds': 30.0,
        'caucion_ticker_1d': None,  # None = resolver CAAP1D (caución en pesos 1 día)
        'tasa_caucion_tomadora_pct_anual': 0.0,
        'tasa_caucion_colocadora_pct_anual': 0.0,
        'dias_entre_plazos': 1.0,
        'slippage_estimado_pct': 0.05,
        'time_in_force_arbitrage': 'FOK',
        'min_profundidad': 5,
        'max_profundidad': None,
        'max_data_age_seconds': 15.0,
        'max_spread_pct': 1.5,
        'log_resumen_interval_seconds': 30.0,
        'tickers_referencia_dolar': [],  # solo AL30, sin GD30
        'par_referencia_mep': ('AL30 - CI', 'AL30D - CI'),
        'comparar_con_referencia_mep': False,
        'par_referencia_ccl': None,
        'comparar_con_referencia_ccl': False,
        'check_balance_cuenta': True,
        'balance_check_interval_seconds': 60.0,
        'order_timeout_seconds': 60.0,
        'websocket_timeout_seconds': 90.0,
        'max_reconnect_attempts': 5,
        'log_ultra_detallado': True,
    }
    bot = CanjeMEPPrimary(config)
    if bot.initialize():
        bot.run()
    else:
        print("[ERROR] No se pudo inicializar")


if __name__ == "__main__":
    main()
