# ⚡ AL30 Arbitrage Bot — CI vs 24hs Settlement (Primary API / BYMA)

> Automated OMS that detects and executes settlement arbitrage on AL30 bonds — buying CI (same-day) and selling 24hs (next-day) when the implied overnight rate exceeds the repo (caución) rate.

---

## 🇬🇧 English

### Strategy

Exploits the pricing inefficiency between two settlement tranches of the same bond (AL30 in ARS):

```
Implied TNA = (bid_24hs / offer_CI − 1) × (365 / 1 day)

Signal: Execute if Implied TNA > Caución tomadora rate (CAAP1D)
```

When triggered: **Buy AL30-CI + Sell AL30-24hs** simultaneously via FOK/IOC orders.

### Architecture

```
Primary WebSocket (BYMA/MERV)
        │
        ▼
  L2 Order Book (BI/OF depth 5)
  AL30 - CI  │  AL30 - 24hs
        │
        ▼
  TNA Engine ──── CAAP1D rate (live from Primary)
        │
        ▼
  Signal → Dual-leg execution (FOK/IOC)
        │
        ▼
  Order Manager + Fill tracker + P&L logger
```

### Features

| Module | Description |
|---|---|
| **TNA engine** | Computes implied overnight ARS rate from L2 book spread |
| **CAAP1D feed** | Live caución rate from Primary (refreshed every 30s) |
| **Dual-leg OMS** | Simultaneous FOK/IOC orders on both settlement tranches |
| **Commission model** | ECO Valores tarifario: 0.49% / 0.19% (Club promo) + 0.01% market rights |
| **Fill tracker** | Per-pair fill reconciliation with realized P&L logging |
| **Auto-reconnect** | WebSocket health check every 30s, up to 5 reconnect attempts |
| **Staleness guard** | Rejects market data older than 15s |

### Risk controls

- Max spread filter (`max_spread_pct`) — rejects crossed or illiquid books
- Min depth filter (`min_profundidad`) — requires minimum book depth before firing
- Order timeout (`order_timeout_seconds`) — cancels unexecuted legs automatically
- Slippage estimate baked into signal threshold

### Configuration (`.env`)

```
PRIMARY_USERNAME=your_user
PRIMARY_PASSWORD=your_password
PRIMARY_ACCOUNT=your_account
```

### Key parameters

```python
config = {
    'instrument': 'AL30',
    'time_in_force_arbitrage': 'FOK',   # FOK or IOC
    'caucion_ticker_1d': 'CAAP1D',      # Live repo rate
    'slippage_estimado_pct': 0.05,
    'max_spread_pct': 1.5,
    'max_data_age_seconds': 15.0,
    'min_profundidad': 5,
    'use_promocion_eco': False,          # Toggle commission tier
}
```

### Skills demonstrated

- Fixed income arbitrage between settlement tranches (CI / 24hs)
- Live L2 order book processing via Primary WebSocket
- Implied rate calculation from bid/offer spread
- Dual-leg atomic execution with FOK/IOC
- Real-time commission-adjusted signal thresholding
- Order lifecycle management (NEW → FILLED / CANCELLED / REJECTED)
- Realistic P&L attribution per arbitrage round

---

## 🇦🇷 Español

### Estrategia

Explota la ineficiencia de precio entre dos plazos de liquidación del mismo bono (AL30 en ARS):

```
TNA implícita = (bid_24hs / offer_CI − 1) × (365 / 1 día)

Señal: Ejecutar si TNA implícita > tasa caución tomadora (CAAP1D)
```

Al dispararse: **Comprar AL30-CI + Vender AL30-24hs** simultáneamente con órdenes FOK/IOC.

### Controles de riesgo

- Filtro de spread máximo — rechaza libros cruzados o ilíquidos
- Filtro de profundidad mínima — exige profundidad de book antes de disparar
- Timeout de órdenes — cancela patas no ejecutadas automáticamente
- Slippage estimado incorporado al umbral de señal

### Modelo de comisiones

ECO Valores (Bonos/Letras/ONs):
- Sin promoción: 0.49% por pata
- Con Club: 0.19% por pata
- Derechos de mercado: 0.01%
- Round-trip: 2 patas → ajuste automático en el umbral

### Skills que demuestra

- Arbitraje de renta fija entre plazos de liquidación (CI / 24hs)
- Procesamiento de book L2 en tiempo real vía WebSocket Primary
- Cálculo de tasa implícita desde spread bid/offer
- Ejecución atómica de dos patas con FOK/IOC
- Umbral de señal ajustado por comisiones en tiempo real
- Gestión del ciclo de vida de órdenes (NEW → FILLED / CANCELLED / REJECTED)
- Atribución de P&L realizado por ronda de arbitraje

---

## Author

**unabomber1618** · [github.com/unabomber1618](https://github.com/unabomber1618)

> *Built for live execution on BYMA fixed income markets — AL30 settlement arbitrage (CI vs 24hs).*
