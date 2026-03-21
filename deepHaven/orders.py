from deephaven import time_table
from deephaven import agg

_CUSIPS = ("`US912810TW54`,`US91282CKA53`,`US91282CKB37`,"
           "`US91282CKC10`,`US912797KV05`,`US912797KW87`,`US912797KX60`")
_DESCS  = ("`T 4.250 02/15/2054`,`T 4.375 02/15/2034`,`T 4.500 02/15/2029`,"
           "`T 4.625 02/15/2027`,`T 5.250 02/13/2025`,`T 5.200 05/15/2025`,"
           "`T 5.150 02/12/2026`")
_TENORS = "`30Y`,`10Y`,`5Y`,`2Y`,`3M`,`6M`,`1Y`"

# ─────────────────────────────────────────────────────────────────────────────
# 1-second clock — all age calculations depend on ClockTs, not currentTimeMillis
# ─────────────────────────────────────────────────────────────────────────────
_clock = time_table("PT1S").view(["ClockTs = Timestamp"])

# ─────────────────────────────────────────────────────────────────────────────
# Raw order stream
#
# TWO LEVERS for controlling active order count:
#
#   1. Order interval  PT1S  → new order every 1 second
#   2. FillDelayMillis 6000–12000 → each order lives 6–12 s (random, per order)
#
# Steady-state active ≈ avg(FillDelay) / order_interval
#                     ≈ 9s / 1s  = ~9 target, but random spread gives 5–12
#
# Tweak PT1S and the FillDelayMillis range to taste:
#   Fewer actives  → slower interval (PT2S) or shorter delay
#   More actives   → faster interval (PT500MS) or longer delay
#   More variance  → widen the random range (e.g. 4000–16000)
# ─────────────────────────────────────────────────────────────────────────────
_raw = (
    time_table("PT1S")                              # ← one order per second
    .update([
        "OrderId          = `ORD-` + String.format(`%06d`, ii)",
        "ClOrdId          = `CL-`  + String.format(`%08d`, ii)",
        "OrderTime        = Timestamp",

        # ── Per-order random fill delay (fixed at creation, never changes) ──
        # Range 6 000 – 12 000 ms → avg 9 s → with 1s interval → ~9 active
        # The random spread means orders don't all fill at the same moment,
        # creating the natural ebb-and-flow (5 → 3 → 6 → 4 …) the user wants
        "FillDelayMillis  = (long)(6_000 + Math.random() * 6_000)",
        "FillNanos        = epochNanos(OrderTime) + FillDelayMillis * 1_000_000L",

        "SecurityIndex    = (int)(ii % 7)",
        "CUSIP            = new String[]{" + _CUSIPS + "}[SecurityIndex]",
        "SecurityDesc     = new String[]{" + _DESCS  + "}[SecurityIndex]",
        "Tenor            = new String[]{" + _TENORS + "}[SecurityIndex]",
        "Side             = (ii % 2 == 0) ? `BUY` : `SELL`",
        "OrderType        = new String[]{`LIMIT`,`LIMIT`,`MARKET`,`IOC`,`GTC`}[(int)(ii % 5)]",
        "Quantity         = (long)((int)(Math.random() * 100 + 1) * 1_000_000)",
        "LimitPrice       = Math.round((98.0 + Math.random() * 4.0) * 32.0) / 32.0",
        "Yield            = Math.round((3.5  + Math.random() * 2.0) * 1000.0) / 1000.0",
        "SpreadBps        = (int)(Math.random() * 50 - 10)",
        "Venue            = new String[]{`TRADEWEB`,`BLOOMBERG`,`MARKETAXESS`,`DIRECT`,`FENICS`}[(int)(ii % 5)]",
        "Counterparty     = new String[]{`GS`,`JPM`,`MS`,`BARC`,`CITI`,`BofA`,`HSBC`,`DB`,`UBS`,`CS`}[(int)(Math.random() * 10)]",
        "Trader           = new String[]{`JSMITH`,`ADOE`,`MWONG`,`KPATEL`,`RJONES`}[(int)(ii % 5)]",
        "Book             = new String[]{`RATES-NY`,`RATES-LDN`,`RATES-TKY`,`RATES-HK`}[(int)(ii % 4)]",
        "Account          = new String[]{`PROP`,`CLIENT`,`HEDGE`,`MM`}[(int)(ii % 4)]",
        "Priority         = (int)(Math.random() * 5) + 1",
        "RoutingStrategy  = new String[]{`SMART`,`DIRECT`,`ALGO`,`MANUAL`}[(int)(ii % 4)]",
    ])
    .drop_columns("Timestamp")
)

# ─────────────────────────────────────────────────────────────────────────────
# snapshot_when: re-stamps every order row with ClockTs each second
# ─────────────────────────────────────────────────────────────────────────────
_live = _raw.snapshot_when(_clock, stamp_cols=["ClockTs"])

# ─────────────────────────────────────────────────────────────────────────────
# epochMillis(ClockTs) creates an explicit column dependency so the engine
# re-evaluates AgeMillis every second.  Each order uses its own FillDelayMillis
# so IsActive flips at a different time for every order → staggered REMOVE
# events from active_orders → natural fluctuation in row count.
# ─────────────────────────────────────────────────────────────────────────────
all_orders = (
    _live
    .update([
        "AgeMillis         = epochMillis(ClockTs) - epochMillis(OrderTime)",
        "IsActive          = AgeMillis < FillDelayMillis",   # ← per-order threshold
        "OrderStatus       = IsActive ? `ACTIVE` : `FILLED`",
        "FilledQty         = IsActive ? 0L        : Quantity",
        "RemainingQty      = IsActive ? Quantity  : 0L",
        "FillPct           = IsActive ? 0.0       : 100.0",
        "FillTime          = IsActive ? (java.time.Instant)null : epochNanosToInstant(FillNanos)",
        "SecondsRemaining  = IsActive ? (int)((FillDelayMillis - AgeMillis) / 1000) : 0",
        "LastUpdate        = ClockTs",
    ])
    .drop_columns("ClockTs")
)

# ─────────────────────────────────────────────────────────────────────────────
# active_orders: grows by 1 every second, shrinks when each order's personal
#                FillDelayMillis expires → row count fluctuates naturally
# filled_orders: grows indefinitely as orders complete
# ─────────────────────────────────────────────────────────────────────────────
active_orders = all_orders.where("IsActive")
filled_orders = all_orders.where("!IsActive")

# ─────────────────────────────────────────────────────────────────────────────
# Aggregations
# ─────────────────────────────────────────────────────────────────────────────
order_summary = all_orders.agg_by([
    agg.count_("OrderCount"),
    agg.sum_("TotalNotional = Quantity"),
    agg.avg("AvgYield       = Yield"),
    agg.avg("AvgPrice       = LimitPrice"),
], by=["OrderStatus", "Tenor"])

venue_summary = all_orders.agg_by([
    agg.count_("OrderCount"),
    agg.sum_("TotalNotional = Quantity"),
    agg.avg("AvgYield       = Yield"),
], by=["Venue", "OrderStatus"])

trader_summary = all_orders.agg_by([
    agg.count_("OrderCount"),
    agg.sum_("TotalNotional = Quantity"),
    agg.avg("AvgYield       = Yield"),
], by=["Trader", "Book", "OrderStatus"])
