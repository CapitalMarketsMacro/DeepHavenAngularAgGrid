from deephaven import time_table
from deephaven.updateby import rolling_avg_time
from deephaven import merge
import random

# On-the-run Treasury securities (most recently issued)
OTR_SECURITIES = [
    ("US912810TW54", "T 4.250 02/15/2054", "30Y"),
    ("US91282CKA53", "T 4.375 02/15/2034", "10Y"),
    ("US91282CKB37", "T 4.500 02/15/2029", "5Y"),
    ("US91282CKC10", "T 4.625 02/15/2027", "2Y"),
    ("US912797KV05", "T 5.250 02/13/2025", "3M"),
    ("US912797KW87", "T 5.200 05/15/2025", "6M"),
    ("US912797KX60", "T 5.150 02/12/2026", "1Y"),
]

# Create a ticking time table - generates new orders every 2 seconds
all_orders = time_table("PT2S").update([
    # Order identifiers
    "OrderId = `ORD-` + String.format(`%06d`, ii)",
    "ClOrdId = `CL-` + String.format(`%08d`, System.currentTimeMillis() % 100000000 + ii)",

    # On-the-run Treasury instrument selection
    "SecurityIndex = (int)(ii % 7)",
    "CUSIP = new String[]{`US912810TW54`, `US91282CKA53`, `US91282CKB37`, `US91282CKC10`, `US912797KV05`, `US912797KW87`, `US912797KX60`}[SecurityIndex]",
    "SecurityDesc = new String[]{`T 4.250 02/15/2054`, `T 4.375 02/15/2034`, `T 4.500 02/15/2029`, `T 4.625 02/15/2027`, `T 5.250 02/13/2025`, `T 5.200 05/15/2025`, `T 5.150 02/12/2026`}[SecurityIndex]",
    "Tenor = new String[]{`30Y`, `10Y`, `5Y`, `2Y`, `3M`, `6M`, `1Y`}[SecurityIndex]",

    # Order details
    "Side = (ii % 2 == 0) ? `BUY` : `SELL`",
    "OrderType = new String[]{`LIMIT`, `LIMIT`, `MARKET`, `IOC`, `GTC`}[(int)(ii % 5)]",
    "Quantity = (int)(Math.round(Math.random() * 100 + 1) * 1_000_000)",
    "Price = 98.0 + Math.round(Math.random() * 400) / 128.0",
    "Yield = 4.0 + Math.round(Math.random() * 150) / 100.0",

    # Venue & trading info
    "Venue = new String[]{`TRADEWEB`, `BLOOMBERG`, `MARKETAXESS`, `DIRECT`, `FENICS`}[(int)(ii % 5)]",
    "Counterparty = new String[]{`GS`, `JPM`, `MS`, `BARC`, `CITI`, `BofA`, `HSBC`, `DB`, `UBS`, `CS`}[(int)(Math.round(Math.random() * 9))]",

    # Order status - starts as ACTIVE
    "OrderStatus = `ACTIVE`",
    "FilledQty = 0",
    "RemainingQty = Quantity",

    # Timestamps
    "OrderTime = Timestamp",
    "LastUpdateTime = Timestamp",

    # Trader info
    "Trader = new String[]{`JSMITH`, `ADOE`, `MWONG`, `KPATEL`, `RJONES`}[(int)(ii % 5)]",
    "Book = new String[]{`RATES-NY`, `RATES-LDN`, `RATES-TKY`, `RATES-HK`}[(int)(ii % 4)]",
    "Account = new String[]{`PROP`, `CLIENT`, `HEDGE`, `MM`}[(int)(ii % 4)]",

    # Priority & routing
    "Priority = (int)(Math.round(Math.random() * 5) + 1)",
    "RoutingStrategy = new String[]{`SMART`, `DIRECT`, `ALGO`, `MANUAL`}[(int)(ii % 4)]",
])

# Create active orders view - orders that are less than 4 seconds old
# After 4 seconds, orders are considered "filled" and removed from this view
active_orders = all_orders.where([
    "Timestamp - OrderTime < 'PT4S'"
]).update([
    "OrderStatus = `ACTIVE`",
    "TimeToFill = (int)((4000 - (System.currentTimeMillis() - epochMillis(OrderTime))) / 1000)",
    "TimeToFill = TimeToFill < 0 ? 0 : TimeToFill"
])

# Create filled orders view - orders that are 4+ seconds old
filled_orders = all_orders.where([
    "Timestamp - OrderTime >= 'PT4S'"
]).update([
    "OrderStatus = `FILLED`",
    "FilledQty = Quantity",
    "RemainingQty = 0",
    "FillTime = OrderTime + 'PT4S'",
    "LastUpdateTime = OrderTime + 'PT4S'"
])

# Combined view showing all orders with their current status
all_orders_status = all_orders.update([
    "OrderStatus = (Timestamp - OrderTime < 'PT4S') ? `ACTIVE` : `FILLED`",
    "FilledQty = (Timestamp - OrderTime >= 'PT4S') ? Quantity : 0",
    "RemainingQty = (Timestamp - OrderTime < 'PT4S') ? Quantity : 0",
    "AgeSeconds = (int)((System.currentTimeMillis() - epochMillis(OrderTime)) / 1000)"
])

# Summary statistics
order_summary = all_orders_status.agg_by([
    agg.count_("TotalOrders"),
    agg.sum_("TotalQuantity=Quantity"),
], by=["OrderStatus"])

