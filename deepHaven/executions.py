from deephaven import time_table
from deephaven import empty_table
import random

# Create a ticking time table - 1 row per second
executions = time_table("PT1S").update([
    # Execution identifiers
    "ExecId = `EXE-` + String.format(`%06d`, ii)",
    "OrderId = `ORD-` + String.format(`%04d`, (int)(ii % 500))",

    # Instrument
    "Symbol = new String[]{`UST 2Y`, `UST 5Y`, `UST 10Y`, `UST 30Y`, `SOFR 3M`, `TIPS 10Y`}[(int)(ii % 6)]",
    "CUSIP = new String[]{`91282CJL6`, `91282CJM4`, `91282CJN2`, `912810TM0`, `91282CJP7`, `912810TP3`}[(int)(ii % 6)]",

    # Execution details
    "Side = (ii % 2 == 0) ? `BUY` : `SELL`",
    "Quantity = (int)(Math.round(Math.random() * 50 + 1) * 1_000_000)",
    "Price = 99.0 + Math.round(Math.random() * 400) / 128.0",
    "Yield = 3.5 + Math.round(Math.random() * 200) / 100.0",
    "Notional = Quantity * Price / 100.0",

    # Venue & counterparty
    "Venue = new String[]{`D2C`, `D2D`, `ECN`, `RFQ`, `CLOB`}[(int)(ii % 5)]",
    "Counterparty = new String[]{`GS`, `JPM`, `MS`, `BARC`, `CITI`, `BofA`, `HSBC`, `DB`}[(int)(Math.round(Math.random() * 7))]",

    # Status
    "ExecStatus = new String[]{`FILLED`, `FILLED`, `FILLED`, `PARTIAL`, `REJECTED`}[(int)(Math.round(Math.random() * 4))]",
    "Trader = new String[]{`JSMITH`, `ADOE`, `MWONG`, `KPATEL`}[(int)(ii % 4)]",
    "Book = new String[]{`RATES-NY`, `RATES-LDN`, `RATES-TKY`}[(int)(ii % 3)]",
])
