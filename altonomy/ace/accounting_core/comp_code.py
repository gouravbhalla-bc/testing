from enum import Enum


class CompCode(str, Enum):

    FX_SPOT_BASE = "1001"
    FX_SPOT_QUOTE = "1002"
    FX_SPOT_FEE = "1003"

    EXECUTION_START = "2001"
    EXECUTION_END = "2002"
    EXECUTION_FEE = "2003"

    DEPOSIT_NOM = "3001"
    DEPOSIT_INTEREST = "3002"
    DEPOSIT_INIT = "3003"
    LOAN_NOM = "4001"
    LOAN_INTEREST = "4002"
    LOAN_INIT = "4003"

    CASHFLOW_TRANSFER = "5001"


class DealType(str, Enum):

    FX_SPOT = "FX Spot"
    EXECUTION = "Execution"
    CASHFLOW = "Cash Flow"
    LOAN_AND_DEPOSIT = "Loan & Deposit"
    FUTURES = "Futures"
    OPTIONS = "Options"
