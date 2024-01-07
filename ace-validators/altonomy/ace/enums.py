from enum import Enum


class DealProcessingStatus(str, Enum):

    Pending = "pending"
    Confirmed = "confirmed"
    Processing = "processing"
    Settled = "settled"
    Cancelled = "cancelled"


class FeedType(str, Enum):

    Cash = "Cash"
    PV = "PV"


class SystemSource(str, Enum):

    XALPHA = "XAL"
    MANUAL = "MAN"


class CompCode(str, Enum):

    FX_SPOT_BASE = "1001"
    FX_SPOT_QUOTE = "1002"
    FX_SPOT_FEE = "1003"

    EXECUTION_START = "2001"
    EXECUTION_END = "2002"
    EXECUTION_FEE = "2003"

    EXECUTION_CASHFLOW_START = "2011"
    EXECUTION_CASHFLOW_END = "2012"
    EXECUTION_CASHFLOW_FEE = "2013"
    EXECUTION_CASHFLOW_TRANSFER = "2014"

    # currently unused
    # DEPOSIT_NOM = "3001"
    # DEPOSIT_INTEREST = "3002"
    # DEPOSIT_INIT = "3003"
    # LOAN_NOM = "4001"
    # LOAN_INTEREST = "4002"
    # LOAN_INIT = "4003"

    FUTURES_BASE = "3001"
    FUTURES_QUOTE = "3002"
    FUTURES_FEE = "3003"

    OPTIONS_PREMIUM = "4001"
    OPTIONS_NOTIONAL = "4002"
    OPTIONS_FEE = "4003"
    OPTIONS_SPOT_EXERCISE_BASE = "4004"
    OPTIONS_SPOT_EXERCISE_QUOTE = "4005"

    CASHFLOW_TRANSFER = "5001"
    CASHFLOW_MM_FEE = "5002"
    CASHFLOW_REFERRAL_FEE = "5003"
    CASHFLOW_TRANSACTION_FEE = "5004"
    CASHFLOW_PNL_DIVIDENDING = "5005"
    CASHFLOW_MM_PROFIT_SHARE = "5006"
    CASHFLOW_NON_TRADING_EXPENSE = "5007"
    CASHFLOW_INTERCO_LOAN = "5008"
    CASHFLOW_INTERCO_RETURN = "5009"
    CASHFLOW_FUNDING = "5010"
    CASHFLOW_ETC = "5011"
    CASHFLOW_BUSINESS_PNL = "5012"
    CASHFLOW_OTHER_INCOME = "5013"
    CASHFLOW_OTHER_EXPENSE = "5014"
    CASHFLOW_INVESTMENTS = "5015"
    CASHFLOW_FUNDING_FEE = "5016"
    CASHFLOW_INSURANCE_CLEAR = "5017"

    CASHFLOW_NFT_BID_ASK = "5018"
    CASHFLOW_NFT_TOKEN = "5019"
    CASHFLOW_NFT_SERVICE_FEE = "5020"

    INITIAL_MARGIN_IN = "6001"
    INITIAL_MARGIN_OUT = "6002"
    VARIATION_MARGIN = "6003"


class DealType(str, Enum):

    FX_SPOT = "FX Spot"
    EXECUTION = "Execution"
    CASHFLOW = "Cash Flow"
    FUTURES = "Futures"
    # currently unused
    # LOAN_AND_DEPOSIT = "Loan & Deposit"
    OPTIONS = "Options"


class RecordType(str, Enum):

    CREATE = "CREATE"
    DELETE = "DELETE"


class TransferType(str, Enum):

    TRADE = "trade"
    TRANSFER = "transfer"


class Product(str, Enum):

    FX_SPOT = "FX Spot"
    EXECUTION = "Execution"
    CASHFLOW = "Cash Flow"

    # FUTURES = "Futures"
    # LOAN_AND_DEPOSIT = "Loan & Deposit"
    # OPTIONS = "Options"


class CashFlowPurpose(str, Enum):

    TRANSFER = "transfer"
    MM_FEE = "mm fee"
    REFERRAL_FEE = "referral fee"
    TRANSACTION_FEE = "transaction fee"
    PNL_DIVIDENDING = "p&l dividending"
    MM_PROFIT_SHARE = "mm profit share"
    NON_TRADING_EXPENSE = "non-trading expense"
    INTERCO_LOAN = "interco-loan"
    INTERCO_RETURN = "interco-return"
    FUNDING = "funding"
    OTHERS = "others"
    ETC = "etc"
    BUSINESS_PNL = "business pnl"
    OTHER_INCOME = "other income"
    OTHER_EXPENSE = "other expense"
    INVESTMENTS = "investments"

    EXECUTION_START = "execution start"
    EXECUTION_END = "execution end"
    EXECUTION_FEE = "execution fee"
    EXECUTION_TRANSFER = "execution transfer"

    FUNDING_FEE = "trade funding fee"
    INSURANCE_CLEAR = "trade insurance clear fee"

    NFT_BID_ASK = "nft bid ask price"
    NFT_TOKEN = "nft token"
    NFT_SERVICE_FEE = "nft service fee"

    VARIATION_MARGIN = "variation margin"


class SystemRemark(str, Enum):

    NEW = "NEW"
    REVERSAL = "REVERSAL"
    AMEND = "AMEND"


class OptionExpiryStatus(str, Enum):
    OPEN = "open"
    NOT_EXERCISED = "not_exercised"
    EXERCISED = "exercised"


class OptionSettlementCCY(str, Enum):
    INVERSE = "inverse"
    VANILLA = "vanilla"


class OptionSettlementMethod(str, Enum):
    PHYSICAL = "physical"
    CASH = "cash"


class OptionInitialMarginDirection(str, Enum):
    SEND = "send"
    RECEIVE = "receive"
