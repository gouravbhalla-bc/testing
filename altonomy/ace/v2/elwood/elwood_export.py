from altonomy.ace.models import ElwoodExport
from altonomy.ace.v2.trade.models.trade_v2 import TradeV2
from altonomy.ace.v2.elwood.models.deal import Deal
from altonomy.ace.v2.elwood.models.elwood_deal import ElwoodDeal


# 8002
def get_agency(deal: ElwoodDeal, export_num: int) -> ElwoodExport:
    export = None
    if export_num == 1:
        export = get_elwood_export_agency_1(deal)
        export.export = export_num
    elif export_num == 2:
        export = get_elwood_export_agency_2(deal)
        export.export = export_num
    return export


# 8002, 8839
def get_agency_trade(trade: TradeV2, deal: Deal, export_num: int) -> ElwoodExport:
    export = None
    if export_num == 3:
        export = get_elwood_export_agency_3(trade, deal)
        export.export = export_num
    return export


# 8000, 8838
def get_principle(trade: TradeV2, deal: Deal, export_num: int) -> ElwoodExport:
    export = None
    if export_num == 1:
        export = get_elwood_export_principle_1(trade, deal)
        export.export = export_num
    elif export_num == 2:
        export = get_elwood_export_principle_2(trade, deal)
        export.export = export_num
    elif export_num == 3:
        export = get_elwood_export_principle_3(trade, deal)
        export.export = export_num
    elif export_num == 4:
        export = get_elwood_export_principle_4(trade, deal)
        export.export = export_num
    return export


def get_elwood_export_agency_1(deal: ElwoodDeal) -> ElwoodExport:
    export_1 = get_elwood_export_agency(deal)
    export_1.buy_sell = 'BUY'
    if deal.start_asset_amount is not None and deal.start_asset_amount != 0:
        export_1.price = abs((deal.end_asset_amount - deal.fee_amount) / deal.start_asset_amount)
    # export_1.cacl_fee = True if export_1.fee != 0 else False
    export_1.unique_id = f'{export_1.unique_id}_SYN'
    export_1.counterparty = get_counterparty(export_1.exchange, 'hedge')
    return export_1


def get_elwood_export_agency_2(deal: ElwoodDeal) -> ElwoodExport:
    export_2 = get_elwood_export_agency(deal)
    export_2.buy_sell = 'SELL'
    if deal.start_asset_amount is not None and deal.start_asset_amount != 0:
        export_2.price = abs(deal.end_asset_amount / deal.start_asset_amount)
    # export_2.cacl_fee = True if export_2.fee != 0 else False
    export_2.counterparty = get_counterparty(export_2.exchange, deal.counterparty_name)
    return export_2


def get_elwood_export_agency_3(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export_3 = get_elwood_export_principle(trade, deal)
    if trade.base_amount is not None and trade.quote_amount is not None and trade.base_amount != 0:
        export_3.price = abs(trade.quote_amount / trade.base_amount)
    if trade.fee_amount is not None:
        export_3.fee = trade.fee_amount * -1
        # export_3.cacl_fee = True if export_3.fee != 0 else False
    export_3.unique_id = f'A{trade.deal_id}'
    export_3.counterparty = get_counterparty(export_3.exchange, trade.counterparty_name)
    return export_3


# X-Alpha Export1 and Export2 8002
def get_elwood_export_agency(deal: ElwoodDeal) -> ElwoodExport:
    export = ElwoodExport()
    export.portfolio = deal.portfolio_number
    export.deal_id = deal.deal_id
    export.version = deal.version
    export.trade_date_time = deal.value_date
    export.exchange = 'otc'
    export.symbol = ''
    export.product_type = 'FXSpot'
    export.base = deal.start_asset
    export.quote = deal.end_asset
    export.instrument_expiry = None
    export.quantity = abs(deal.start_asset_amount)
    export.fee = 0
    export.cacl_fee = False
    export.fee_currency = deal.fee_asset
    export.book = get_book(deal.portfolio_number)
    export.account = get_account(deal.portfolio_number, export.exchange)
    export.strategy = deal.counterparty_name
    export.trader = deal.maker_id
    export.memo = deal.deal_ref
    export.unique_id = f'A{deal.deal_id}'
    export.counterparty = get_counterparty(export.exchange, deal.counterparty_name)
    export.effective_date_end = deal.valid_to
    return export


# TradeV2 8000
def get_elwood_export_principle_1(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export_1 = get_elwood_export_principle(trade, deal)
    if trade.base_amount is not None and trade.quote_amount is not None and trade.base_amount != 0:
        export_1.price = abs(trade.quote_amount / trade.base_amount)
    if trade.fee_amount is not None:
        export_1.fee = trade.fee_amount * -1
        # export_1.cacl_fee = True if export_1.fee != 0 else False
    export_1.strategy = trade.counterparty_name
    export_1.memo = f'{trade.deal_ref}{export_1.product_type}'
    export_1.counterparty = get_counterparty(export_1.exchange, trade.counterparty_name)
    return export_1


def get_elwood_export_principle_2(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export_2 = get_elwood_export_principle_3(trade, deal)
    export_2.strategy = trade.counterparty_name
    if trade.base_amount is not None:
        export_2.buy_sell = 'SELL' if trade.base_amount >= 0 else 'BUY'
    export_2.unique_id = f'P{trade.deal_id}_SYN1'
    return export_2


def get_elwood_export_principle_3(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export_3 = get_elwood_export_principle(trade, deal)
    export_3.price = deal.reference_price if deal is not None else None
    if trade.fee_amount is not None:
        export_3.fee = trade.fee_amount
        # export_3.cacl_fee = True if export_3.fee != 0 else False
    export_3.strategy = 'hedge'
    export_3.memo = f'{trade.deal_ref}{export_3.product_type}-SYN'
    export_3.unique_id = f'P{trade.deal_id}_SYN2'
    export_3.counterparty = get_counterparty(export_3.exchange, 'Trading Desk')
    return export_3


def get_elwood_export_principle_4(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export_4 = get_elwood_export_principle(trade, deal)
    if trade.base_amount is not None and trade.quote_amount is not None and trade.base_amount != 0:
        export_4.price = abs(trade.quote_amount / trade.base_amount)
    # export_4.price = deal.reference_price if deal is not None else None
    if trade.fee_amount is not None:
        export_4.fee = trade.fee_amount * -1
        # export_4.cacl_fee = True if export_4.fee != 0 else False
    export_4.strategy = 'hedge'
    export_4.memo = f'{trade.deal_ref}{export_4.product_type}'
    export_4.unique_id = f'P{trade.deal_id}'
    export_4.counterparty = get_counterparty(export_4.exchange, 'Trading Desk')
    return export_4


def get_elwood_export_principle(trade: TradeV2, deal: Deal) -> ElwoodExport:
    export = ElwoodExport()
    export.portfolio = trade.portfolio
    export.deal_id = trade.deal_id
    export.version = deal.version if deal is not None else None
    export.trade_date_time = trade.value_date
    if trade.base_amount is not None:
        export.buy_sell = 'BUY' if trade.base_amount >= 0 else 'SELL'
    export.exchange = get_exchange(trade)
    export.symbol = ''
    export.product_type = get_product_type(trade)
    export.base = trade.base_asset
    export.quote = trade.quote_asset
    export.instrument_expiry = None
    export.quantity = abs(trade.base_amount)
    export.fee_currency = trade.fee_asset
    export.cacl_fee = False
    export.book = get_book(trade.portfolio)
    export.account = get_account(trade.portfolio, export.exchange)
    export.strategy = trade.counterparty_name
    export.trader = deal.maker_id if deal is not None else None
    export.memo = f'{trade.deal_ref}{export.product_type}'
    export.unique_id = f'P{trade.deal_id}'
    export.effective_date_end = trade.effective_date_end
    return export


def get_product_type(trade: TradeV2) -> str:
    if trade is not None:
        if trade.product == 'FX Spot':
            return 'FXSpot'
        if trade.product == 'Futures':
            return 'Swap'
    return None


def get_exchange(trade: TradeV2) -> str:
    if trade is not None:
        if trade.product == 'FX Spot':
            return 'otc'
        if trade.product == 'Futures':
            # Binance Holdings Limited
            if trade.counterparty_ref == 'CID100695':
                if 'USDT' in trade.contract:
                    return 'binanceusdm'
                else:
                    return 'binancecoinm'
            # FTX Trading Ltd
            if trade.counterparty_ref == 'CID100586':
                return 'ftx'
    return None


def get_book(portfolio: str) -> str:
    if portfolio in ['8000', '8838']:
        return 'OTC Spot - Principal'
    elif portfolio in ['8002', '8839']:
        return 'OTC Spot - Agency'
    else:
        return None


def get_account(portfolio: str, elwood_exchange: str) -> str:
    if portfolio in ['8000', '8838']:
        if elwood_exchange == 'otc':
            return 'otc-blockchain-singapore'
        elif elwood_exchange == 'ftx':
            return 'ftx-otc-blockchain-singapore'
        elif elwood_exchange == 'binanceusdm':
            return 'binanceusdm-otc-blockchain-singapore'
        else:
            return f'{elwood_exchange}-otc'
    elif portfolio in ['8002', '8839']:
        return 'otc-blockchain-singapore'
    else:
        return None


def get_counterparty(exchange: str, counterparty: str) -> str:
    if exchange == 'otc':
        return counterparty
    else:
        return ''
