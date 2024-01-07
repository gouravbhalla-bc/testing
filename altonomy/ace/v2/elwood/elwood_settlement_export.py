from altonomy.ace.models import ElwoodSettlementExport
from altonomy.settlement_engine.endpoints.schemas import SettlementV2


def get_elwood_settlement(settlement: SettlementV2) -> ElwoodSettlementExport:
    export = ElwoodSettlementExport()
    export.export = 1
    export.settlement_id = settlement.settlement_id
    # export.version = settlement.version

    export.transaction_date_time = settlement.created_on
    export.created_date_time = settlement.created_on
    export.exchange = 'otc'
    export.symbol = settlement.asset
    export.type = 'withdrawal' if settlement.direction == 'outgoing' else 'deposit'
    export.quantity = abs(settlement.amount)
    export.fee = -(settlement.txn_fee)
    export.fee_reducing = False if settlement.txn_fee == 0 else True
    export.account = 'otc-blockchain-singapore'
    export.sub_account = f'otc-{settlement.settlement_account_name}' if settlement.settlement_account_name is not None else ''
    export.tx_hash = settlement.tx_id
    export.from_address = settlement.tx_addr_source
    export.to_address = settlement.tx_addr_destination
    export.note = 'residual' if settlement.other_reference == 'residual settlement' else settlement.counterparty_name
    export.external_id = settlement.settlement_ref
    export.strategy = settlement.counterparty_name
    export.book = 'Client balances'
    export.effective_date_end = settlement.valid_to
    return export
