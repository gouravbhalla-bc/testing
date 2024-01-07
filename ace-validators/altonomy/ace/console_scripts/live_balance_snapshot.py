import time
from typing import List
from slack import WebClient
from slack.errors import SlackApiError
from altonomy.ace import config
from altonomy.ace.external.optimus_client import OptimusClient
from altonomy.ace.v2.ems.ems_ctrl import EMSCtrl
import pandas as pd
import dataframe_image as dfi
# from tabulate import tabulate


class LiveBalanceSnapshot(object):

    def __init__(self):
        self.oc = OptimusClient()
        self.ec = EMSCtrl(optimus_client=self.oc)
        self.token = config.ACE_TOKEN
        self.channel = config.LIVE_BALANCE_CHANNEL
        self.exceptional_accs = config.LIVE_BALANCE_EXCEPTIONAL_ACCS
        self.exceptional_exchange = config.LIVE_BALANCE_EXCEPTIONAL_EXCHANGE
        self.group_by = config.LIVE_BALANCE_GROUP_BY
        self.file_name = f"balance_snapshot_{time.time()}.png"

    def get_balance_snapshot(self):
        err, accounts = self.oc.get_accounts()
        if err is not None:
            return err, None
        account_ids = set()
        for acc in accounts:
            nitro_account_id = acc.get("nitro_account_id", 0)
            exchange = acc.get("exchange", "")
            if nitro_account_id not in self.exceptional_accs and exchange not in self.exceptional_exchange:
                try:
                    nitro_account_id = int(nitro_account_id)
                    if nitro_account_id > 0:
                        account_ids.add(nitro_account_id)
                except BaseException:
                    pass
        account_ids = list(account_ids)
        group_by = self.group_by.split(",") if self.group_by is not None else []
        resp = self.ec.get_formated_account_balances(account_ids, group_by)
        return resp

    def format_snapshot_to_table(self, balances: List[dict]):
        _balances = []
        # format balances
        for balance in balances:
            _balances.append({
                "Function": balance.get("function", "NA") if balance.get("function") != "" else "NA",
                "Exchange": balance.get("exchange_name", ""),
                "Available ($)": "{:,}".format(round(balance.get("usd_available", 0), 2)),
                "Frozen ($)": "{:,}".format(round(balance.get("usd_frozen", 0), 2)),
                "Equity ($)": "{:,}".format(round(balance.get("usd_total", 0), 2)),
                "Principal ($)": "{:,}".format(round(balance.get("principal", 0), 2)),
                "Agency ($)": "{:,}".format(round(balance.get("agency", 0), 2)),
            })
        query_output = pd.DataFrame(_balances)
        results = query_output[
            ['Function', 'Exchange', 'Available ($)', 'Frozen ($)', 'Equity ($)', 'Principal ($)', 'Agency ($)']]
        aligned = results.style.set_properties(**{'text-align': 'center'})
        dfi.export(aligned, self.file_name, table_conversion='matplotlib')
        return results

    def send_balance_snapshot(self):
        balances = self.get_balance_snapshot()
        table_df = self.format_snapshot_to_table(balances)
        slack_client = WebClient(token=self.token)
        message = 'Institution Balance Snapshot'
        try:
            result = slack_client.files_upload(
                channels=self.channel,
                title=message,
                file=self.file_name,
            )
        except SlackApiError as e:
            print("[Error]", e)
            try:
                result = slack_client.chat_postMessage(
                    channel=self.channel,
                    # text=tabulate(table_df, tablefmt="grid")
                    text=f"```{table_df.to_string()}```"

                )
                print(result)
            except SlackApiError as e:
                print("[Error]", e)

        # Send alert if exposure is greater than 30 miliion
        message = 'Exchange greater than 30M exposure:'
        for index in table_df.index:
            exposure = table_df['Equity ($)'][index].replace(",", "")
            exchange = table_df['Exchange'][index]
            if int(float(exposure)) > 30000000 and exchange.lower() != "total" and self.group_by == "exchange":
                message += f"\n {exchange} has exposure greater than 30M."
        print(message)
        try:
            if len(message.split("\n")) > 1:
                result = slack_client.chat_postMessage(
                    channel=self.channel,
                    # text=tabulate(table_df, tablefmt="grid")
                    text=message
                )
                print(result)
        except SlackApiError as e:
            print("[Error]", e)


def main():
    lbs = LiveBalanceSnapshot()
    lbs.send_balance_snapshot()


if __name__ == "__main__":
    main()
