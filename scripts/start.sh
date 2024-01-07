#!/bin/bash

process="$1"

if [ "$process" = "alt_ace_2" ]; then
  python -m altonomy.ace.v2.ace_client $2
elif [ "$process" = "alt_athena_2" ]; then
  python -m altonomy.ace.v2.athena_client ${@:2}
elif [ "$process" = "alt_athena_update_snapshots" ]; then
  python -m altonomy.ace.v2.adhoc.update_snapshots ${@:2}
elif [ "$process" = "elwood_process_transfer" ]; then
  python -m altonomy.ace.elwood_process_transfer
elif [ "$process" = "ems_client" ]; then
  python -m altonomy.ace.v2.ems_client $2
elif [ "$process" = "send_balance" ]; then
  python -m altonomy.ace.console_scripts.live_balance_snapshot
else
  uvicorn altonomy.ace.main:app --port=8022 --host 0.0.0.0
fi
