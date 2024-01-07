# Altonomy Ace API
![coverage-badge](/tests/test_helpers/coverage.svg)

## Install
```sh
pip install -U altonomy-ace-services --extra-index-url=https://pypi.altono.me
```

## Script
```sh
#!/bin/bash
# file: run_ace_service.sh

kill -15 $(cat ace_pid.txt)
nohup uvicorn altonomy.ace.main:app --port=8022 >> ace_log.out &
echo $! > ace_pid.txt
```

## Config
```ini
[Server]
ENDPOINT = <ALT_CLIENT_ENDPOINT>
XALPHA_EP = <XALPHA_API_ENDPOINT>
NITRO_EP = <NITRO_API_ENDPOINT>
ACE_EP = <ACE_API_ENDPOINT>

[AceDB]
DB_USERNAME = <DB USER>
DB_PASSWORD = <DB PASSWORD>
DB_HOSTNAME = <DB HOST>
DB_INSTNAME = <DA NAME>
DB_HOSTNAME_LOCAL = <Optional:DB_HOST_LOCAL>

[AceVar]
OPTIMUS_USERNAME = <OPTIMUS_USERNAME>
OPTIMUS_PASSWORD = <OPTIMUS_PASSWORD>
LIVE_BALANCE_EXCEPTIONAL_ACCS = <LIVE_BALANCE_EXCEPTIONAL_ACCS>
LIVE_BALANCE_GROUP_BY = <LIVE_BALANCE_GROUP_BY>

[Slack]
ACE_TOKEN = <ACE_TOKEN>
LIVE_BALANCE_CHANNEL = <LIVE_BALANCE_CHANNEL>
```

## Setup DB
```sh
alembic init alembic
# then
# config alembic.ini
# update sqlalchemy.url = mysql+pymysql://root:root@localhost/Ace
```

```python
# In env.py
import inspect
import os
import sys
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
from altonomy.ace.models import Base
target_metadata = Base.metadata
```

```sh
alembic revision --autogenerate -m "Update Ace Table Schema"
alembic upgrade head
```

## Test
```sh
pytest -s tests/
```

## Start API
```sh
uvicorn altonomy.ace.main:app --reload --port=8022
```

## Check Code
```sh
flake8 --ignore=E501 altonomy/
```

## Build package
```sh
source .env/bin/activate
python -m pip install --upgrade pip setuptools wheel
python setup.py sdist bdist_wheel
```
