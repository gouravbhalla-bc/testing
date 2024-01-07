import json
import traceback

import requests
from altonomy.ace import config


def _post(path, **kwargs):
    url = f"{config.NITRO_EP}/{path}"
    print(f"POST {url} {kwargs}")
    return requests.post(url, **kwargs)


def get_asset_price(pairs, timestamp):
    path = "api/ticker/price"
    data = {
        "pairs": pairs,
        "timestamp": timestamp,
    }

    resp = None
    retries = 3

    print(f"Request {len(pairs)} pairs, timestamp: {timestamp}")

    while resp is None and retries > 0:
        try:
            resp = _post(path, data=json.dumps(data))
        except Exception:
            print(f"Error. Retrying {path}, retries left: {retries}")
            print(traceback.format_exc())

        retries -= 1

    if resp is None:
        print("No response")
        return {}

    try:
        return resp.json()
    except Exception:
        print("Failed to decode due to error.")
        print(traceback.format_exc())
        return {}
