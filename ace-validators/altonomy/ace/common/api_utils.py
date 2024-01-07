import requests

from altonomy.ace import config
from typing import Any
from typing import List
# import urllib.parse


def get_status_code(message: str) -> int:
    if message in ["invalid token"]:
        return 403
    return 400


def get_jwt_payload(token: str, scopes: List[str]) -> Any:
    try:
        r = requests.post(
            config.ALT_CLIENT_ENDPOINT + "/auth_api/auth/verify",
            json={"token": token, "scopes": scopes},
        )
        result = r.json()
        err = result.get("detail", None)
        return err, result
    except BaseException:
        return "invalid token", None


def get_user(token: str, user_id: int):
    try:
        headers = {
            "Alt-Auth-Token": token
        }
        r = requests.get(
            config.ALT_CLIENT_ENDPOINT + "/auth_api/user/list/" + str(user_id),
            headers=headers
        )
        result = r.json()
        err = result.get("detail", None)
        return err, result
    except BaseException:
        return "invalid token", None


def get_users(token: str, tags: str = "altex:checker"):
    try:
        headers = {
            "Alt-Auth-Token": token
        }
        r = requests.get(
            config.ALT_CLIENT_ENDPOINT + "/auth_api/user/list",
            headers=headers,
            params={
                "tags": tags
            },
            verify=False
        )
        result = r.json()
        err = None
        if isinstance(result, dict):
            err = result.get("detail", None)
        return err, result
    except BaseException:
        return "invalid token", None


# def send_telegram_message(message):
#     token = config.TELEGRAM_TOKEN
#     chat_id = config.TELEGRAM_CHATID
#     message = urllib.parse.quote(message)
#     url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&parse_mode=HTML&text={message}"
#     response = requests.get(url)
#     if response.status_code == 200:
#         return None, "Request sent successfully"
#     else:
#         return "Error in sending request", None
