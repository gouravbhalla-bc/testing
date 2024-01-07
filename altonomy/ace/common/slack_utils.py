from altonomy.ace import config
# from altonomy.ace.common.api_utils import get_user
# from slack import WebClient


slack_token = config.SLACK_TOKEN
slack_channel = config.OPTIMUS_CHANNEL
tasks_path = "ace_admin/#/admin/tasks"


def sendTaskNotificationToSlackChannel(token: str, maker_id: int, checker_id: int, channel: str = slack_channel):
    # if maker_id != checker_id:
    #     # if maker and checker not same, send slack notification
    #     maker_name = None
    #     checker_name = None
    #     err, resp = get_user(token, maker_id)
    #     if err is None:
    #         maker_name = resp.get("username")
    #     else:
    #         return err, None
    #     err, resp = get_user(token, checker_id)
    #     if err is None:
    #         checker_name = resp.get("username")
    #     else:
    #         return err, None
    #     msg_payload = f"{maker_name} has send a task to {checker_name}, please check:\n{config.EXTERNAL_EP}/{tasks_path}"
    #     client = WebClient(token=slack_token)
    #     response = client.chat_postMessage(channel=channel, text=msg_payload)
    #     # response = client.chat_postEphemeral(channel=channel, text=msg_payload, user=Slack_UID)
    #     if response["ok"]:
    #         return None, True
    #     else:
    #         return response.get("message", "slack api error"), None
    # else:
    #     # do not send notification
    #     return None, True
    return None, True
