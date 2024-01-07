import os
import requests
import sys

# The following may change based on environment
USERNAME = "FILL_ME"
PASSWORD = "FILL_ME"
AUTH_URL = "https://test-optimus.altono.xyz/auth_api/auth/login"
ACE_URL = "https://test-ace.altono.xyz/ace_api/athena/v2/manual-feeds"

# Calculated values
FILE_PATH = ""
JWT_TOKEN = ""

def get_jwt():
    global JWT_TOKEN
    resp = requests.post(AUTH_URL, json={"username": USERNAME, "password": PASSWORD})
    if resp.ok:
        JWT_TOKEN = resp.json()["jwt_token"]
        print("Got token", JWT_TOKEN)
    else:
        print(
            "Failed to get token. Please double check your credentials",
            resp.status_code,
        )
        exit(1)


def ensure_file_exists():
    global FILE_PATH
    args = sys.argv[1:]
    if len(args) == 0:
        print('File not specified. Usage:\n python ./manual-feed-uploader <CSV FILE_PATH>')
        exit(1)
    FILE_PATH = args[0].strip()
    if not os.path.exists(FILE_PATH):
        print("File not found at:", FILE_PATH)
        exit(1)
    print('File found', FILE_PATH)


def post_file():
    print("Uploading file to ACE Now", FILE_PATH)
    files = {"f": ('f', open(FILE_PATH, "rb"), "application/text.csv")}
    resp = requests.post(ACE_URL, files=files, headers={'alt-auth-token': JWT_TOKEN })
    if resp.ok:
        print(resp.status_code, resp.json())
    else:
        print("An error has occurred:", resp.status_code)
        print(resp.json())


ensure_file_exists()
get_jwt()
post_file()
