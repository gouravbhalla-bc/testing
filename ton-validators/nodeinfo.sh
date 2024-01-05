#!/bin/zsh

screen -dmS mainnet-16 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon-mainnet-16-green-3ujv 5016 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon-mainnet-16-green-3ujv 5016 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS mainnet-17 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon-mainnet-17-green-5t5d 5017 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon-mainnet-17-green-5t5d 5017 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS mainnet-18 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon-mainnet-18-green-ds9b 5018 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon-mainnet-18-green-ds9b 5018 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-19 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-19-green-u1fd 5019 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-19-green-u1fd 5019 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-20 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-20-green-xdgd 5020 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-20-green-xdgd 5020 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-21 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-21-green-wgel 5021 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-21-green-wgel 5021 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-22 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-22-green-qzd2 5022 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-22-green-qzd2 5022 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-23 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-23-green-ucvl 5023 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-23-green-ucvl 5023 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-24 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-24-green-v7fu 5024 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-24-green-v7fu 5024 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'

screen -dmS -mainnet-25 zsh -c '
source ~/.zshrc;
export MYTON_HTTP_WRAPPER_JWT_TOKEN=<TOKEN>
myton-http-wrapper-curl ton-validator-melon2-mainnet-25-green-y4or 5025 "mytonctrl/wl" "{\"args\": []}";
myton-http-wrapper-curl ton-validator-melon2-mainnet-25-green-y4or 5025 "mytonctrl/pools_list" "{\"args\": []}";
exec zsh'