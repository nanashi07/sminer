# TD Transaction

```sh
curl 'wss://services.thinkorswim.com/Services/WsJson' \
  -H 'Pragma: no-cache' \
  -H 'Origin: https://trade.thinkorswim.com' \
  -H 'Accept-Language: en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,zh-CN;q=0.6' \
  -H 'Sec-WebSocket-Key: WIEz1dg8810WGWB21P+5tA==' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36' \
  -H 'Upgrade: websocket' \
  -H 'Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: Upgrade' \
  -H 'Sec-WebSocket-Version: 13' \
  --compressed
```

## Place order

### Precheck order, step 1

```json
{
    "payload": [
        {
            "header": {
                "id": "update-draft-order-TQQQ",
                "ver": 0,
                "service": "place_order"
            },
            "params": {
                "action": "CONFIRM",
                "accountCode": "635323218",
                "marker": "SINGLE",
                "orders": [
                    {
                        "tif": "DAY",
                        "orderType": "LIMIT",
                        "requestType": "EDIT_ORDER",
                        "legs": [
                            {
                                "symbol": "TQQQ",
                                "quantity": -400
                            }
                        ],
                        "tag": "TOSWeb"
                    }
                ]
            }
        }
    ]
}
```

response

```
TODO
```

### Precheck order, step 2

```json
{
    "payload": [
        {
            "header": {
                "id": "update-draft-order-TQQQ",
                "ver": 1,
                "service": "place_order"
            },
            "params": {
                "action": "CONFIRM",
                "accountCode": "635323218",
                "marker": "SINGLE",
                "orders": [
                    {
                        "tif": "DAY",
                        "orderType": "LIMIT",
                        "requestType": "EDIT_ORDER",
                        "limitPrice": 48.23,
                        "legs": [
                            {
                                "symbol": "TQQQ",
                                "quantity": -400
                            }
                        ],
                        "tag": "TOSWeb"
                    }
                ]
            }
        }
    ]
}
```

response

```
TODO
```

### Submit order

```json
{
    "payload": [
        {
            "header": {
                "ver": 0,
                "service": "place_order",
                "id": "update-draft-order-TQQQ"
            },
            "params": {
                "accountCode": "635323218",
                "action": "SUBMIT",
                "marker": "SINGLE",
                "orders": [
                    {
                        "tif": "DAY",
                        "orderType": "LIMIT",
                        "limitPrice": 48.23,
                        "requestType": "EDIT_ORDER",
                        "legs": [
                            {
                                "symbol": "TQQQ",
                                "quantity": -400
                            }
                        ],
                        "tag": "TOSWeb"
                    }
                ]
            }
        }
    ]
}
```

response

```
TODO
```

## Get config api

```sh
curl 'https://trade.thinkorswim.com/v1/api/config' \
  -H 'Connection: keep-alive' \
  -H 'Pragma: no-cache' \
  -H 'Cache-Control: no-cache' \
  -H 'sec-ch-ua: " Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"' \
  -H 'DNT: 1' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'Accept: */*' \
  -H 'Sec-Fetch-Site: same-origin' \
  -H 'Sec-Fetch-Mode: cors' \
  -H 'Sec-Fetch-Dest: empty' \
  -H 'Referer: https://trade.thinkorswim.com/trade?symbol=TQQQ' \
  -H 'Accept-Language: en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,zh-CN;q=0.6' \
  -H 'Cookie: language=zh-TW; utag_main=v_id:017ed6e73731001c239a1409acf505078001e07000942$_sn:23$_se:2$_ss:0$_st:1646837456808$ses_id:1646835413734%3Bexp-session$_pn:2%3Bexp-session' \
  --compressed
```

response

```json
{
    "apiUrl": "https://api.tdameritrade.com/v1",
    "apiKey": "TDATRADERX@AMER.OAUTHAP",
    "mobileGatewayUrl": {
        "livetrading": "wss://services.thinkorswim.com/Services/WsJson",
        "papermoney": "wss://services-demo.thinkorswim.com/Services/WsJson"
    },
    "authUrl": "https://auth.tdameritrade.com/auth",
    "activeTraderEnabled": true,
    "lmsApiKey": "TOSWeb",
    "isLMS": false,
    "educationUrl": "https://education.thinkorswim.com"
}
```
