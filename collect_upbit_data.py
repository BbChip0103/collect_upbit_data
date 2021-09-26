import asyncio
import websockets
import json
import ccxt
import os
import os.path as pth
import datetime as dt


metadata_path = '../metadata/coin_exchange.json'
with open(metadata_path) as f:
    exchange_api_dict = json.loads(f.read())


async def upbit_ws_client(coin_list, callback):
    uri = 'wss://api.upbit.com/websocket/v1'
    async with websockets.connect(uri) as websocket:
        subscribe_fmt = [
            {'ticket': 'bbchip13'},
            {'format': 'SIMPLE'}
        ]
        subscribe_fmt += [
            {
                'type': 'ticker',
                'codes': ['KRW-{}'.format(coin_name)],
                'isOnlyRealtime': True
            } for coin_name in coin_list
        ] 

        subscribe_data = json.dumps(subscribe_fmt)
        await websocket.send(subscribe_data)

        result_base_path = 'upbit'
        os.makedirs(result_base_path, exist_ok=True)

        while True:
            res = await websocket.recv()
            res = json.loads(res)
#            print(res['cd'], res['tp'], res['tv'], res['ttms'])
            target_result = [res['ttms'], res['cd'], res['tp'], res['tv']]
            target_result = list(map(str, target_result))

            current_time = dt.datetime.now()
            current_time_str = current_time.strftime('%Y%m%d_%H%M')
            result_full_path = pth.join(result_base_path, current_time_str+'_'+'upbit.csv')
            if not pth.isfile(result_full_path):
                print(result_full_path)
                with open(result_full_path, 'w') as f:
                    f.write(','.join(['timestamp', 'symbol', 'price', 'volume'])+'\n')
                    f.write(','.join(target_result)+'\n')
            else:
                with open(result_full_path, 'a') as f:
                    f.write(','.join(target_result)+'\n')


def get_upbit_coin_list():
    upbit_exchange_id = 'upbit'
    upbit_exchange_class = getattr(ccxt, upbit_exchange_id)
    upbit_exchange = upbit_exchange_class({
        'apiKey': exchange_api_dict['upbit']['apiKey'],
        'secret': exchange_api_dict['upbit']['secret'],
    })

    upbit_coin_dict = {
        k:v for k, v in upbit_exchange.load_markets().items() 
            if '/KRW' in k
    }
    upbit_coin_list = [
        name.replace('/KRW', '') for name in list(upbit_coin_dict.keys())
    ]
    return upbit_coin_list


def get_binance_coin_list():
    binance_exchange_id = 'binance'
    binance_exchange_class = getattr(ccxt, binance_exchange_id)
    binance_exchange = binance_exchange_class({
        'apiKey': exchange_api_dict['binance']['apiKey'],
        'secret': exchange_api_dict['binance']['secret'],
    })

    ### TODO: Remove this
    binance_exchange.nonce = lambda: binance_exchange.milliseconds() - 1000

    binance_coin_dict = {
        k:v for k, v in binance_exchange.load_markets().items() 
            if '/USDT' in k and v['active'] == True
    }
    binance_coin_list = [
        name.replace('/USDT', '') for name in list(binance_coin_dict.keys())
    ]
    return binance_coin_list


def upbit_callback_func():
    pass


if __name__ == "__main__":
    upbit_coin_list = get_upbit_coin_list()
#    binance_coin_list = get_binance_coin_list()

#    overlapped_coin_list = list(set(upbit_coin_list)&set(binance_coin_list)) 
    overlapped_coin_list = upbit_coin_list

    tasks = [
        asyncio.ensure_future(
            upbit_ws_client(overlapped_coin_list, upbit_callback_func)
        ),
    ]
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(asyncio.wait(tasks))
