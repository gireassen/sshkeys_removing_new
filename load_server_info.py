import aiohttp
import asyncio
from routeros_api import RouterOsApiPool #"aioRouterOS" и "aioMikrotik" ??
from functions import read_json_file
import re
from typing import List, Dict

data_load = asyncio.run(read_json_file('conf/config.json'))
dsn = f"postgresql+asyncpg://{data_load['pg_data']['user']}:{data_load['pg_data']['password_pg']}@{data_load['pg_data']['ip_addr']}:5432/{data_load['pg_data']['database']}"

username_wg = data_load['wg_data']['username']
host_wg = data_load['wg_data']['host']
password_wg = data_load['wg_data']['password']
port_wg = data_load['wg_data']['port']
api_netbox = data_load['netbox']['API_ENDPOINT']
netbox_next = data_load['netbox']['AWX_NEXT']
netbox_headers = data_load['netbox']['HEADERS']

connection = RouterOsApiPool(host = host_wg,
username=username_wg,
password=password_wg,
port=port_wg,
plaintext_login=True)

def get_peers_wg() -> List[dict]:
    '''
    command to get info about peers
    -
    /interface/wireguard/peers/
    переписать под "aioRouterOS" и "aioMikrotik"
    '''
    api = connection.get_api()
    wg_command = '/interface/wireguard/peers/'
    list_address = api.get_resource(wg_command)
    wg_users = list_address.get()
    return wg_users

async def list_poligon(list: List[dict]) -> Dict[str, str]:
    regex = r"\b(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\b"
    regex_2 = r"^([\d]{1,3}\.){3}[\d]{1,3}"
    result = {}
    for poligon in list:
        key = poligon.get('comment')
        value = await asyncio.to_thread(re.search, regex_2, poligon.get('allowed-address'))
        if value:
            value_new = value.group()
        match = await asyncio.to_thread(re.search, regex, value_new)
        if match:
            if int(value_new.split(".")[-1]) <= 100:
                result[key]= value_new
    return result

async def get_info_from_netbox() -> Dict[str, tuple]:
    '''
    функция берет по апи инфу по всем ип(их адрес и хостнейм) с NetBox
    '''
    subnet_regex = re.compile(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/(\d{1,2})$')
    dicts = {}
    async with aiohttp.ClientSession() as session:
        next_page = api_netbox
        while next_page is not None:
            response = await session.get(next_page, headers=netbox_headers, ssl=False)
            data = await response.json()
            total_results = data["results"]
            next_page = data["next"]
            for result in total_results:
                if result.get('primary_ip4') is not None:
                    ip_address = result['primary_ip4'].get('address')
                    match = subnet_regex.match(ip_address)
                    ip_address_wm = match.group(1)
                    dicts[result['name']] = (
                        result['name'],
                        result.get('comments', ''),
                        ip_address_wm,
                        result['tags'][0]['name'],
                        result['status']['value']
                    )
                else:
                    ip_address = None
                    dicts[result['name']] = (
                        result['name'],
                        result.get('comments', ''),
                        ip_address,
                        result['tags'][0]['name'],
                        result['status']['value']
                    )
    return dicts


# asyncio.run(list_poligon(get_peers_wg()))
# results = asyncio.run(list_poligon(get_peers_wg()))
# print(results)
# for server, addr in results.items():
#     print(server)
if __name__ == "__main___":
    print(__file__)