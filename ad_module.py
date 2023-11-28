import asyncio
import aiohttp
import sys
import collections.abc
import collections
if sys.version_info.major == 3 and sys.version_info.minor >= 10:
    import collections
    setattr(collections, "MutableMapping", collections.abc.MutableMapping)
from ldap3 import Server, Connection, ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES


from functions import read_json_file
data_load = asyncio.run(read_json_file('conf/config.json'))
username = data_load['ad_data']['ad_username']
password = data_load['ad_data']['password_ad']
server_url = f"ldap://{data_load['ad_data']['server_d']}"
s_f = data_load['ad_data']['search_filter']
users = {}

async def get_active_directory_users(server_url, username, password):
    async with aiohttp.ClientSession() as session:
        try:
            server = Server(server_url)
            connection = Connection(server, user=username, password=password, auto_bind=True)
            connection.start_tls()
            
            search_base = "OU=TKO-users,DC=tko,DC=local"  # modify this according to your Active Directory structure
            
            connection.search(search_base, s_f, attributes=[ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES])
            response = connection.response
            
            users = []
            for entry in response:
                user = {
                    'sAMAccountName':   entry['attributes'].get('sAMAccountName', None),
                    'cn':               entry['attributes'].get('cn', None),
                    'mail':             entry['attributes'].get('mail', None),
                    'status':           entry['attributes'].get('userAccountControl', None),
                    'ssh_key':          entry['attributes'].get('pubSshKey', None),
                }
                users.append(user)
            connection.unbind()
            return users
        except Exception:
            pass

async def main_ad():
    while True:
        active_directory_users = await get_active_directory_users(server_url, username, password)
        return active_directory_users

async def main():
    results = await asyncio.gather(main_ad())
    return results

# loop = asyncio.new_event_loop()
# asyncio.set_event_loop(loop) 
# loop.run_until_complete(main())

if __name__ == "__main___":
    print(__file__)
