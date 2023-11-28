import aiohttp
import asyncio
from ldap3 import Server, Connection, ALL_ATTRIBUTES, ALL_OPERATIONAL_ATTRIBUTES
from concurrent.futures import ThreadPoolExecutor

def fetch_active_directory_data(server_url, username, password):
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

async def get_active_directory_users(server_url, username, password):    
    try:
        # create a thread pool
        with ThreadPoolExecutor() as executor:
            users = await asyncio.get_event_loop().run_in_executor(executor, fetch_active_directory_data, server_url, username, password)
        return users
    except Exception:
        pass
