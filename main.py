import asyncio
import ad_module
from async_bd import (
    created_db,
    insert_ad_user,
    select_ad,
    select_ad_all,
    insert_ad_user_with_ssh_key_1,
    insert_ad_user_with_ssh_key_2,
    select_and_update_objects_ad_users_2,
    select_and_update_ssh_key,
    insert_server,
    select_server,
    update_server_ip,
    select_server_all,
    select_keys_all,
    bind_keys_and_servers,
    check_keys_and_servers,
    get_all_ad_users,
    select_server_one,
    get_server_with_user_key,
    get_key_with_user_id,
    )
from load_server_info import (
    list_poligon,
    get_info_from_netbox,
    get_peers_wg
    )
import async_ssh
import concurrent.futures
from itertools import islice
import functions
import re
import datetime

data_load = asyncio.run(functions.read_json_file('conf/config.json'))
username = data_load['ssh']['username']
password = data_load['ssh']['password']
path_lin = data_load['ssh']['path_lin']

 
async def get_users_from_ad() -> list[dict]:
    return await ad_module.main_ad()

async def pull_ad_users(list_data: list[dict]) -> None:
    '''
    добавление новых пользователей + ключи
    '''
    try:
        for item in list_data:
            if item.get('ssh_key') is None: # если нет ключа
                if await select_ad(ad_username=item.get('sAMAccountName')) is None: # если нет пользователя в БД
                    if item.get('status') in [512, 514, 66048, 66050]:
                        status_dict = {
                            512: "Enabled",
                            514: "Disabled",
                            66048: "Enabled, password never expires",
                            66050: "Disabled, password never expires"
                        }
                        data_status = status_dict.get(item.get('status'), "None")
                        await insert_ad_user(ad_username=item.get('sAMAccountName'),
                                                ru_username=item.get('cn'),
                                                ad_mail=item.get('mail'),
                                                user_status=data_status)
                        ad_username = item.get('sAMAccountName')
                        print(f'[{datetime.datetime.now().time()}] added from AD: {ad_username}')
            if item.get('ssh_key') is not None: # если есть ключ
                if await select_ad(ad_username=item.get('sAMAccountName')) is None: # если нет пользователя в БД
                    if item.get('status') in [512, 514, 66048, 66050]:
                        status_dict = {
                            512: "Enabled",
                            514: "Disabled",
                            66048: "Enabled, password never expires",
                            66050: "Disabled, password never expires"
                        }
                        data_status = status_dict.get(item.get('status'), "None")
                        await insert_ad_user_with_ssh_key_1(ad_username=item.get('sAMAccountName'),
                                                        ru_username=item.get('cn'),
                                                        ad_mail=item.get('mail'),
                                                        user_status=data_status,
                                                        key_is=item.get('ssh_key')
                                                        ),
                        await insert_ad_user_with_ssh_key_2(
                                                        ad_username=item.get('sAMAccountName'),
                                                        key_is=item.get('ssh_key')
                                                        )
                        ad_username = item.get('sAMAccountName')
                        print(f'[{datetime.datetime.now().time()}] added from AD + ssh key: {ad_username}')
    except Exception:
        pass

async def actualise_data_ad(list_data: list[dict]) -> None:
    '''
    актуализация данных пользователей
    '''
    try:
        for item in list_data:
            user_info = await select_ad_all(ad_username=item.get('sAMAccountName'))
            if item.get('status') in [512, 514, 66048, 66050]:
                status_dict = {
                            512: "Enabled",
                            514: "Disabled",
                            66048: "Enabled, password never expires",
                            66050: "Disabled, password never expires"
                        }
                data_status = status_dict.get(item.get('status'), "None")
                id = user_info.get('id')
                if user_info.get('ad_username') != item.get('sAMAccountName'):
                    await select_and_update_objects_ad_users_2(id = id, ad_username = item.get('sAMAccountName', ''))
                    ad_username = item.get('sAMAccountName')
                if user_info.get('ru_username') != item.get('cn'):
                    await select_and_update_objects_ad_users_2(id = id, ru_username = item.get('cn', ''))
                    ad_username = item.get('sAMAccountName')
                if user_info.get('ad_mail') != item.get('mail'):
                    await select_and_update_objects_ad_users_2(id = id, ad_mail = item.get('mail', ''))
                    ad_username = item.get('sAMAccountName')
                if user_info.get('user_status') != data_status:
                    await select_and_update_objects_ad_users_2(id = id, user_status = data_status)
                    ad_username = item.get('sAMAccountName')
                if user_info.get('ssh_key') != item.get('ssh_key'):
                    await select_and_update_ssh_key(ad_id = id, new_key = item.get('ssh_key'))
                    ad_username = item.get('sAMAccountName')
    except Exception:
        pass

async def pull_nb_servers(dtln: dict) -> None:
    async def handle_server(key, value):
        server_with_ip = await select_server(hostname=key, ip_addr=value[2])
        server_without_ip = await select_server(hostname=key)
        if server_with_ip is None and server_without_ip is None:
            await insert_server(hostname=key, ip_addr=value[2])
        elif server_without_ip is not None:
            await update_server_ip(server_name=key, ip=value[2])
            
    n = 30  #число серверов для обработки за раз
    server_iterator = iter(dtln.items())
    while chunk := list(islice(server_iterator, n)):
        await asyncio.gather(*(handle_server(key, value) for key, value in chunk))

async def pull_wg_servers(wg_servers: dict) -> None:
    async def handle_server(key, value):
        server_with_ip = await select_server(hostname=key, ip_addr=value)
        server_without_ip = await select_server(hostname=key)
        if server_with_ip is None and server_without_ip is None:
            await insert_server(hostname=key, ip_addr=value)
            print(f'[{datetime.datetime.now().time()}] inserted server from wireguard: {value[2]}')
        elif server_without_ip is not None:
            await update_server_ip(server_name=key, ip=value)
            print(f'[{datetime.datetime.now().time()}] updated server from wireguard: {value[2]}')
    await asyncio.gather(*(handle_server(key, value) for key, value in wg_servers.items()))

async def to_bind_ssh_key_with_servers():
    username = 'user'
    password = 'password'
    path_lin = '/home/user/.ssh/authorized_keys'

    list_of_servers = await select_server_all()
    list_of_keys = await select_keys_all()

    for server in list_of_servers: #list_of_servers.get(server)['ip_addr']
        keys_of_server = await read_keys_from_server(list_of_servers.get(server)['ip_addr'], username, password, path_lin)

        if keys_of_server:
            await bind_keys_if_exists_on_server(server, keys_of_server, list_of_keys)

async def read_keys_from_server(server, username, password, path):
    server_ip = server
    if server_ip:
        return await async_ssh.asyncssh_read_file(server_ip, port=22, username=username, password=password, remote_path=path)

async def bind_keys_if_exists_on_server(server, keys_of_server, list_of_keys):
    for key in list_of_keys:
        key_value = list_of_keys.get(key)['key']
        if key_value in keys_of_server:
            if await check_keys_and_servers(server_id=server, key_id=key) is False:
                await bind_keys_and_servers(server_id=server, key_id=key)
                print(f'[{datetime.datetime.now().time()}] binded ssh key and server: {key} / {server}')

async def actualise_ssh_keys_data():
    '''
    актуализация ключей, в зависимости от статуса пользователя
    '''
    pattern = r'\w+@\w+'
    disabled, enabled = await get_all_ad_users()

    for key_status, users in {'disabled': disabled, 'enabled': enabled}.items():
        for user in users:
            id_servers = await get_server_with_user_key(user_id = user)
            to_do_servers = [await select_server_one(server_id=server_dis) for server_dis in id_servers]
            for server in to_do_servers:
                for _, value in server.items():
                    ip_addr = value['ip_addr']
                    line_is = await get_key_with_user_id(user_id = user)
                    if key_status == 'disabled':
                        await async_ssh.comment_key(hostname = ip_addr, port=22, username = username, password = password, path_lin = path_lin, key_to_search = line_is)
                        # line_is = re.findall(pattern, line_is)
                        # print(f'[{datetime.datetime.now().time()}] commented: {ip_addr} / {line_is[0]}')
                    else:
                        await async_ssh.uncomment_key(hostname = ip_addr, port=22, username = username, password = password, path_lin = path_lin, key_to_search = line_is)
                        # line_is = re.findall(pattern, line_is)
                        # print(f'[{datetime.datetime.now().time()}] uncommented: {ip_addr} / {line_is[0]}')

# asyncio.run(pull_ad_users(asyncio.run(get_users_from_ad())))
# asyncio.run(actualise_data_ad(asyncio.run(get_users_from_ad())))
# asd = asyncio.run(get_info_from_netbox())
# asyncio.run(pull_nb_servers(asd))
# results = asyncio.run(list_poligon(get_peers_wg()))
# asyncio.run(pull_wg_servers(results))
# asyncio.run(to_bind_ssh_key_with_servers())
# asyncio.run(actualise_ssh_keys_data())

async def run_tasks_separately():
    # Сначала создаем задачи, которые будут выполняться параллельно
    task1 = asyncio.create_task(pull_ad_users(await get_users_from_ad()))
    task2 = asyncio.create_task(actualise_data_ad(await get_users_from_ad()))

    asd = await get_info_from_netbox()
    task3 = asyncio.create_task(pull_nb_servers(asd))

    results = await list_poligon(get_peers_wg())
    task4 = asyncio.create_task(pull_wg_servers(results))

    task5 = asyncio.create_task(to_bind_ssh_key_with_servers())
    task6 = asyncio.create_task(actualise_ssh_keys_data())

    # Дожидаемся окончания выполнения всех задач
    await asyncio.gather(task1, task2, task3, task4, task5, task6)

# async def run_tasks_separately():
#     ad_users_task = asyncio.create_task(pull_ad_users(get_users_from_ad()))
#     data_ad_task = asyncio.create_task(actualise_data_ad(get_users_from_ad()))

#     asd = await get_info_from_netbox()
#     nb_servers_task = asyncio.create_task(pull_nb_servers(asd))

#     results = await list_poligon(get_peers_wg())
#     wg_servers_task = asyncio.create_task(pull_wg_servers(results))

#     ssh_key_task = asyncio.create_task(to_bind_ssh_key_with_servers())
#     ssh_keys_data_task = asyncio.create_task(actualise_ssh_keys_data())

#     # Независимо ждем завершения каждой задачи
#     await ad_users_task
#     await data_ad_task
#     await nb_servers_task
#     await wg_servers_task
#     await ssh_key_task
#     await ssh_keys_data_task

if __name__ == "__main__":
    asyncio.run(created_db())
    asyncio.run(run_tasks_separately())