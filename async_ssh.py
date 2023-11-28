import asyncio
import asyncssh
from typing import Callable
from asyncssh.sftp import SFTPNoSuchFile
import re 
import functions

data_load = asyncio.run(functions.read_json_file('conf/config.json'))
username = data_load['ssh']['username']
password = data_load['ssh']['password']
path_lin = data_load['ssh']['path_lin']

def ssh_connection(func):
    '''
    декоратор для создания ссш соединения
    '''
    async def wrapper(hostname, port, username, password, *args, **kwargs):
        try:
            conn = await asyncio.wait_for(asyncssh.connect(
                hostname,
                port=port,
                username=username,
                password=password,
                known_hosts=None
            ), timeout=4) 
            return await func(conn, *args, **kwargs)
        except asyncssh.misc.PermissionDenied as e:
            print(f"Ошибка: {e}. Server {hostname}")
        except asyncssh.misc.HostKeyNotVerifiable as e:
            print(f"Ошибка: {e}. Ключ хоста не доверен.")
        except ConnectionRefusedError as e:
            print(f"Connection refused: {e}. Возможно, сервис SSH не запущен или порт заблокирован.")
        except asyncio.TimeoutError as e:
            print(f"Timeout error: {e}. Время ожидания соединения истекло.")
    return wrapper

@ssh_connection
async def comment_key(conn, path_lin, key_to_search) -> None:
    '''
    закомментировать строку
    -
    asyncio.run(comment_key(ip[str], port[int], username, password, path_lin, line_is))
    '''
    pattern = r'\w+@\w+'
    key_to_search_2 = re.findall(pattern, key_to_search)
    line_to_search = f'.*{key_to_search_2[0]}.*'
    commented_line_to_search = f'#.*{line_to_search}.*'
    command_check_commented = f"grep '{commented_line_to_search}' {path_lin}"
    result_check_commented = await conn.run(command_check_commented)    # ищем закомментированные строки с ключом
    
    if key_to_search in result_check_commented.stdout: # если ключ уже закомментирован, то не делаем ничего
        return None

    command_to_comment = f"sed -i '/{line_to_search}/s/^/#/' {path_lin}" # иначе закомментируем
    result_comment = await conn.run(command_to_comment)
    return result_comment.stdout

@ssh_connection
async def uncomment_key(conn, path_lin, key_to_search) -> None:
    '''
    раскомментировать строку
    ---
    asyncio.run(uncomment_line('server_ip', port, username, password, path_lin, key_to_search = line_is))
    '''
    pattern = r'\w+@\w+'
    key_to_search_2 = re.findall(pattern, key_to_search)
    path_lin = path_lin
    key_to_search = key_to_search_2[0]
    command_2 = f"sed -i '/^#.*{key_to_search}/s/^#//g' {path_lin}"
    result = await conn.run(command_2)
    return result.stdout

@ssh_connection
async def remove_key(conn, path_lin, key_to_search) -> None:
    '''
    удалить ключ с пользователем "слово@слово"
    ---
    asyncio.run(remove_key('server_ip', port, username, password, path_lin, key_to_search = line_is))
    '''
    path_lin = path_lin
    key_to_search = key_to_search
    command = f"sed -i '/{key_to_search}/d' {path_lin}"
    print(command)
    result = await conn.run(command)
    return result.stdout

@ssh_connection
async def asyncssh_read_file(conn, remote_path) -> list[str]:
    '''
    чтение содержимого файла по ссш
    ---
    '''
    try:
        sftp = await conn.start_sftp_client()
        remote_file = await sftp.open(remote_path)
        file_content = await remote_file.read()
        await remote_file.close()
    except SFTPNoSuchFile:
        print(f"Файл не найден: {remote_path}")
        return []
    finally: 
        sftp.exit()
        conn.close()
    return file_content.strip().split('\n')

# asd = asyncio.run(remove_key('10.166.100.12', 22, username, password, path_lin, key_to_search = line_is))
# print(asd)
# asd = asyncio.run(uncomment_key('10.166.100.12', 22, username, password, path_lin, line_is))
# print(asd)

if __name__ == "__main___":
    print(__name__)
    