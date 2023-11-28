import aiofiles
import json
import time
import asyncio
import sys
import datetime

def log_decorator(func):
    async def wrapper(*args, **kwargs):
        current_date = datetime.datetime.now().strftime("%d-%m-%Y")
        try:
            with open(f'logs/{func.__name__}-{current_date}.log', 'a') as log_file:
                original_stdout = sys.stdout
                sys.stdout = log_file

                try:
                    result = await func(*args, **kwargs)
                finally:
                    # восстанавливаем sys.stdout даже если было исключение
                    sys.stdout = original_stdout
        except IOError as e:
            print('Ошибка при записи в файл:', e)
            result = await func(*args, **kwargs)
        
        return result

    return wrapper

async def read_json_file(filename: str) -> dict:
    try:
        async with aiofiles.open(filename, 'r', encoding='utf-8') as file:
            content = await file.read()
            data = json.loads(content)
            return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{filename}' not found.")
    except json.JSONDecodeError:
        raise ValueError(f"File '{filename}' is not a valid JSON file.")

async def is_more_thirty_days(time_stamp_from_bd) -> bool:
    '''
    проверка если после блокировки ключа прошло 30 дней
    '''
    now = time.time()
    return now - time_stamp_from_bd > 2592000

if __name__ == "__main___":
    print(__file__)