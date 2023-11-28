from __future__ import annotations
import asyncio
from functools import wraps
from typing import Callable, List
from sqlalchemy import (
    ForeignKey, 
    func, 
    select,
    BigInteger,
    Column,
    Table,
    insert,
    ) 
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    AsyncAttrs,
    async_sessionmaker,
    )
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    sessionmaker,
    mapped_column,
    relationship,
    selectinload,
    )
import greenlet
from functions import read_json_file
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
import datetime

data_load = asyncio.run(read_json_file('conf/config.json'))
dsn = f"postgresql+asyncpg://{data_load['pg_data']['user']}:{data_load['pg_data']['password_pg']}@{data_load['pg_data']['ip_addr']}:5432/{data_load['pg_data']['database']}"

class Base(AsyncAttrs, DeclarativeBase):
    pass

'''Таблица association_table определяет связь между таблицами servers и ssh_keys. 
Она содержит два столбца: server_id, который является внешним ключом для таблицы servers, 
и key_id, который является внешним ключом для таблицы ssh_keys. Оба столбца являются первичными ключами.'''
association_table = Table(
    "association_table",
    Base.metadata,
    Column("server_id", ForeignKey("servers.id"), primary_key=True),
    Column("key_id", ForeignKey("ssh_keys.id"), primary_key=True),
)

class Adusers(Base):
    '''
    Класс Adusers представляет таблицу "ad_users" и содержит следующие поля:
    - id: целочисленный столбец, являющийся первичным ключом таблицы
    - sshkey: список объектов класса "Usersandkeys", связанных с данным пользователем
    - adusername: строковый столбец, содержащий имя пользователя в AD (Active Directory)
    - ruusername: строковый столбец, содержащий имя пользователя на русском языке
    - admail: строковый столбец, содержащий почту пользователя в AD
    - userstatus: строковый столбец, содержащий статус пользователя

    '''
    __tablename__ = "ad_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    ad_username: Mapped[str] = mapped_column(nullable=True)
    ru_username: Mapped[str] = mapped_column(nullable=True)
    ad_mail: Mapped[str] = mapped_column(nullable=True)
    user_status: Mapped[str] = mapped_column(nullable=True)

    ssh_key: Mapped[List["Usersandkeys"]] = relationship(back_populates="aduser")

    async def __str__(self) -> str:
        return f'{self.id}: {self.ssh_key}, {self.ad_username}, {self.ru_username}, {self.ad_mail}, {self.user_status}'

class Usersandkeys(Base):
    '''
    Класс Usersandkeys представляет связующую таблицу "users_and_keys" и содержит следующие поля:
    - id: целочисленный столбец, являющийся первичным ключом таблицы
    - aduserid: целочисленный столбец, содержащий идентификатор пользователя из таблицы "adusers" (внешний ключ)
    - aduser: объект класса "Adusers", связанный с данной записью
    - sshkeyid: целочисленный столбец, содержащий идентификатор SSH-ключа из таблицы "sshkeys" (внешний ключ)
    '''
    __tablename__ = "users_and_keys"

    id: Mapped[int] = mapped_column(primary_key=True)
    ad_user_id: Mapped[int] = mapped_column(ForeignKey("ad_users.id"))
    aduser: Mapped["Adusers"] = relationship(back_populates="ssh_key")

    ssh_key_id: Mapped[int] = mapped_column(ForeignKey("ssh_keys.id")) # many-to-many
    ssh_key: Mapped["Sshkeys"] = relationship("Sshkeys", back_populates="key_user") # many-to-many

    async def __str__(self) -> str:
        return f'{self.id}: {self.ad_user_id}, {self.aduser}'

class Sshkeys(Base):
    '''
    Класс Sshkeys представляет таблицу "ssh_keys" и содержит следующие поля:
    - id: целочисленный столбец, являющийся первичным ключом таблицы
    - key: строковый столбец, содержащий SSH-ключ
    - key_user: объект класса "Usersandkeys", связанный с данной записью.
    '''
    __tablename__ = "ssh_keys"

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(nullable=True)
    time_was_blocked: Mapped[int] = mapped_column(nullable=True)
    is_blocked: Mapped[bool] = mapped_column(nullable=True, default = False) 
    key_user = relationship("Usersandkeys", back_populates="ssh_key")  #one-to-one

    async def __str__(self) -> str:
        return f'{self.id}: {self.key}, {self.time_was_blocked}, {self.is_blocked}, {self.key_user}'

class Servers(Base):
    '''
    Класс Servers отображается на таблицу servers и содержит следующие столбцы:\n 
    - id
    - hostname 
    - ip_addr
    '''
    __tablename__ = "servers"

    id: Mapped[int] = mapped_column(primary_key=True)
    hostname: Mapped[str] = mapped_column(nullable=True)
    ip_addr: Mapped[str] = mapped_column(nullable=True)

    async def __str__(self) -> str:
        return f'{self.id}: {self.hostname}, {self.ip_addr}'

def async_decorator(func: Callable) -> Callable:
    '''
    Внутри функции wrapper создается объект engine с использованием функции create_async_engine, 
    который устанавливает соединение с базой данных и позволяет выполнять асинхронные операции с базой данных.

    Затем создается объект async_session, который представляет асинхронную сессию для работы с базой данных. 

    Затем с помощью async with создается контекст базы данных conn, 
    в рамках которого выполняется синхронная операция создания таблиц базы данных.

    Далее, с использованием оператора async with создается контекст асинхронной сессии session, 
    в рамках которого выполняется исходная функция func с передачей асинхронной сессии и остальных аргументов 
    и ключевых аргументов.

    Наконец, используется функция dispose для очистки ресурсов базы данных и завершения работы.
    '''
    async def wrapper(*args, **kwargs):
        engine = create_async_engine(dsn, echo=False) #echo - достоверный высер по запросам бд
        async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
        async with async_session() as session:
            result = await func(session, *args, **kwargs)
        await engine.dispose()
        return result
    return wrapper

async def created_db() -> None:
    '''
    Создать БД
    '''
    engine = create_async_engine(dsn,echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await engine.dispose()

@async_decorator
async def insert_ad_user(session: async_sessionmaker[AsyncSession], ad_username: str, ru_username: str, ad_mail: str, user_status: str) -> None:
    '''
    Добавить пользователя из АД в таблицу "ad_users"
    '''
    async with session.begin():
        session.add_all(
            [
                Adusers(ssh_key=[], ad_username=ad_username, ru_username = ru_username, ad_mail = ad_mail, user_status = user_status),
            ]
        )

@async_decorator
async def insert_ad_user_with_ssh_key_1(session: async_sessionmaker[AsyncSession], ad_username: str, ru_username: str, ad_mail: str, user_status: str, key_is: str) -> None:
    '''
    Добавить пользователя из АД в таблицу "ad_users", и добавить ему ССШ ключ в таблицу ssh_keys,
    а так же связать их в таблице users_and_keys.
    '''
    async with session.begin():
        ad_user = Adusers(ad_username=ad_username, ru_username=ru_username, ad_mail=ad_mail, user_status=user_status)
        ssh_key = Sshkeys(key=key_is)
        session.add_all([ad_user, ssh_key])

@async_decorator
async def insert_ad_user_with_ssh_key_2(session: async_sessionmaker[AsyncSession], ad_username: str, key_is: str) -> None:
    '''
    после insert_ad_user_with_ssh_key_1 связываем ключ и пользователя
    '''
    async with session.begin():
        ad_user = await session.execute(select(Adusers).filter(Adusers.ad_username == ad_username))
        ssh_key = await session.execute(select(Sshkeys).filter(Sshkeys.key == key_is))
        result_1 = ad_user.scalars().first()
        result_2 = ssh_key.scalars().first()
        user_and_key = Usersandkeys(ad_user_id=result_1.id, ssh_key_id=result_2.id)
        session.add(user_and_key)
        
@async_decorator
async def insert_ad_user_with_ssh_key_3(session: async_sessionmaker[AsyncSession], ad_id: str, key_is: str) -> None:
    '''
    после insert_ad_user_with_ssh_key_1 связываем ключ и пользователя по ad_id
    '''
    async with session.begin():
        ad_user = await session.execute(select(Adusers).filter(Adusers.id == ad_id))
        ssh_key = await session.execute(select(Sshkeys).filter(Sshkeys.key == key_is))
        result_1 = ad_user.scalars().first()
        result_2 = ssh_key.scalars().first()
        user_and_key = Usersandkeys(ad_user_id=result_1.id, ssh_key_id=result_2.id)
        session.add(user_and_key)

@async_decorator
async def insert_ad_user_add_ssh_key(session: async_sessionmaker[AsyncSession], ad_username: str, ru_username: str, ad_mail: str, user_status: str, ssh_key_id: str) -> None:
    '''
    Добавить пользователя из АД и привязать ему существующий ключ
    '''
    async with session.begin():
        session.add_all(
            [
                Adusers(ssh_key=[Usersandkeys(ssh_key_id=ssh_key_id)],  ad_username=ad_username, ru_username = ru_username, ad_mail = ad_mail, user_status = user_status),
            ]
        )

@async_decorator
async def select_and_update_objects_ad_users_2(session: async_sessionmaker[AsyncSession], **kwargs) -> None:
    '''
    изменить данные пользователя АД по его id
    ---
    1. Принимает асинхронную сессию async_session и набор ключевых аргументов kwargs.
    2. Создает асинхронный контекстный менеджер с использованием сессии async_session.
    3. Перебирает каждый ключ и значение в kwargs.
    4. Если ключ равен 'id', то сохраняет значение в переменную id и создает запрос на выборку объекта Adusers с соответствующим идентификатором.
    5. Запускает выполнение запроса с помощью метода session.execute().
    6. Получает результат запроса и извлекает один результат с помощью метода result.scalars().one().
    7. Если ключ равен одному из полей объекта (например, 'adusername', 'ruusername', 'admail' и 'userstatus'), 
    то присваивает соответствующему полю новое значение из kwargs.
    8. Запускает фиксацию изменений с помощью метода session.commit().
    9. Повторяет шаги с 3 по 8 для каждого ключа и значения в kwargs.
    '''
    stmt = select(Adusers).filter(Adusers.id == kwargs.get('id', ''))
    result = await session.execute(stmt)
    a1 = result.scalars().one()
    
    for key, value in kwargs.items():
        if key == 'id':
            continue
            
        setattr(a1, key, value)
        print(f'[{datetime.datetime.now().time()}] actualized: {a1}')

    await session.commit()

@async_decorator
async def select_ad(session: async_sessionmaker[AsyncSession], **kwargs):
    '''
    Получить пользователя АД по любому аргументу, если пользователь существует, то вернет этот же аргумент, иначе None

    Аргументы: 
    - ad_username
    - ru_username
    - ad_mail
    - user_status
    '''
    stmt = select(Adusers)
    for key, value in kwargs.items():
        stmt = stmt.filter(getattr(Adusers, key) == value)
    ad_user = await session.execute(stmt)
    result = ad_user.scalars().first()
    if key == "ad_username":
        return result.ad_username if result is not None else None
    if key == "ru_username":
        return result.ru_username if result is not None else None
    if key == "ad_mail":
        return result.ad_mail if result is not None else None
    if key == "user_status":
        return result.user_status if result is not None else None

@async_decorator
async def select_ad_all(session: async_sessionmaker[AsyncSession], **kwargs) -> dict:
    '''
    Получить все данные пользователя АД по ad_username аргументу ad_username 
    '''
    user = {
        "id": "",
        "ad_username": "",
        "ru_username": "",
        "ad_mail": "",
        "user_status": "",
    }
    stmt = select(Adusers)
    for key, value in kwargs.items():
        stmt = stmt.filter(getattr(Adusers, key) == value)
    ad_user = await session.execute(stmt)
    result = ad_user.scalars().first()
    if key == "ad_username":
        user["id"]=result.id
        user["ad_username"]=result.ad_username
        user["ru_username"]=result.ru_username
        user["ad_mail"]=result.ad_mail
        user["user_status"]=result.user_status
        return user if result is not None else None

@async_decorator
async def select_and_update_ssh_key(session: async_sessionmaker[AsyncSession], ad_id: int, new_key: str):
    '''
    актуализирует ссш-ключ у пользователя
    '''

    stmt = select(Usersandkeys.ssh_key_id).filter(Usersandkeys.ad_user_id == ad_id)
    result = await session.execute(stmt)
    a1 = result.scalars().first()
    if a1 is not None:
        a2 = await session.get(Sshkeys, a1)
        a2.key = new_key
        await session.commit()
    if a1 is None:
        ssh_key = Sshkeys(key=new_key)
        session.add(ssh_key)
        await session.commit()
        await insert_ad_user_with_ssh_key_3(ad_id = ad_id, key_is = new_key)
    print(f'[{datetime.datetime.now().time()}] actualized: {ad_id}')

@async_decorator
async def insert_server(session: async_sessionmaker[AsyncSession], hostname: str, ip_addr: str) -> None:
    '''
    добавить сервер в таблицу "servers"
    '''
    async with session.begin():
        session.add_all(
            [
                Servers(hostname=hostname, ip_addr = ip_addr),
            ]
        )
    print(f'[{datetime.datetime.now().time()}] inserted server: {ip_addr}')

@async_decorator
async def select_server(session: async_sessionmaker[AsyncSession], **kwargs) -> any:
    '''
    Получить server аргумент, если server существует, то вернет этот же аргумент, иначе None.
    Так же модет принимает два аргумента "hostname", "ip_addr" и возвращает "id".

    Аргументы: 
    - id
    - hostname
    - ip_addr

    select_server(hostname = '', ip_addr= '') вернет id сервера или None
    select_server(hostname = '') вернет hostname или None,  nfr ;t c ip_addr
    '''
    stmt = select(Servers)
    if len(kwargs.items()) == 1:
        for key, value in kwargs.items():
            stmt = stmt.filter(getattr(Servers, key) == value)
        server = await session.execute(stmt)
        result = server.scalars().first()
        if key == "id":
            return result.id if result is not None else None
        if key == "hostname":
            return result.hostname if result is not None else None
        if key == "ip_addr":
            return result.ip_addr if result is not None else None
    elif len(kwargs.items()) == 2:
        stmt = stmt.where(Servers.hostname == kwargs.get('hostname'), Servers.ip_addr == kwargs.get('ip_addr'))
        server = await session.execute(stmt)
        result = server.scalars().first()
        return result.id if result is not None else None
        
@async_decorator
async def select_server_all(session: async_sessionmaker[AsyncSession]) -> dict:
    '''
    Возвращает словарь серверов с id, hostname, ip_addr
    '''

    stmt = select(Servers)
    server = await session.execute(stmt)
    result = server.scalars().all()
    dict = {}
    for x in result:
        dict[x.id] = {
            'hostname': x.hostname,
            'ip_addr': x.ip_addr,
        }
    return dict

@async_decorator
async def select_server_one(session: async_sessionmaker[AsyncSession], server_id: int) -> dict:
    '''
    Возвращает сервер с id, hostname, ip_addr
    '''

    stmt = select(Servers).filter(Servers.id == server_id)
    server = await session.execute(stmt)
    result = server.scalars().all()
    dict = {}
    for x in result:
        dict[x.id] = {
            'hostname': x.hostname,
            'ip_addr': x.ip_addr,
        }
    return dict

@async_decorator
async def update_server_ip(session: AsyncSession, server_name: int, ip: str):
    '''
    актуализирует ип сервера
    '''
    stmt = select(Servers).filter(Servers.hostname == server_name)
    result = await session.execute(stmt)
    a1 = result.scalars().one()
    a1.ip_addr = ip
    session.merge(a1)
    await session.commit()
    print(f'[{datetime.datetime.now().time()}] updated server: {ip}')

@async_decorator
async def select_keys_all(session: async_sessionmaker[AsyncSession], **kwargs) -> dict:
    '''
    Возвращает словарь ключей с id
    '''

    stmt = select(Sshkeys)
    server = await session.execute(stmt)
    result = server.scalars().all()
    dict = {}
    for x in result:
        dict[x.id] = {
            'key': x.key,
            'time_was_blocked': x.time_was_blocked,
            'is_blocked': x.is_blocked,
        }
    return dict

@async_decorator
async def bind_keys_and_servers(session: async_sessionmaker[AsyncSession], server_id: int, key_id: int) -> None:
    data = {"server_id": server_id, "key_id": key_id}
    stmt = insert(association_table).values(**data)
    await session.execute(stmt)
    await session.commit()

@async_decorator
async def check_keys_and_servers(session: async_sessionmaker[AsyncSession], server_id: int, key_id: int) -> bool:
    data = {"server_id": server_id, "key_id": key_id}
    stmt = select(association_table).filter_by(**data)
    server = await session.execute(stmt)
    result = server.scalars().all()
    if result: return True
    else: return False

@async_decorator
async def get_all_ad_users(session: async_sessionmaker[AsyncSession]) -> tuple[dict[str, dict], dict[str, dict]]:
    '''
    Разделяет на заблоченных и активных пользователей\n
    users[0] - disabled\n
    users[1] - enabled\n
    '''
    users = {"disabled": {}, "enabled": {}}
    
    stmt = select(Adusers)
    server = await session.execute(stmt)
    result = server.scalars().all()

    for user in result:
        status = user.user_status.lower()
        if 'disabled' in status or 'enabled' in status:
            users_dict = users['disabled'] if 'disabled' in status else users['enabled']
            users_dict[user.id] = {
                "ad_username": user.ad_username,
                "ru_username": user.ru_username,
                "ad_mail": user.ad_mail,
                "user_status": user.user_status,
            }

    return users["disabled"], users["enabled"]

@async_decorator
async def get_server_with_user_key(session: async_sessionmaker[AsyncSession], user_id: int):
    stmt = select(Usersandkeys).filter(Usersandkeys.ad_user_id == user_id)
    server = await session.execute(stmt)
    result = server.scalars().all()
    if result:
        user_key_id: int = result[0].ssh_key_id
        key_is = {"key_id": user_key_id}
        stmt = select(association_table).filter_by(**key_is)
        server = await session.execute(stmt)
        result = server.scalars().all()
    return result

@async_decorator
async def get_key_with_user_id(session: async_sessionmaker[AsyncSession], user_id: int) -> str:
    stmt = select(Usersandkeys).filter(Usersandkeys.ad_user_id == user_id)
    server = await session.execute(stmt)
    result = server.scalars().all()
    if result:
        user_key_id: int = result[0].ssh_key_id
        stmt = select(Sshkeys.key).filter(Sshkeys.id == user_key_id)
        server = await session.execute(stmt)
        result = server.scalars().all()
    return result[0]

#asyncio.run(async_main())
# asyncio.run(insert_objects(ad_username="a2", ru_username = "ВадикБЛять", ad_mail = "Вадик@БЛять", user_status = "ууууууубля"))
# asyncio.run(select_and_update_objects(id = 7))
# asyncio.run(insert_ad_user_with_ssh_key(ad_username="22zzz", ru_username = "22vvv", ad_mail = "z@v", user_status = "zzzzzvvvvv", key_is = 'Zzzzzzzzzzzzzzzzz'))
# asd = asyncio.run(select_ad_all(ad_username = "aalenkin"))
# print(asd)
# asyncio.run(select_and_update_objects_ad_users_2(id = 257, ru_username = "wgbot@test", ad_mail = "z@v"))
# asyncio.run(insert_ad_user_with_ssh_key_1(ad_username="22zzz", ru_username = "22vvv", ad_mail = "z@v", user_status = "zzzzzvvvvv", key_is = 'Zzzzzzzzzzzzzzzzz'))
#asyncio.run(insert_ad_user_with_ssh_key_2(ad_username="22zzz", key_is = 'Zzzzzzzzzzzzzzzzz'))
# asd = asyncio.run(select_and_update_ssh_key(ad_id = 159, new_key = "1232131231231"))
# print(1)
# asyncio.run(insert_server(hostname="22zzz", ip_addr = '10.10.10.10.'))
# asd = asyncio.run(select_server(ip_addr="10.166.101.71/24"))
# asd2 = asyncio.run(select_server(hostname = 'Gorodec'))
# print(1)

# asyncio.run(bind_keys_and_servers(server_id = 483, key_id = 6))
# print(asyncio.run(check_keys_and_servers(server_id = 1321, key_id = 6)))
# ads = asyncio.run(get_all_ad_users())
# print(1)

# asyncio.run(get_server_with_user_key(user_id = 20))
# asyncio.run(select_server_one(server_id = 1322))
# asyncio.run(get_key_with_user_id(user_id = 20))

if __name__ == "__main___":
    print(__file__)