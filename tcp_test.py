import asyncio
import logging
from datetime import datetime
import random
from zoneinfo import ZoneInfo

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase


logging.basicConfig(level=logging.INFO)

stop_server = False


class Base(DeclarativeBase):
    """Базовая модель для ORM"""


class Request(Base):
    """Модель базы данных для хранения запросов"""

    __tablename__ = 'requests'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_address = Column(String, nullable=False)
    message = Column(String, nullable=False)
    timestamp = Column(
        DateTime, default=lambda: datetime.now(ZoneInfo('Europe/Moscow'))
        )


# Создание подключения к базе данных и сессии
DATABASE_URL = 'sqlite:///requests.db'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


def save_request_in_db(client_address, message):
    """Запись запроса в базу данных через ORM"""

    session = Session()
    session.add(Request(client_address=client_address, message=message))
    session.commit()
    session.close()


async def handle_client(reader, writer):
    """Сопрограмма обработки запроса клиента"""

    client_address = writer.get_extra_info('peername')

    str_client_address = f'{client_address[0]}:{client_address[1]}'

    try:
        while True:
            data = await reader.read(1024)

            if not data:
                break

            message = data.decode()
            save_request_in_db(str_client_address, message)
            writer.write(f'Эхо-ответ: {message}'.encode())
            await writer.drain()

    except asyncio.CancelledError:
        logging.info(
            f'Клиент отключился, пропало соединение: {client_address}'
        )

    finally:
        writer.close()
        await writer.wait_closed()


async def run_server():
    """Сопрограмма запуска TCP-сервера"""

    global stop_server

    server = await asyncio.start_server(handle_client, '127.0.0.1', 8080)
    logging.info('Сервер запущен на 127.0.0.1:8080')
    async with server:
        while not stop_server:
            await asyncio.sleep(1)

    logging.info('Сервер остановлен')


async def tcp_client(id):
    """Сопрограмма запуска TCP-клиентов"""

    reader, writer = await asyncio.open_connection('127.0.0.1', 8080)

    for i in range(5):
        message = f'Сообщение {i}, клиент {id}'
        writer.write(message.encode())
        await writer.drain()
        data = await reader.read(1024)
        logging.info(f'Ответ от сервера: {data.decode()}')

        await asyncio.sleep(random.randint(5, 10))

    writer.close()

    await writer.wait_closed()


async def main():
    """Основная программа"""

    global stop_server

    server_task = asyncio.create_task(run_server())

    await asyncio.sleep(2)

    client_tasks = [asyncio.create_task(tcp_client(i)) for i in range(10)]

    await asyncio.gather(*client_tasks)

    stop_server = True

    await server_task

if __name__ == '__main__':

    # Запуск основной программы
    asyncio.run(main())
