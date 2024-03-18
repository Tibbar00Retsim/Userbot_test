from pyrogram import Client, filters
from pyrogram.errors import UserDeactivated, BotBlocked
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.sql import select, update
from asyncio import sleep
import datetime
import asyncio
import logging

logging.basicConfig(level=logging.INFO)

class UserBot:

    def __init__(self):
        self.app = Client("my_account")
        self.engine = create_engine('postgresql://user:password@localhost/dbname')
        self.metadata = MetaData()
        self.users = Table('users', self.metadata, autoload_with=self.engine)
        self.funnel = [
            {"time": 6*60, "text": "Текст1", "trigger": None},
            {"time": 39*60, "text": "Текст2", "trigger": "Триггер1"},
            {"time": (1*24*60*60) + (2*60*60), "text": "Текст3", "trigger": None}
        ]

    def start(self):

        self.app.on_message(filters.text)(self.handle_text)
        self.app.on_message(filters.text & filters.incoming)(self.check_triggers)
        self.app.run()

    async def handle_text(self, client, message):
      
        with self.engine.connect() as connection:
            s = select(self.users).where(self.users.c.id == message.from_user.id)
            result = connection.execute(s)
            user = result.fetchone()
            if not user:
                ins = self.users.insert().values(id=message.from_user.id, created_at=datetime.datetime.now(), status='alive', status_updated_at=datetime.datetime.now())
                connection.execute(ins)

    async def check_triggers(self, client, message):
    
        with self.engine.connect() as connection:
            s = select(self.users).where(self.users.c.id == message.from_user.id)
            result = connection.execute(s)
            user = result.fetchone()
            if user and user['status'] == 'alive':
                if 'прекрасно' in message.text or 'ожидать' in message.text:
                    upd = self.users.update().where(self.users.c.id == message.from_user.id).values(status='finished', status_updated_at=datetime.datetime.now())
                    connection.execute(upd)
                else:
                    for step in self.funnel:
                        if step['trigger'] and step['trigger'] in message.text:
                            break
                        else:
                            try:
                                await sleep(step['time'])
                                await self.app.send_message(message.chat.id, step['text'])
                            except (UserDeactivated, BotBlocked) as e:
                                logging.error(f"Ошибка при отправке сообщения: {e}")
                                upd = self.users.update().where(self.users.c.id == message.from_user.id).values(status='dead', status_updated_at=datetime.datetime.now())
                                connection.execute(upd)
                                break

    async def check_users(self):
        
        while True:
            with self.engine.connect() as connection:
                s = select(self.users).where(self.users.c.status == 'alive')
                result = connection.execute(s)
                for user in result:
                    asyncio.create_task(self.check_triggers(user))
            await asyncio.sleep(60)

if __name__ == "__main__":
    bot = UserBot()
    bot.start()
