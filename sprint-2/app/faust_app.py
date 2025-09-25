import re
import logging

import faust

from models import Message, BanUser, ForbiddenWord
import config

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

app = faust.App(
    "msg-filter",
    broker=f"kafka://{config.BROKER}",
    store="memory://"
)

raw_msg_topic = app.topic(config.RAW_MSG_TOPIC, value_type=Message, key_type=str)
filtered_msg_topic = app.topic(config.FILTERED_MSG_TOPIC, value_type=Message, key_type=str)
blocked_users_topic = app.topic(config.BLOCKED_USERS_TOPIC, value_type=BanUser)
forbidden_words_topic = app.topic(config.FORBIDDEN_WORDS_TOPIC, value_type=ForbiddenWord)

blocked_users_table = app.Table(
    "blocked-users",
    partitions=2,
)

forbidden_words_table = app.Table(
    "forbidden-words",
    partitions=2,
    default=bool
)

@app.agent()
async def filter_words(stream: faust.StreamT[str, Message]):
    async for key, msg in stream:
        msg = Message(**msg)
        words = re.findall(r"\w+", msg.value)
        patterns = [re.escape(word) for word in words if forbidden_words_table[word.lower()]]
        if patterns:
            regex = r"\b(" + "|".join(patterns) + r")\b"
            msg.value = re.sub(regex, "***", msg.value, flags=re.IGNORECASE)
        await filtered_msg_topic.send(key=key, value=msg)
        logger.info(f"Сообщение {msg} прошло фильтрацию запрещенных слов и отправлено с ключом {key}")

@app.agent(raw_msg_topic, sink=[filter_words])
async def filter_users(stream):
    async for key, msg in stream.items():
        if msg.sender not in blocked_users_table.get(msg.recipient, []):
            logger.info(f"Сообщение {msg} прошло фильтрацию по отправителю")
            yield key, msg
        else:
            logger.info(f"Сообщение {msg} заблокировано по причине нежелательного отправителя")

@app.agent(blocked_users_topic)
async def change_user_block_list(stream):
    async for ban_user in stream:
        logger.debug(f"Текущее состояние таблицы заблокированных юзеров{dict(blocked_users_table)}")
        logger.debug(f"Обрабатываем блокировку юзера {ban_user}")
        current = blocked_users_table.get(ban_user.initiator, []).copy()
        if ban_user.unblock:
            if ban_user.target in current:
                current.remove(ban_user.target)
        elif ban_user.target not in current:
            current.append(ban_user.target)
        blocked_users_table[ban_user.initiator] = current
        logger.debug(f"Новое состояние таблицы заблокированных юзеров{dict(blocked_users_table)}")

@app.agent(forbidden_words_topic)
async def change_forbidden_words(stream):
    async for forbidden_word in stream:
        word = forbidden_word.value.lower()
        logger.debug(forbidden_word)
        if forbidden_word.unblock:
            forbidden_words_table[word] = False
            logger.debug(f"Разблочили слово {word}")
        else:
            forbidden_words_table[word] = True
            logger.debug(f"Заблочили слово {word}")
        logger.debug(f"Текущее состояние заблоченных слов:{dict(forbidden_words_table)}")

@app.page('/blocked')
async def get_blocked_users(web, request):
    return web.json(dict(blocked_users_table))

@app.page('/forbidden')
async def get_forbidden_words(web, request):
    return web.json(dict(forbidden_words_table))
