import re
import logging

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

import faust


class Message(faust.Record):
    sender: str
    recipient: str
    value: str

class BanUser(faust.Record):
    initiator: str
    target: str
    unblock: bool

class ForbiddenWord(faust.Record):
    value: str
    unblock: bool

app = faust.App(
    "msg-filter",
    broker="kafka://127.0.0.1:9094",
    store="memory://"
)

raw_msg_topic = app.topic('messages', value_type=Message, key_type=str)
filtered_msg_topic = app.topic('filtered_messages', value_type=Message, key_type=str)
blocked_users_topic = app.topic('blocked_users', value_type=BanUser)
forbidden_words_topic = app.topic('forbidden_words', value_type=ForbiddenWord)

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
        logger.info(words)
        logger.info(f"forbidden_words_table snapshot: {dict(forbidden_words_table)}")
        patterns = [re.escape(word) for word in words if forbidden_words_table[word.lower()]]
        logger.info(f"patterns: {patterns}")
        if patterns:
            regex = r"\b(" + "|".join(patterns) + r")\b"
            msg.value = re.sub(regex, "***", msg.value, flags=re.IGNORECASE)
        logger.info(msg)
        await filtered_msg_topic.send(key=key, value=msg)

@app.agent(raw_msg_topic, sink=[filter_words])
async def filter_users(stream):
    async for key, msg in stream.items():
        if msg.sender not in blocked_users_table.get(msg.recipient, []):
            yield key, msg
        else:
            logger.info(f"Сообщение {msg} заблокировано по причине нежелательного отправителя")

@app.agent(blocked_users_topic)
async def change_user_block_list(stream):
    async for ban_user in stream:
        current = blocked_users_table.get(ban_user.initiator, []).copy()
        if ban_user.unblock:
            if ban_user.target in current:
                current.remove(ban_user.target)
        elif ban_user.target not in current:
            current.append(ban_user.target)
        blocked_users_table[ban_user.initiator] = current

@app.agent(forbidden_words_topic)
async def change_forbidden_words(stream):
    async for forbidden_word in stream:
        word = forbidden_word.value.lower()
        logger.info(forbidden_word)
        if forbidden_word.unblock:
            forbidden_words_table[word] = False
            logger.info(f"Разблочили слово {word}")
        else:
            forbidden_words_table[word] = True
            logger.info(f"Заблочили слово {word}")
        logger.info(dict(forbidden_words_table))

@app.page('/blocked')
async def get_blocked_users(web, request):
    return web.json(dict(blocked_users_table))

@app.page('/forbidden')
async def get_forbidden_words(web, request):
    return web.json(dict(forbidden_words_table))
