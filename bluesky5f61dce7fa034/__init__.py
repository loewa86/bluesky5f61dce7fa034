import asyncio
import websockets
import orjson  # Используем orjson для быстрого парсинга JSON
import random
import logging
import datetime
import hashlib
from typing import AsyncGenerator, Any, Dict
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    ExternalParentId,
    Url,
    Domain,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
    
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)


DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 1000  # Увеличено
DEFAULT_MIN_POST_LENGTH = 5
DEFAULT_SKIP_PROBABILITY = 0.0

def read_parameters(parameters):
    if not parameters or not isinstance(parameters, dict):
        return (
            DEFAULT_OLDNESS_SECONDS,
            DEFAULT_MAXIMUM_ITEMS,
            DEFAULT_MIN_POST_LENGTH,
            DEFAULT_SKIP_PROBABILITY
        )
    
    return (
        parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS),
        parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS),
        parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH),
        parameters.get("skip_probability", DEFAULT_SKIP_PROBABILITY)
    )

jetstreams_array_endpoints = [
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network"
]

async def connect_to_jetstream(endpoint, max_posts):
    post_count = 0  # Теперь переменная явно объявлена
    uri = f"wss://{endpoint}/subscribe?wantedCollections=app.bsky.feed.post"
    logging.info(f"[BlueSky] Connecting to Jetstream: {uri}")

    async with websockets.connect(uri) as websocket:
        logging.info(f"[BlueSky] Connected to Jetstream WebSocket: {endpoint}")

        while post_count < max_posts:  # Теперь не будет ошибки
            try:
                message = await websocket.recv()
                event = orjson.loads(message)  

                if 'commit' not in event or event['commit']['collection'] != 'app.bsky.feed.post':
                    continue
                if event['commit']['operation'] != 'create':
                    continue

                post_count += 1  # Увеличиваем счётчик сообщений
                
                content_ = str(event['commit']['record'].get('text', ''))
                author_ = event['did']
                author_sha1_hex = hashlib.sha1(author_.encode()).hexdigest()
                
                # Нормализация createdAt
                created_at = event['commit']['record'].get('createdAt', '')
                if created_at.endswith("+00:00"):
                    created_at = created_at[:-6] + "Z"

                external_id = event['commit']['rkey']
                external_parent_id = ""
                if 'reply' in event['commit']['record']:
                    external_parent_id = event['commit']['record']['reply']['parent']['uri']
                    if '/app.bsky.feed.post/' in external_parent_id:
                        external_parent_id = external_parent_id.split('/app.bsky.feed.post/')[-1]
                url = f"https://bsky.app/profile/{author_}/post/{external_id}"

                item = Item(
                    content=Content(content_),
                    author=Author(author_sha1_hex),
                    created_at=CreatedAt(created_at),
                    domain=Domain("bsky.app"),
                    external_id=ExternalId(external_id),
                    external_parent_id=ExternalParentId(external_parent_id),
                    url=Url(url)
                )
                yield item

            except websockets.exceptions.ConnectionClosed:
                logging.error(f"[BlueSky] Connection closed unexpectedly: {endpoint}")
                break
            except Exception as e:
                logging.exception(f"[BlueSky] Error in consumer ({endpoint}): {e}")

                

async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length, skip_probability = read_parameters(parameters)
    yielded_items = 0

    # Создаём список async-генераторов
    streams = [connect_to_jetstream(endpoint, maximum_items_to_collect) for endpoint in jetstreams_array_endpoints]

    # Запускаем все соединения параллельно и обрабатываем их корректно
    for stream in streams:
        try:
            async for item in stream:  # Теперь корректно обрабатываем async-генератор
                if yielded_items >= maximum_items_to_collect:
                    return
                
                if len(item.content) >= min_post_length:
                    yield item
                    yielded_items += 1
        except Exception as e:
            logging.exception(f"[Bluesky] Error in stream: {e}")

    logging.info(f"[Bluesky] Found {yielded_items} items, session is over.")
