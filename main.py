import hashlib
from datetime import datetime
import json
import os
import argparse
import asyncio
import aiohttp

from download_session import DownloadSession
from meta import Meta

# def hash(text):
#     hash_object = hashlib.md5(text.encode())
#     md5_hash = hash_object.hexdigest()
#     return md5_hash

def get_data_path():
    script_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(script_path, "cards")
    os.makedirs(data_path, exist_ok=True)
    return data_path



async def main_async():
    parser = argparse.ArgumentParser(description='Download Yu-Gi-Oh! card data')
    parser.add_argument('--card-count', type=int, help='Number of cards to download (optional)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of cards to process in each batch')
    parser.add_argument('--concurrent-operations', type=int, default=10, help='Number of concurrent download operations')
    args = parser.parse_args()

    settings = {}
    if args.card_count:
        settings['download_count'] = args.card_count
    if args.batch_size:
        settings['batch_size'] = args.batch_size
    if args.concurrent_operations:
        settings['concurrent_operations'] = args.concurrent_operations

    session = DownloadSession(get_data_path(), 7, settings)
    await session.start()

if __name__ == "__main__":
    asyncio.run(main_async())