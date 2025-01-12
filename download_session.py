import asyncio
from datetime import datetime
import aiohttp

from card import Card
from meta import Meta

class DownloadSession:
    __download_path: str
    __api_version: int
    __settings: dict
    __card_info: list | None

    def __init__(self, download_path: str, api_version: int, settings: dict = {}):
        self.__download_path = download_path
        self.__api_version = api_version
        self.__settings = settings
        self.__card_info = None

    @property
    def card_info_uri(self):
        return f'https://db.ygoprodeck.com/api/v{self.__api_version}/cardinfo.php'

    async def __get_card_info(self, session: aiohttp.ClientSession)-> list:
        if (self.__card_info is None):
            async with session.get(self.card_info_uri) as response:
                json = await response.json()
                self.__card_info = json['data']
        return self.__card_info
        
    async def __process_single_card(self, session, info, semaphore) -> str | None:
        async with semaphore:
            try:
                card_info = Card(info, self.download_path)
                card_saved = await card_info.sync(session)
                if (card_saved):
                    return card_info.name
                return None
            
            except Exception as e:
                print(f"Error processing card {info['name']}: {e}")
                return None

    async def __process_batch(self, session: aiohttp.ClientSession, batch: [], semaphore):
        tasks = [asyncio.create_task(self.__process_single_card(session, card, semaphore)) for card in batch]
        results = await asyncio.gather(*tasks)
        updated_cards = [card for card in results if card is not None]
            
        return updated_cards

    async def start(self):
        async with aiohttp.ClientSession() as session:
            await self.__get_card_info(session)

        print(f"Processing {self.cards_to_download} cards in batches of {self.batch_size}...")

        start_time = datetime.now()
        all_updated_cards = []
        total_processed = 0
        
        semaphore = asyncio.Semaphore(self.concurrent_operations)

        async with aiohttp.ClientSession() as session:
            for i in range(0, self.card_count, self.batch_size):
                batch = self.card_info[i:i + self.batch_size]
                batch_updated = await self.__process_batch(session, batch, semaphore)
                all_updated_cards.extend(batch_updated)

                total_processed += len(batch)
                print(f"Processed {total_processed}/{self.card_count} cards. Added or updated {len(batch_updated)} cards.")
                
                if (len(all_updated_cards) >= self.cards_to_download):
                    break

        end_time = datetime.now()

        Meta().update(start_time, 
                    end_time, 
                    total_processed, 
                    self.card_count,
                    all_updated_cards)

    @property
    def api_version(self)-> int:
        return self.__api_version
    
    @property
    def settings(self)-> dict:
        return self.__settings
    
    @property
    def card_info(self)-> list[str] | None:
        return self.__card_info
    
    @property
    def card_count(self)-> int:
        return len(self.card_info)
    
    @property
    def batch_size(self)-> int:
        if 'batch_size' not in self.__settings:
            return 10
        return self.__settings['batch_size']
    
    @property
    def cards_to_download(self)-> int:
        if 'download_count' not in self.__settings:
            return self.card_count
        return self.__settings['download_count']

    @property
    def download_path(self)-> str:
        return self.__download_path
    
    @property
    def concurrent_operations(self)-> int:
        if 'concurrent_operations' not in self.__settings:
            return 10
        return self.__settings['concurrent_operations']
    