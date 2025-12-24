import asyncio
from datetime import datetime
import hashlib
import random
import aiohttp

from card import Card
from meta import Meta

class DownloadSession:
    __download_path: str
    __api_version: int
    __settings: dict
    __card_info: list | None
    __api_response_hash: str
    __api_unchanged: bool

    def __init__(self, download_path: str, api_version: int, settings: dict = {}):
        self.__download_path = download_path
        self.__api_version = api_version
        self.__settings = settings
        self.__card_info = None
        self.__api_response_hash = ""
        self.__api_unchanged = False

    @property
    def card_info_uri(self):
        return f'https://db.ygoprodeck.com/api/v{self.__api_version}/cardinfo.php'

    async def __get_card_info(self, session: aiohttp.ClientSession) -> list:
        """Fetch card info from API and check if changed"""
        if self.__card_info is None:
            async with session.get(self.card_info_uri) as response:
                raw_json = await response.text()

                # Compute hash of API response
                current_hash = hashlib.sha256(raw_json.encode()).hexdigest()
                self.__api_response_hash = current_hash

                # Check if API changed since last run
                last_hash = Meta().get_last_api_hash()
                if last_hash == current_hash:
                    print(f"API response unchanged (hash: {current_hash[:8]}...)")
                    self.__api_unchanged = True
                else:
                    if last_hash:
                        print(f"API response changed: {last_hash[:8]}... -> {current_hash[:8]}...")
                    else:
                        print(f"First run or no previous hash (hash: {current_hash[:8]}...)")
                    self.__api_unchanged = False

                import json as json_lib
                json_data = json_lib.loads(raw_json)
                self.__card_info = json_data['data']

        return self.__card_info

    async def __verify_sample_cards(self, session: aiohttp.ClientSession, sample_size: int):
        """Verify a random sample of cards for integrity checking"""
        sample = random.sample(self.card_info, min(sample_size, len(self.card_info)))
        print(f"Verifying {len(sample)} sample cards with full hash checks...")

        semaphore = asyncio.Semaphore(self.concurrent_operations)
        updated_cards = []

        for card_data in sample:
            card = Card(card_data, self.download_path)
            # Force full hash verification for sample
            card_saved = await card.sync(session, force_full_check=True)
            if card_saved:
                updated_cards.append(card.name)

        return updated_cards

    def __should_do_full_verification(self) -> bool:
        """Determine if this run should do full hash verification"""
        last_full_check = Meta().get_last_full_verification_date()

        if not last_full_check:
            return True  # Never done before

        try:
            from datetime import datetime
            last_date = datetime.fromisoformat(last_full_check)
            days_since = (datetime.now() - last_date).days
            return days_since >= 28  # Every 4 weeks
        except Exception:
            return True  # Error parsing date, do full check to be safe

    async def __process_single_card(self, session, info, semaphore, force_full_check: bool = False) -> str | None:
        async with semaphore:
            try:
                card_info = Card(info, self.download_path)
                card_saved = await card_info.sync(session, force_full_check)
                if card_saved:
                    return card_info.name
                return None

            except Exception as e:
                print(f"Error processing card {info['name']}: {e}")
                return None

    async def __process_batch(self, session: aiohttp.ClientSession, batch: [], semaphore, force_full_check: bool = False):
        tasks = [asyncio.create_task(self.__process_single_card(session, card, semaphore, force_full_check)) for card in batch]
        results = await asyncio.gather(*tasks)
        updated_cards = [card for card in results if card is not None]

        return updated_cards

    async def start(self):
        """Main download orchestration"""
        # Fetch API data and check if changed
        async with aiohttp.ClientSession() as session:
            await self.__get_card_info(session)

        # Check if we need full hash verification
        force_full_check = self.__should_do_full_verification()

        if force_full_check:
            print("Performing periodic full hash verification (every 4 weeks)")

        # Early exit if API unchanged (unless full verification needed)
        if self.__api_unchanged and not force_full_check:
            print("API response unchanged - performing sample verification only")
            start_time = datetime.now()

            # Verify 5% sample (approximately 650 cards for 13K total)
            sample_size = max(int(self.card_count * 0.05), 10)

            async with aiohttp.ClientSession() as session:
                updated_cards = await self.__verify_sample_cards(session, sample_size)

            end_time = datetime.now()

            Meta().update(
                start_time,
                end_time,
                sample_size,  # cards_processed
                self.card_count,  # total_cards
                updated_cards,
                self.__api_response_hash,
                api_unchanged=True,
                full_hash_checks=sample_size
            )

            print(f"Sample verification complete: {len(updated_cards)} cards updated")
            return

        # Normal processing
        print(f"Processing {self.cards_to_download} cards in batches of {self.batch_size}...")
        start_time = datetime.now()
        all_updated_cards = []
        total_processed = 0
        full_hash_checks = 0

        semaphore = asyncio.Semaphore(self.concurrent_operations)

        async with aiohttp.ClientSession() as session:
            for i in range(0, self.card_count, self.batch_size):
                batch = self.card_info[i:i + self.batch_size]
                batch_updated = await self.__process_batch(
                    session, batch, semaphore, force_full_check
                )
                all_updated_cards.extend(batch_updated)

                total_processed += len(batch)
                if force_full_check:
                    full_hash_checks += len(batch)

                print(f"Processed {total_processed}/{self.card_count} cards. Updated {len(batch_updated)} cards.")

                if len(all_updated_cards) >= self.cards_to_download:
                    break

        end_time = datetime.now()

        Meta().update(
            start_time,
            end_time,
            total_processed,
            self.card_count,
            all_updated_cards,
            self.__api_response_hash,
            api_unchanged=self.__api_unchanged,
            full_hash_checks=full_hash_checks
        )

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
    