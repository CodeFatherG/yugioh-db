import asyncio
from datetime import datetime
import json
import os
import aiohttp

class Card():
    def __init__(self, json: dict, path: str):
        self.__json = json
        self.__path = path

    def __get_card_path(self, path: str, id: int)-> str:
        card_path = os.path.join(path, str(id))
        os.makedirs(card_path, exist_ok=True)
        return card_path

    def __sync_card_info_id(self, card_id: int)-> bool:
        path = os.path.join(self.__get_card_path(self.__path, card_id), "info.json")

        modified_json = self.__json.copy()
        modified_json['id'] = card_id

        # Check if card info already exists
        if os.path.exists(path):
            with open(path, "r") as r:
                existing_card = json.load(r)
            
            if existing_card == self.__json:
                print("\tCard info already exists for " + self.__json['name'] + " with id " + str(card_id))
                return False

        print("\tSaving card info for " + self.__json['name'] + " with id " + str(card_id))
        with open(path, "w") as w:
            json.dump(self.__json, w, indent=4)

        return True

    def sync_card_info(self) -> bool:
        return any([self.__sync_card_info_id(card_id) for card_id in self.identities])
        
    
    async def __download_image_async(self, session: aiohttp.ClientSession, image_url: str, image_path: str) -> bool:
        print(f"\tDownloading {image_url} to {image_path}")
        start_time = datetime.now()
        
        try:
            async with session.get(image_url) as response:
                end_time = datetime.now()
                response_time = end_time - start_time
                
                if response.status != 200:
                    print(f"Failed to download image {image_url}: {response.status} ({response_time})")
                    return False
                
                content = await response.read()

                # Check if image already exists
                if os.path.exists(image_path):
                    with open(image_path, "rb") as f:
                        existing_content = f.read()
                    
                    if content == existing_content:
                        print(f"\tImage {image_url} already exists at {image_path} ({response_time})")
                        return False

                with open(image_path, "wb") as f:
                    f.write(content)
            
            print(f"\tDownloaded {image_url} to {image_path} ({response_time})")
            return True
        
        except Exception as e:
            print(f"Error downloading {image_url}: {str(e)}")

    def __get_image_dict_for_id(self, card_id: int) -> dict:
        return next(image for image in self.__json['card_images'] if image['id'] == card_id)

    async def download_images_async_for_id(self, session: aiohttp.ClientSession, card_id: int)-> bool:
        dir = os.path.join(self.__get_card_path(self.__path, card_id), "images")
        os.makedirs(dir, exist_ok=True)

        images = self.__get_image_dict_for_id(card_id)
        if not images:
            print(f"No images found for card id {card_id}")
            return False

        # images is a dict with keys 'image_url', 'image_url_small', 'image_url_cropped'
        tasks = []
        for image_type, url_key in [("full", 'image_url'), ("small", 'image_url_small'), ("cropped", 'image_url_cropped')]:
            path = os.path.join(dir, f"{image_type}.jpg")
            tasks.append(self.__download_image_async(session, images[url_key], path))
        
        # Check if any image returned True (i.e. was downloaded)
        if any(await asyncio.gather(*tasks)):
            print("\tImages downloaded for " + self.name)
            return True
        
        return False

    async def download_images_async(self, session: aiohttp.ClientSession)-> bool:
        return any([await self.download_images_async_for_id(session, card_id) for card_id in self.identities])
           
    async def sync(self, session: aiohttp.ClientSession):
        info_saved = self.sync_card_info()
        image_saved = await self.download_images_async(session)
        return info_saved or image_saved
    
    @property
    def name(self):
        return self.__json['name']
    
    @property
    def id(self):
        return self.__json['id']
    
    @property
    def identities(self):
        return list(set([image['id'] for image in self.__json['card_images']]))