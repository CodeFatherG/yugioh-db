import asyncio
from datetime import datetime
import hashlib
import json
import os
import aiohttp


def compute_file_hash(filepath: str) -> str:
    """Compute SHA-256 hash of file content"""
    hash_obj = hashlib.sha256()
    with open(filepath, 'rb') as f:
        # Read in chunks for memory efficiency
        for chunk in iter(lambda: f.read(8192), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def compute_partial_hash(filepath: str, byte_count: int) -> str:
    """Compute SHA-256 hash of first N bytes of file"""
    hash_obj = hashlib.sha256()
    with open(filepath, 'rb') as f:
        data = f.read(byte_count)
        hash_obj.update(data)
    return hash_obj.hexdigest()


async def verify_image_with_range_request(
    session: aiohttp.ClientSession,
    url: str,
    local_path: str,
    hash_path: str,
    force_full_check: bool = False
) -> tuple[bool, bool]:
    """
    Verify if image needs updating using Range request.

    Returns: (needs_download, verified_full_hash)
    - needs_download: True if image should be downloaded
    - verified_full_hash: True if full hash was verified (for periodic checking)
    """
    # Check if both image and hash exist
    if not (os.path.exists(local_path) and os.path.exists(hash_path)):
        return (True, False)  # Must download

    # Read stored hash
    try:
        with open(hash_path, 'r') as f:
            stored_hash = f.read().strip()
    except Exception:
        return (True, False)  # Hash file corrupted, re-download

    # Periodic full verification
    if force_full_check:
        current_hash = compute_file_hash(local_path)
        if current_hash != stored_hash:
            return (True, True)  # Hash mismatch, re-download
        return (False, True)  # Verified, no download needed

    # Fast path: HTTP Range request for first 8KB
    try:
        headers = {'Range': 'bytes=0-8191'}
        async with session.get(url, headers=headers) as response:
            if response.status not in (200, 206):
                # Server doesn't support Range or error, fall back to full download
                return (True, False)

            partial_content = await response.read()
    except Exception as e:
        print(f"\tRange request failed for {url}: {e}")
        return (True, False)

    # Compare partial hash
    partial_hash = hashlib.sha256(partial_content).hexdigest()
    stored_partial_hash = compute_partial_hash(local_path, len(partial_content))

    if partial_hash != stored_partial_hash:
        return (True, False)  # Different, must download

    # Partial hash matches - assume file unchanged
    return (False, False)


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
        
    
    async def __download_image_async(
        self,
        session: aiohttp.ClientSession,
        image_url: str,
        image_path: str,
        force_full_check: bool = False
    ) -> bool:
        """
        Download image if needed, using hash verification to avoid unnecessary downloads.

        Returns: True if image was downloaded/updated, False otherwise
        """
        hash_path = image_path + ".hash"

        # Check if we need to download using Range request
        needs_download, verified_full = await verify_image_with_range_request(
            session, image_url, image_path, hash_path, force_full_check
        )

        if not needs_download:
            if verified_full:
                print(f"\tImage verified (full hash): {image_url}")
            else:
                print(f"\tImage unchanged (range check): {image_url}")
            return False

        # Download full image
        print(f"\tDownloading {image_url} to {image_path}")
        start_time = datetime.now()

        try:
            async with session.get(image_url) as response:
                end_time = datetime.now()
                response_time = end_time - start_time

                if response.status != 200:
                    print(f"Failed to download {image_url}: {response.status} ({response_time})")
                    return False

                content = await response.read()

                # Write to temp file first (atomic write)
                temp_path = image_path + ".tmp"
                with open(temp_path, "wb") as f:
                    f.write(content)

                # Compute hash of downloaded content
                new_hash = hashlib.sha256(content).hexdigest()

                # Write hash to temp file
                temp_hash_path = hash_path + ".tmp"
                with open(temp_hash_path, "w") as f:
                    f.write(new_hash)

                # Atomic rename (ensures consistency)
                os.rename(temp_path, image_path)
                os.rename(temp_hash_path, hash_path)

            print(f"\tDownloaded {image_url} ({response_time})")
            return True

        except Exception as e:
            print(f"Error downloading {image_url}: {str(e)}")
            # Cleanup temp files if they exist
            temp_path = image_path + ".tmp"
            temp_hash_path = hash_path + ".tmp"
            for temp_file in [temp_path, temp_hash_path]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            return False

    def __get_image_dict_for_id(self, card_id: int) -> dict:
        return next(image for image in self.__json['card_images'] if image['id'] == card_id)

    async def download_images_async_for_id(
        self,
        session: aiohttp.ClientSession,
        card_id: int,
        force_full_check: bool = False
    ) -> bool:
        """Download/verify images for a specific card ID"""
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
            tasks.append(self.__download_image_async(
                session, images[url_key], path, force_full_check
            ))

        # Check if any image returned True (i.e. was downloaded)
        results = await asyncio.gather(*tasks)
        if any(results):
            print(f"\tImages updated for {self.name}")
            return True

        return False

    async def download_images_async(
        self,
        session: aiohttp.ClientSession,
        force_full_check: bool = False
    ) -> bool:
        return any([await self.download_images_async_for_id(session, card_id, force_full_check) for card_id in self.identities])

    async def sync(
        self,
        session: aiohttp.ClientSession,
        force_full_check: bool = False
    ):
        """Sync card info and images in parallel"""
        # Run info save in thread pool (it's synchronous)
        info_task = asyncio.to_thread(self.sync_card_info)

        # Run image downloads async
        image_task = self.download_images_async(session, force_full_check)

        # Wait for both to complete
        info_saved, image_saved = await asyncio.gather(info_task, image_task)

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