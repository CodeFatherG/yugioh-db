import hashlib
from datetime import datetime
import json
import os
import argparse
import asyncio
import aiohttp

def hash(text):
    hash_object = hashlib.md5(text.encode())
    md5_hash = hash_object.hexdigest()
    return md5_hash

main_attributes = [
    "id", "name", "type", "desc", "atk", "def", "level", "race",
    "attribute", "scale", "archetype", "linkval", "linkmarkers",
]

def get_data_path():
    script_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(script_path, "cards")
    os.makedirs(data_path, exist_ok=True)
    return data_path

def get_card_path(card_id):
    card_path = os.path.join(get_data_path(), str(card_id))
    os.makedirs(card_path, exist_ok=True)
    return card_path

def process_card(card):
    card_info = {attribute: card.get(attribute) for attribute in main_attributes}
    og_id = card_info["id"]
    images = card["card_images"]
    [main_images] = [image for image in images if image["id"] == og_id]
    card_info.update({
        "image_url": main_images["image_url"],
        "image_url_small": main_images["image_url_small"],
        "image_url_cropped": main_images["image_url_cropped"],
    })
    return card_info

async def download_image_async(session, image_url, image_path) -> bool:
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

async def download_images_async(session, card):
    card_id = card['id']
    image_dir = os.path.join(get_card_path(card_id), "images")
    os.makedirs(image_dir, exist_ok=True)

    tasks = []
    for image_type, url_key in [("full", 'image_url'), ("small", 'image_url_small'), ("cropped", 'image_url_cropped')]:
        image_path = os.path.join(image_dir, f"{image_type}.jpg")
        tasks.append(download_image_async(session, card[url_key], image_path))
    
    # Check if any image returned True (i.e. was downloaded)
    if any(await asyncio.gather(*tasks)):
        print("\tImages downloaded for " + card['name'])
        return True
    
    return False

def save_card_info(card) -> bool:
    info_path = os.path.join(get_card_path(card['id']), "info.json")

    # Check if card info already exists
    if os.path.exists(info_path):
        with open(info_path, "r") as r:
            existing_card = json.load(r)
        
        if existing_card == card:
            print("\tCard info already exists for " + card['name'])
            return False

    print("\tSaving card info for " + card['name'])
    with open(info_path, "w") as w:
        json.dump(card, w, indent=4)

    return True

async def sync_card_async(session, card) -> bool:
    info_saved = save_card_info(card)
    image_saved = await download_images_async(session, card)
    return info_saved or image_saved

def update_meta_json(start_time, end_time, cards_processed, total_cards, updated_cards):
    meta_file = "meta.json"
    entry = {
        "triggered_time": start_time.isoformat(),
        "completed_time": end_time.isoformat(),
        "time_taken": str(end_time - start_time),
        "cards_processed": cards_processed,
        "cards_found": total_cards,
        "cards_updated": len(updated_cards),
        "updated_cards": updated_cards
    }
    
    if os.path.exists(meta_file):
        try:
            with open(meta_file, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: {meta_file} contains invalid JSON. Starting with empty data.")
            data = []
        except Exception as e:
            print(f"Error reading {meta_file}: {str(e)}. Starting with empty data.")
            data = []
    else:
        print(f"{meta_file} doesn't exist. Creating new file.")
        data = []
    
    data.append(entry)
    
    try:
        with open(meta_file, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Updated {meta_file}")
    except Exception as e:
        print(f"Error writing to {meta_file}: {str(e)}")
    
    print("Time taken: " + str(end_time - start_time))
    print("Cards processed: " + str(cards_processed))
    print("Cards found: " + str(total_cards))
    print("Cards updated: " + str(len(updated_cards)))

async def process_single_card(session, card, semaphore) -> str | None:
    async with semaphore:
        try:
            card_info = process_card(card)
            card_saved = await sync_card_async(session, card_info)
            if (card_saved):
                return card_info['name']  # Return the name of the updated card
            return None
        
        except Exception as e:
            print(f"Error processing card {card['name']}: {e}")
            return None

async def process_batch(session, batch, semaphore):
    tasks = [asyncio.create_task(process_single_card(session, card, semaphore)) for card in batch]
    results = await asyncio.gather(*tasks)
    updated_cards = [card for card in results if card is not None]
        
    return updated_cards

async def main_async():
    parser = argparse.ArgumentParser(description='Download Yu-Gi-Oh! card data')
    parser.add_argument('--card-count', type=int, help='Number of cards to download (optional)')
    parser.add_argument('--card-id', type=int, help='The card id to sync (optional)')
    parser.add_argument('--card-name', type=str, help='The card name to sync (optional)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of cards to process in each batch')
    args = parser.parse_args()

    async with aiohttp.ClientSession() as session:
        response = await session.get("https://db.ygoprodeck.com/api/v7/cardinfo.php")
        cardinfo_json = await response.json()

    card_count = len(cardinfo_json["data"])
    cards_to_process = min(args.card_count or card_count, card_count)
    batch_size = args.batch_size

    print(f"Processing {cards_to_process} cards in batches of {batch_size}...")

    start_time = datetime.now()
    
    all_updated_cards = []
    total_processed = 0
    semaphore = asyncio.Semaphore(10)  # Limit concurrent operations

    async with aiohttp.ClientSession() as session:
        for i in range(0, card_count, batch_size):
            batch = cardinfo_json["data"][i:i+batch_size]
            batch_updated = await process_batch(session, batch, semaphore)
            all_updated_cards.extend(batch_updated)

            total_processed += len(batch)
            print(f"Processed {total_processed}/{card_count} cards. Added or updated {len(batch_updated)} cards.")
            
            if (len(all_updated_cards) >= cards_to_process):
                break

    end_time = datetime.now()

    update_meta_json(start_time, 
                     end_time, 
                     total_processed, 
                     len(cardinfo_json["data"]),
                     all_updated_cards)
    
if __name__ == "__main__":
    asyncio.run(main_async())