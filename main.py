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

async def download_image_async(session, image_url, image_path):
    if os.path.exists(image_path):
        return

    print(f"\tDownloading {image_url} to {image_path}")
    start_time = datetime.now()
    
    try:
        async with session.get(image_url) as response:
            end_time = datetime.now()
            response_time = end_time - start_time
            
            if response_time.total_seconds() >= 1:
                response_time_str = f"{response_time.total_seconds():.2f}s"
            else:
                response_time_str = f"{response_time.total_seconds() * 1000:.2f}ms"
            
            if response.status != 200:
                print(f"Failed to download image {image_url}: {response.status} ({response_time_str})")
                return
            
            content = await response.read()
            with open(image_path, "wb") as f:
                f.write(content)
        
        print(f"\tDownloaded {image_url} to {image_path} ({response_time_str})")
    
    except Exception as e:
        print(f"Error downloading {image_url}: {str(e)}")

async def download_images_async(session, card):
    card_id = card['id']
    image_dir = os.path.join(get_card_path(card_id), "images")
    os.makedirs(image_dir, exist_ok=True)

    tasks = [
        download_image_async(session, card['image_url'], os.path.join(image_dir, "full.jpg")),
        download_image_async(session, card['image_url_small'], os.path.join(image_dir, "small.jpg")),
        download_image_async(session, card['image_url_cropped'], os.path.join(image_dir, "cropped.jpg"))
    ]
    await asyncio.gather(*tasks)

def save_card_info(card):
    info_path = os.path.join(get_card_path(card['id']), "info.json")
    print("\tSaving card info for " + card['name'])
    with open(info_path, "w") as w:
        json.dump(card, w, indent=4)

def store_hash(card):
    card_id = card['id']
    card_hash = hash(json.dumps(card, sort_keys=True))
    hash_path = os.path.join(get_card_path(card_id), "hash.txt")
    with open(hash_path, "w") as w:
        w.write(card_hash)

def compare_hash(card) -> bool:
    card_id = card['id']
    card_hash = hash(json.dumps(card, sort_keys=True))
    hash_path = os.path.join(get_card_path(card_id), "hash.txt")
    if not os.path.exists(hash_path):
        return False
    with open(hash_path, "r") as r:
        old_hash = r.read()
    return card_hash == old_hash

async def sync_card_async(session, card):
    save_card_info(card)
    await download_images_async(session, card)
    store_hash(card)

def update_meta_json(start_time, end_time, cards_processed, total_cards, cards_added):
    meta_file = "meta.json"
    entry = {
        "triggered_time": start_time.isoformat(),
        "completed_time": end_time.isoformat(),
        "time_taken": str(end_time - start_time),
        "cards_processed": cards_processed,
        "cards_found": total_cards,
        "cards_added": cards_added
    }
    
    if os.path.exists(meta_file):
        with open(meta_file, "r") as f:
            data = json.load(f)
    else:
        data = []
    
    data.append(entry)
    
    with open(meta_file, "w") as f:
        json.dump(data, f, indent=4)

    print("Updated meta.json")
    print("Time taken: " + str(end_time - start_time))
    print("Cards processed: " + str(cards_processed))
    print("Cards found: " + str(total_cards))
    print("Cards added: " + str(cards_added))

async def process_batch(session, batch, semaphore, total_processed, total_to_process):
    async def process_single_card(card):
        async with semaphore:
            try:
                card_info = process_card(card)
                if not compare_hash(card_info):
                    await sync_card_async(session, card_info)
                    return True
            except Exception as e:
                print(f"Error processing card {card['name']}: {e}")
        return False

    tasks = [asyncio.create_task(process_single_card(card)) for card in batch]
    results = await asyncio.gather(*tasks)
    cards_added = sum(results)
    
    total_processed += len(batch)
    print(f"Processed {total_processed}/{total_to_process} cards. Added {cards_added} new cards.")
    
    return cards_added, total_processed

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

    cards_to_process = min(args.card_count or len(cardinfo_json["data"]), len(cardinfo_json["data"]))
    batch_size = args.batch_size

    print(f"Processing {cards_to_process} cards in batches of {batch_size}...")

    start_time = datetime.now()
    
    cards_added = 0
    total_processed = 0
    semaphore = asyncio.Semaphore(10)  # Limit concurrent operations

    async with aiohttp.ClientSession() as session:
        for i in range(0, cards_to_process, batch_size):
            batch = cardinfo_json["data"][i:i+batch_size]
            batch_added, total_processed = await process_batch(session, batch, semaphore, total_processed, cards_to_process)
            cards_added += batch_added
            
            if total_processed >= cards_to_process:
                break

    end_time = datetime.now()

    update_meta_json(start_time, 
                     end_time, 
                     total_processed, 
                     len(cardinfo_json["data"]),
                     cards_added)

if __name__ == "__main__":
    asyncio.run(main_async())