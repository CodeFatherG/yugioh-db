import hashlib
from datetime import datetime
import json
import os
import requests
import argparse

def hash(text):
    hash_object = hashlib.md5(text.encode())
    md5_hash = hash_object.hexdigest()
    return md5_hash

main_attributes = [
    "id",
    "name",
    "type",
    "desc",
    "atk",
    "def",
    "level",
    "race",
    "attribute",
    "scale",
    "archetype",
    "linkval",
    "linkmarkers",
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
    # Base info
    card_info = {attribute: card.get(attribute) for attribute in main_attributes}
    # Images
    og_id = card_info["id"]
    images = card["card_images"]
    [main_images] = [image for image in images if image["id"] == og_id]
    card_info.update(
        {
            "image_url": main_images["image_url"],
            "image_url_small": main_images["image_url_small"],
            "image_url_cropped": main_images["image_url_cropped"],
        }
    )
    return card_info

def download_image(image_url, image_path):
    # Check if file exists
    if os.path.exists(image_path):
        return

    print("Downloading " + image_url)
    response = requests.get(image_url)
    with open(image_path, "wb") as f:
        f.write(response.content)

def download_images(card):
    card_id = card['id']
    image_dir = os.path.join(get_card_path(card_id), "images")
    os.makedirs(image_dir, exist_ok=True)
    download_image(card['image_url'], os.path.join(image_dir, "full.jpg"))
    download_image(card['image_url_small'], os.path.join(image_dir, "small.jpg"))
    download_image(card['image_url_cropped'], os.path.join(image_dir, "cropped.jpg"))

def save_card_info(card):
    info_path = os.path.join(get_card_path(card['id']), "info.json")

    print("Saving card info for " + card['name'])
    with open(info_path, "w") as w:
        json.dump(card, w)

def store_hash(card):
    card_id = card['id']
    card_hash = hash(json.dumps(card))
    hash_path = os.path.join(get_card_path(card_id), "hash.txt")
    with open(hash_path, "w") as w:
        w.write(card_hash)

def compare_hash(card) -> bool:
    card_id = card['id']
    card_hash = hash(json.dumps(card))
    hash_path = os.path.join(get_card_path(card_id), "hash.txt")
    if not os.path.exists(hash_path):
        return False
    with open(hash_path, "r") as r:
        old_hash = r.read()
    return card_hash == old_hash

def main():
    parser = argparse.ArgumentParser(description='Download Yu-Gi-Oh! card data')
    parser.add_argument('--card-count', type=int, help='Number of cards to download (optional)')
    args = parser.parse_args()

    response = requests.get("https://db.ygoprodeck.com/api/v7/cardinfo.php")
    cardinfo_json = response.json()
    
    if args.card_count:
        cards_to_process = args.card_count
    else:
        cards_to_process = len(cardinfo_json["data"])

    print(f"Processing {cards_to_process} cards...")

    with open("meta.txt", "w") as w:
        w.write(datetime.now().isoformat() + ":\t" + cards_to_process + " cards" + "\n")

    cards_processed = 0
    for card in cardinfo_json["data"]:
        try:
            card = process_card(card)
            if compare_hash(card):
                continue
            save_card_info(card)
            download_images(card)
            store_hash(card)

            cards_processed += 1
            if (cards_processed >= cards_to_process):
                break
        except Exception as e:
            print("Error processing card " + card["name"] + ":", e)

if __name__ == "__main__":
    main()