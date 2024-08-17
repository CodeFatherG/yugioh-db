import hashlib
from datetime import datetime
import json
import os
import requests


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
    
    if os.path.exists(info_path):
        return
    
    with open(info_path, "w") as w:
        json.dump(card, w)

def main():
    with open("meta.txt", "w") as w:
        w.write(datetime.now().isoformat() + "\n")
    
    response = requests.get("https://db.ygoprodeck.com/api/v7/cardinfo.php")
    cardinfo_json = response.json()

    for card in cardinfo_json["data"]:
        card = process_card(card)
        save_card_info(card)
        download_images(card)

if __name__ == "__main__":
    main()

