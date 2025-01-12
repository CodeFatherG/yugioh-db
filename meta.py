from datetime import datetime
import json
import os

class Meta():
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "meta.json")

    def update(self, start_time: datetime, end_time: datetime, cards_processed: int, total_cards: int, updated_cards: int):
        entry = {
            "triggered_time": start_time.isoformat(),
            "completed_time": end_time.isoformat(),
            "time_taken": str(end_time - start_time),
            "cards_processed": cards_processed,
            "cards_found": total_cards,
            "cards_updated": len(updated_cards),
            "updated_cards": updated_cards
        }

        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: {self.path} contains invalid JSON. Starting with empty data.")
                data = []
            except Exception as e:
                print(f"Error reading {self.path}: {str(e)}. Starting with empty data.")
                data = []
        else:
            print(f"{self.path} doesn't exist. Creating new file.")
            data = []
        
        data.append(entry)
        
        try:
            with open(self.path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Updated {self.path}")
        except Exception as e:
            print(f"Error writing to {self.path}: {str(e)}")
        
        print("Time taken: " + str(end_time - start_time))
        print("Cards processed: " + str(cards_processed))
        print("Cards found: " + str(total_cards))
        print("Cards updated: " + str(len(updated_cards)))