from datetime import datetime
import json
import os

class Meta():
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "meta.json")

    def get_last_api_hash(self) -> str | None:
        """Get API response hash from last run"""
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, 'r') as f:
                data = json.load(f)
            if data and len(data) > 0:
                return data[-1].get('api_response_hash')
        except Exception:
            pass

        return None

    def get_last_full_verification_date(self) -> str | None:
        """Get date of last run that performed full hash verification"""
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, 'r') as f:
                data = json.load(f)

            # Search backwards for last run with full verification
            for entry in reversed(data):
                if entry.get('full_hash_verifications', 0) > 0:
                    # Check if it was a full run (not just sample)
                    if entry.get('cards_processed') == entry.get('cards_found'):
                        return entry['triggered_time']
        except Exception:
            pass

        return None

    def update(
        self,
        start_time: datetime,
        end_time: datetime,
        cards_processed: int,
        total_cards: int,
        updated_cards: list,
        api_response_hash: str,
        api_unchanged: bool = False,
        full_hash_checks: int = 0
    ):
        """Update metadata with run information"""
        entry = {
            "triggered_time": start_time.isoformat(),
            "completed_time": end_time.isoformat(),
            "time_taken": str(end_time - start_time),
            "cards_processed": cards_processed,
            "cards_found": total_cards,
            "cards_updated": len(updated_cards),
            "updated_cards": updated_cards,
            "api_response_hash": api_response_hash,
            "api_unchanged": api_unchanged,
            "full_hash_verifications": full_hash_checks
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