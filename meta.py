from datetime import datetime
import json
import os

class Meta():
    def __init__(self):
        self.path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "meta.json")
        self._ensure_new_format()

    def _ensure_new_format(self):
        """Migrate old array format to new object format with resume_state"""
        if not os.path.exists(self.path):
            return

        try:
            with open(self.path, 'r') as f:
                data = json.load(f)

            # Check if already in new format (has 'audit_log' key)
            if isinstance(data, dict) and 'audit_log' in data:
                return

            # Migrate from old array format to new object format
            if isinstance(data, list):
                new_data = {
                    "resume_state": None,
                    "audit_log": data
                }
                with open(self.path, 'w') as f:
                    json.dump(new_data, f, indent=2)
                print("Migrated meta.json to new format with resume capability")
        except Exception as e:
            print(f"Error migrating meta.json format: {str(e)}")

    def get_last_api_hash(self) -> str | None:
        """Get API response hash from last run"""
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, 'r') as f:
                data = json.load(f)

            # Handle new format (object with audit_log)
            if isinstance(data, dict) and 'audit_log' in data:
                audit_log = data['audit_log']
                if audit_log and len(audit_log) > 0:
                    return audit_log[-1].get('api_response_hash')
            # Handle old format (array) - shouldn't happen after migration
            elif isinstance(data, list) and len(data) > 0:
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

            # Get audit_log (handle both new and old format)
            audit_log = data.get('audit_log', data) if isinstance(data, dict) else data

            # Search backwards for last run with full verification
            for entry in reversed(audit_log):
                if entry.get('full_hash_verifications', 0) > 0:
                    # Check if it was a full run (not just sample)
                    if entry.get('cards_processed') == entry.get('cards_found'):
                        return entry['triggered_time']
        except Exception:
            pass

        return None

    def get_resume_state(self) -> dict | None:
        """Get current resume state, or None if not resuming"""
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, 'r') as f:
                data = json.load(f)

            # Only new format has resume_state
            if isinstance(data, dict) and 'resume_state' in data:
                return data['resume_state']
        except Exception as e:
            print(f"Error reading resume state: {str(e)}")

        return None

    def save_resume_state(self, index: int, api_hash: str, target: int, found: int, session_id: str):
        """Update resume state after processing a batch"""
        try:
            # Read current data
            if os.path.exists(self.path):
                with open(self.path, 'r') as f:
                    data = json.load(f)
            else:
                data = {"resume_state": None, "audit_log": []}

            # Ensure new format
            if isinstance(data, list):
                data = {"resume_state": None, "audit_log": data}

            # Update resume state
            data['resume_state'] = {
                "last_processed_index": index,
                "api_hash_at_resume": api_hash,
                "target_updates": target,
                "updates_found": found,
                "session_started": session_id
            }

            # Write back
            with open(self.path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving resume state: {str(e)}")

    def clear_resume_state(self):
        """Clear resume state (API changed, full verification, or completed)"""
        try:
            if not os.path.exists(self.path):
                return

            with open(self.path, 'r') as f:
                data = json.load(f)

            # Ensure new format
            if isinstance(data, list):
                data = {"resume_state": None, "audit_log": data}

            # Clear resume state
            if 'resume_state' in data:
                data['resume_state'] = None

                with open(self.path, 'w') as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error clearing resume state: {str(e)}")

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
                data = {"resume_state": None, "audit_log": []}
            except Exception as e:
                print(f"Error reading {self.path}: {str(e)}. Starting with empty data.")
                data = {"resume_state": None, "audit_log": []}
        else:
            print(f"{self.path} doesn't exist. Creating new file.")
            data = {"resume_state": None, "audit_log": []}

        # Ensure new format
        if isinstance(data, list):
            data = {"resume_state": None, "audit_log": data}

        # Append to audit_log
        if 'audit_log' not in data:
            data['audit_log'] = []
        data['audit_log'].append(entry)
        
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