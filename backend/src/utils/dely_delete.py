import threading
import os

def delayed_file_deletion(filepath, delay=15):
    """Deletes a file after a specified delay."""
    def delete_file():
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                print(f"File {filepath} deleted.")
        except Exception as e:
            print(f"Error deleting file {filepath}: {e}")

    timer = threading.Timer(delay, delete_file)
    timer.start()