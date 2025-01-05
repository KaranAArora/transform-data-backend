import os

def clean_up_file(filepath):
    """Clean up the temporary file after processing."""
    if os.path.exists(filepath):
        os.remove(filepath)
