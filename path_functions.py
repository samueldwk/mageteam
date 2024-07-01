
import os

def dl_folder():
    # Get the current user's home directory
    home_directory = os.path.expanduser("~")

    # Join the home directory with the standard "Downloads" folder
    downloads_folder = os.path.join(home_directory, "Downloads")

    return downloads_folder
