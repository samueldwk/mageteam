# Função para usar app Pushover que serve para mandar a notificações

import os
import dotenv
import requests

dotenv.load_dotenv()


# Pushover notification function
def send_pushover_notification(message):
    """Send a Pushover notification with the provided message."""
    PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN")  # App token
    PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")  # User key

    if not PUSHOVER_APP_TOKEN or not PUSHOVER_USER_KEY:
        raise ValueError(
            "Pushover credentials (App Token and User Key) must be set as environment variables."
        )

    url = "https://api.pushover.net/1/messages.json"
    data = {
        "token": PUSHOVER_APP_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "message": message,
    }

    response = requests.post(url, data=data)
    if response.status_code == 200:
        print("Notification sent successfully!")
    else:
        print(
            f"Failed to send notification: {response.status_code}, {response.text}"
        )
