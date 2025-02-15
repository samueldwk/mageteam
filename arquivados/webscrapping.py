import requests
from bs4 import BeautifulSoup


def fetch_all_img_ids(base_url, page_param, max_pages):
    """
    Scrapes all pages of the website up to a manually defined maximum number of pages.

    Args:
        base_url (str): The base URL for the first page.
        page_param (str): The URL pattern to insert page numbers.
        max_pages (int): The maximum number of pages to scrape.

    Returns:
        list: A list of all extracted `id` attributes from <img> tags.
    """
    img_ids = []

    for page in range(1, max_pages + 1):
        # Construct the URL for the current page
        url = base_url if page == 1 else page_param.format(page=page)

        # Fetch the page content
        response = requests.get(url)
        if response.status_code != 200:
            print(
                f"Page {page} returned status {response.status_code}. Stopping."
            )
            break

        # Parse the HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all <img> elements with an 'id' attribute
        img_elements = soup.find_all("img", id=True)
        if not img_elements:
            print(f"No <img> tags with 'id' found on page {page}. Stopping.")
            break

        # Extract the 'id' values
        img_ids.extend([img["id"] for img in img_elements])

        print(
            f"Scraped page {page}, found {len(img_elements)} <img> tags with 'id'."
        )

    return img_ids


# Input the number of maximum pages manually
max_pages = int(input("Enter the maximum number of pages to scrape: "))

# Base URL and adaptable URL pattern
base_url = "https://loftystyle.com.br/todas-as-roupas/sort-by/position/sort-direction/asc"
page_param = "https://loftystyle.com.br/todas-as-roupas/page/{page}/sort-by/position/sort-direction/asc"

# Fetch img IDs
all_img_ids = fetch_all_img_ids(base_url, page_param, max_pages)

# Display the collected img IDs
print("Total <img> IDs Collected:", len(all_img_ids))
print(all_img_ids)
