import pandas as pd
import requests
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from fpdf import FPDF


def download_image(url):
    """Download image from URL and return as PIL Image"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return Image.open(BytesIO(response.content))
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return None


def overlay_text_on_image(image, text):
    """Overlay text onto an image with a more transparent and larger box."""
    image = image.convert("RGBA")
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    try:
        font = ImageFont.truetype("arial.ttf", 16)  # Standard font size
    except IOError:
        font = ImageFont.load_default()

    # Get text size and adjust background rectangle with padding
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    padding = 15  # Increased padding for better spacing
    bg_rectangle = (
        5,
        5,  # Top-left corner
        text_width + padding,
        text_height + padding,  # Bottom-right corner
    )

    # Draw transparent rectangle (almost fully transparent)
    draw.rectangle(bg_rectangle, fill=(0, 0, 0, 80))  # 30% transparency

    # Draw text
    draw.text(
        (10, 10), text, font=font, fill=(255, 255, 255, 255)
    )  # White text
    image = Image.alpha_composite(image, overlay)
    return image.convert("RGB")  # Convert back to RGB for saving


def create_pdf_with_images(input_excel, output_pdf):
    """Read Excel, download images, overlay text, and save to PDF with 9 images per page in 3 columns, 3 rows."""
    df = pd.read_excel(input_excel)  # Skip first row to use correct headers

    # Check required columns
    required_columns = [
        "Codigo SKU",
        "Nome",
        "Foto",
        "Preço Lançamento",
        "Qtd Vendida",
        "MKP Final",
    ]
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Missing required columns in Excel file")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=10)

    images_per_row = 3
    images_per_page = 9
    count = 0
    x_offset_start = 10
    y_offset_start = 10
    x_spacing = 65  # Spacing between columns
    y_spacing = 90  # Spacing between rows

    for index, row in df.iterrows():
        if count % images_per_page == 0:
            pdf.add_page()
            y_offset = y_offset_start  # Reset y position for new page
            x_offset = x_offset_start  # Reset x position for new row

        image_url = row["Foto"]
        image = download_image(image_url)

        if image:
            # Convert RGBA images to RGB to avoid transparency issues
            if image.mode == "RGBA":
                image = image.convert("RGB")

            # Prepare text overlay
            text_overlay = f"SKU: {row['Codigo SKU']}\nNome: {row['Nome']}\nPreço de Lançamento: R$ {row['Preço Lançamento']}\nQTD Vendida: R$ {row['Qtd Vendida']}\nMKP Final: {row['MKP Final']}"
            image = overlay_text_on_image(image, text_overlay)

            # Maintain proportions while fitting into PDF
            img_width, img_height = image.size
            max_width, max_height = 60, 80  # Adjusted to fit 9 images per page
            aspect_ratio = img_width / img_height

            if img_width > max_width:
                img_width = max_width
                img_height = img_width / aspect_ratio
            if img_height > max_height:
                img_height = max_height
                img_width = img_height * aspect_ratio

            # Save and insert into PDF
            image_path = f"temp_image_{index}.jpg"
            image.save(image_path, "JPEG")
            pdf.image(
                image_path, x=x_offset, y=y_offset, w=img_width, h=img_height
            )

            # Update positions
            if count % images_per_row == 2:
                y_offset += y_spacing  # Move to the next row
                x_offset = x_offset_start  # Reset x position
            else:
                x_offset += x_spacing  # Move to the next column

            count += 1

    pdf.output(output_pdf)
    print(f"File saved: {output_pdf}")


# Usage example
input_file = r"C:\Users\Samuel Kim\Desktop\Dadri Mural da Riqueza.xlsx"  # Change this to your Excel file path
output_file = "products_with_images.pdf"
create_pdf_with_images(input_file, output_file)
