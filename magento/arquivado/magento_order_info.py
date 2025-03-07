import pandas as pd
import json

# Load the JSON file (modify this path accordingly)
file_path = r"C:\Users\Samuel Kim\Desktop\Magento API\orderInfo.json"
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Extract order-level details
order_details = {
    "Increment ID": data["increment_id"]["$value"],
    "Order ID": data["order_id"]["$value"],
    "Store ID": data["store_id"]["$value"],
    "Created At": data["created_at"]["$value"],
    "Updated At": data["updated_at"]["$value"],
    "Customer ID": data["customer_id"]["$value"],
    "Grand Total": data["grand_total"]["$value"],
    "Shipping Method": data["shipping_method"]["$value"],
    "Order Status": data["status"]["$value"],
    "Customer Email": data["customer_email"]["$value"],
    "Customer Name": f"{data['customer_firstname']['$value']} {data['customer_lastname']['$value']}",
    "Billing City": data["billing_address"]["city"]["$value"],
    "Billing Region": data["billing_address"]["region"]["$value"],
    "Billing Country": data["billing_address"]["country_id"]["$value"],
    "Shipping City": data["shipping_address"]["city"]["$value"],
    "Shipping Region": data["shipping_address"]["region"]["$value"],
    "Shipping Country": data["shipping_address"]["country_id"]["$value"],
    "Payment Method": data["payment"]["method"]["$value"],
    "CC Type": data["payment"]["cc_type"]["$value"],
    "CC Last4": data["payment"]["cc_last4"]["$value"],
}

# Extract item-level details
items_data = []
for item in data["items"]["item"]:
    items_data.append(
        {
            "Increment ID": data["increment_id"]["$value"],
            "Order ID": data["order_id"]["$value"],
            "Item ID": item["item_id"]["$value"],
            "Product ID": item["product_id"]["$value"],
            "Product Name": item["name"]["$value"],
            "SKU": item["sku"]["$value"],
            "Price": item["price"]["$value"],
            "Quantity Ordered": item["qty_ordered"]["$value"],
        }
    )

# Convert to Pandas DataFrames
order_df = pd.DataFrame([order_details])
items_df = pd.DataFrame(items_data)

# Display DataFrames
print("\nOrder Summary:")
print(order_df)

print("\nOrder Items:")
print(items_df)

# Optionally, export to CSV
order_df.to_csv("order_summary.csv", index=False)
items_df.to_csv("order_items.csv", index=False)
