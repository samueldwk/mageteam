# import pandas as pd
# import json

# # Define the file path
# file_path = r"C:\Users\Samuel Kim\Desktop\Magento API\orderList2.json"

# # Load the JSON data
# with open(file_path, "r", encoding="utf-8") as file:
#     data = json.load(file)

# # Extract relevant order-level fields from the JSON
# structured_data = []
# for order in data:
#     structured_data.append(
#         {
#             "Increment ID": order["increment_id"]["$value"],
#             "Order ID": order["order_id"]["$value"],
#             "Store ID": order["store_id"]["$value"],
#             "Created At": order["created_at"]["$value"],
#             "Updated At": order["updated_at"]["$value"],
#             "Customer ID": order["customer_id"]["$value"],
#             "Grand Total": order["grand_total"]["$value"],
#             "Total Paid": order["total_paid"]["$value"],
#             "Total Qty Ordered": order["total_qty_ordered"]["$value"],
#             "Shipping Amount": order["shipping_amount"]["$value"],
#             "Discount Amount": order["discount_amount"]["$value"],
#             "Subtotal": order["subtotal"]["$value"],
#             "Order Status": order["status"]["$value"],
#             "Customer Email": order["customer_email"]["$value"],
#             "Customer Name": f"{order['customer_firstname']['$value']} {order['customer_lastname']['$value']}",
#             "Billing Name": order["billing_name"]["$value"],
#             "Shipping Name": order["shipping_name"]["$value"],
#             "Shipping Method": order["shipping_method"]["$value"],
#             "Shipping Description": order["shipping_description"]["$value"],
#             "Coupon Code": order["coupon_code"]["$value"]
#             if "coupon_code" in order
#             else None,
#             "Discount Description": order["discount_description"]["$value"]
#             if "discount_description" in order
#             else None,
#             "Currency Code": order["order_currency_code"]["$value"],
#             "Customer DOB": order["customer_dob"]["$value"]
#             if "customer_dob" in order
#             else None,
#             "Customer Taxvat": order["customer_taxvat"]["$value"]
#             if "customer_taxvat" in order
#             else None,
#             "Total Items Count": order["total_item_count"]["$value"],
#             "Remote IP": order["remote_ip"]["$value"],
#         }
#     )

# # Convert to DataFrame
# df = pd.DataFrame(structured_data)

# # Display DataFrame
# print(df)


######

import pandas as pd
import json

# Define the file path
file_path = r"C:\Users\Samuel Kim\Desktop\Magento API\orderList2.json"

# Load the JSON data
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Extract relevant order-level fields from the JSON
structured_data = []
for order in data:
    structured_data.append(
        {
            "Increment ID": order.get("increment_id", {}).get("$value"),
            "Order ID": order.get("order_id", {}).get("$value"),
            "Store ID": order.get("store_id", {}).get("$value"),
            "Created At": order.get("created_at", {}).get("$value"),
            "Updated At": order.get("updated_at", {}).get("$value"),
            "Customer ID": order.get("customer_id", {}).get("$value"),
            "Grand Total": order.get("grand_total", {}).get("$value"),
            "Total Paid": order.get("total_paid", {}).get(
                "$value", None
            ),  # Handling missing Total Paid
            "Total Qty Ordered": order.get("total_qty_ordered", {}).get(
                "$value"
            ),
            "Shipping Amount": order.get("shipping_amount", {}).get("$value"),
            "Discount Amount": order.get("discount_amount", {}).get("$value"),
            "Subtotal": order.get("subtotal", {}).get("$value"),
            "Order Status": order.get("status", {}).get("$value"),
            "Customer Email": order.get("customer_email", {}).get("$value"),
            "Customer Name": f"{order.get('customer_firstname', {}).get('$value', '')} {order.get('customer_lastname', {}).get('$value', '')}".strip(),
            "Billing Name": order.get("billing_name", {}).get("$value"),
            "Shipping Name": order.get("shipping_name", {}).get("$value"),
            "Shipping Method": order.get("shipping_method", {}).get("$value"),
            "Shipping Description": order.get("shipping_description", {}).get(
                "$value"
            ),
            "Coupon Code": order.get("coupon_code", {}).get(
                "$value", None
            ),  # Handling missing Coupon Code
            "Discount Description": order.get("discount_description", {}).get(
                "$value", None
            ),
            "Currency Code": order.get("order_currency_code", {}).get(
                "$value"
            ),
            "Customer DOB": order.get("customer_dob", {}).get("$value", None),
            "Customer Taxvat": order.get("customer_taxvat", {}).get(
                "$value", None
            ),
            "Total Items Count": order.get("total_item_count", {}).get(
                "$value"
            ),
            "Remote IP": order.get("remote_ip", {}).get("$value"),
        }
    )

# Convert to DataFrame
df = pd.DataFrame(structured_data)

# Display DataFrame
print(df)
