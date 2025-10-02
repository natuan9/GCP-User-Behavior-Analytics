from pymongo import MongoClient
import json

# Kết nối tới MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["glamira_db"]
collection = db["summary"]

# Tìm những document có cart_products.option là string và khác ""
cursor = collection.find({
    "cart_products": {
        "$elemMatch": {
            "option": {
                "$type": "string",
                "$ne": ""
            }
        }
    }
})

output_file = "invalid_cart_products.jsonl"

with open(output_file, "w", encoding="utf-8") as f:
    for doc in cursor:
        for cart_product in doc.get("cart_products", []):
            if isinstance(cart_product.get("option"), str) and cart_product["option"] != "":
                # Ghi từng dòng JSON vào file
                f.write(json.dumps({
                    "_id": str(doc["_id"]),
                    "option": cart_product["option"]
                }, ensure_ascii=False) + "\n")

print(f"✅ Đã ghi kết quả vào file {output_file}")
