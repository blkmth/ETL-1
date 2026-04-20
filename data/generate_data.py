## fichier de genration de data avec faker

import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker( ['FR_FR', 'en_US', 'de_DE'])
random.seed(42)

PRODUCTS = {
    "Électronique": ["Laptop Pro", "Smartphone X", "Tablette Ultra", "Écouteurs BT", "Webcam 4K"],
    "Mobilier":["Bureau Ergonomique", "Chaise Gaming", "Étagère", "Lampe LED", "Caisson"],
    "Logiciels":["Suite Office", "Antivirus Pro", "IDE Premium", "VPN Annuel", "Licence CAO"],
    "Accessoires":["Souris Sans-fil", "Clavier Mécanique", "Hub USB-C", "Tapis Bureau", "Support PC"],
}

PRICE_RANGE = {
    "Électronique": (89.99,1499.99),
    "Mobilier":(39.99,799.99),
    "Logiciels":(19.99,299.99),
    "Accessoires":( 9.99,149.99),
}

COUNTRIES = ["France", "Belgique", "Suisse", "Canada", "Allemagne", "Espagne"]

def generate_transaction(index: int) -> dict:
    """genere des transactions avec anomalies intentionnelles environs 15%"""

    category = random.choice(list(PRODUCTS.keys()))
    product = random.choice(PRODUCTS[category])
    quantity = random.randint(1,10)
    price_min,price_max = PRICE_RANGE[category]
    unit_price = round(random.uniform(price_min,price_max),  2)

    anomaly = random.random()
    if anomaly < 0.05 :
        unit_price = -unit_price      ## prix negatif  a rejeter
    elif anomaly < 0.08 :
        quantity = 0                  ## quantité zéro a rejeter
    elif anomaly < 0.12 :
        product = " " + product + " " ## esapce parasite a nettoyer
    elif anomaly < 0.15 :
        category = category.upper()    ## casse incherente a normaliser

    sale_date = datetime.now() - timedelta(days=random.randint(0,365))
    return {
        "transaction_id" : f"TXN-{index:06d}-{fake.lexify('????').upper()}",
        "customer_name": fake.name(),
        "product": product,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "sale_date": sale_date.strftime("%d-%m-%Y"),
        "country": random.choice(COUNTRIES),
    }

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    records = [generate_transaction(i) for i in range (1, 100001)]
    df = pd.DataFrame(records)
    df.to_csv("/home/charles-nguessan/Projects/elt_debutant/data/sales_raw.csv", index=False, encoding= "utf-8")

    print(f"{len(df)} transactions genérées  -> data/sales_raw.csv")
