import json
import logging
import time
import random
from typing import Dict, Any
import argparse

from pygridgain import Client
from langchain_openai import OpenAIEmbeddings
from langchain.schema import Document
from langchain_gridgain.vectorstores import GridGainVectorStore
from langchain_gridgain.storage import GridGainStore

from data_loader import data_loader
from utils import connect_to_gridgain, initialize_embeddings_model, initialize_key_value_store, initialize_vector_store, str2bool

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Predefined products with their initial details
predefined_products = {
    "1": {"product": "Organic Bananas", "current_availability": "In Stock", "price": 2.49},
    "2": {"product": "Fresh Whole Milk", "current_availability": "In Stock", "price": 3.99},
    "3": {"product": "Organic Free-Range Eggs", "current_availability": "Low Stock", "price": 4.29},
    "4": {"product": "Greek Yogurt", "current_availability": "In Stock", "price": 3.79},
    "5": {"product": "Multigrain Bread", "current_availability": "In Stock", "price": 3.49},
    "6": {"product": "Chicken Breast", "current_availability": "In Stock", "price": 6.99},
    "7": {"product": "Fresh Salmon Fillet", "current_availability": "Limited Stock", "price": 12.99},
    "8": {"product": "Organic Spinach", "current_availability": "In Stock", "price": 2.99},
    "9": {"product": "Cherry Tomatoes", "current_availability": "In Stock", "price": 3.29},
    "10": {"product": "Almond Milk", "current_availability": "In Stock", "price": 4.49},
    "11": {"product": "Extra Virgin Olive Oil", "current_availability": "In Stock", "price": 7.99},
    "12": {"product": "Organic Apples", "current_availability": "In Stock", "price": 4.99},
    "13": {"product": "Ground Beef", "current_availability": "Low Stock", "price": 5.99},
    "14": {"product": "Tofu", "current_availability": "In Stock", "price": 2.79},
    "15": {"product": "Pasta", "current_availability": "In Stock", "price": 1.99},
    "16": {"product": "Coconut Water", "current_availability": "In Stock", "price": 3.49},
    "17": {"product": "Natural Honey", "current_availability": "Limited Stock", "price": 6.29},
    "18": {"product": "Fresh Mozzarella", "current_availability": "In Stock", "price": 4.99},
    "19": {"product": "Quinoa", "current_availability": "In Stock", "price": 5.49},
    "20": {"product": "Plant-Based Burger Patties", "current_availability": "Low Stock", "price": 5.79},
}

def generate_product_data() -> Dict[str, Any]:
    """
    Generate dynamic product data, but only update predefined products
    """
    
    # Simulating random updates for the predefined products
    availability_options = ["In Stock", "Low Stock", "Limited Stock"]
    delivery_time_options = ["15-30 minutes", "30-45 minutes", "45-60 minutes"]

    # Randomly pick a product from the predefined set
    product_id = str(random.randint(1, 20))  # Ensure we select a valid product ID
    predefined_product = predefined_products.get(product_id)

    if predefined_product:
        # Simulate a price update and availability change
        updated_availability = random.choice(availability_options)
        updated_price = round(random.uniform(1.99, 19.99), 2)

        # Update product data with new availability and price
        product = {
            "id": product_id,
            "product": predefined_product["product"],
            "current_availability": updated_availability,
            "price": updated_price,
            "delivery_time": random.choice(delivery_time_options)
        }
        return product
    else:
        # If the product is not predefined, return a mock product to simulate error handling
        return {
            "id": str(random.randint(21, 50)),  # ID outside the predefined range
            "product": "Unknown Product",
            "current_availability": "Out of Stock",
            "price": 0.0,
            "delivery_time": "N/A"
        }

def update_product_data(
    key_value_store: GridGainStore, 
    vector_store: GridGainVectorStore, 
    embeddings: OpenAIEmbeddings
):
    """
    Emit or update predefined product data in GridGain cache and vector store
    """
    try:
        # Generate a new product
        product = generate_product_data()
        
        # Check if the product ID is in the predefined list
        if product['id'] in predefined_products:
            combined_texts = []
            metadatas = []
            # Update predefined product's availability and price
            predefined_product = predefined_products[product['id']]
            predefined_product['current_availability'] = product['current_availability']
            predefined_product['price'] = product['price']

            # Update in key-value store
            key_value_store.mset([(product['id'], json.dumps(predefined_product))])
            logger.info(f"Updated product in key-value store: {product['id']}")
            
            product_content = f"Name: {product['product']}, Price: {product['price']}, Availability: {product['current_availability']}, Delivery Time: {product['delivery_time']}"
            
            metadata = {
                'id': product['id']
            }

            combined_texts.append(product_content)
            metadatas.append(metadata)
            
            added_titles = vector_store.add_texts(combined_texts, metadatas=metadatas)
            logger.info(f"Updated product in vector store: {added_titles}")
        
        return product
    except Exception as e:
        logger.error(f"Error updating product data: {e}")
        raise

def main(api_key: str, num_products: int = 5, interval: int = 60):
    """
    Main function to Update product data
    """
    client = None
    try:
        # Connect to GridGain
        client = connect_to_gridgain("localhost", 10800)
        
        # Initialize embeddings
        embeddings = initialize_embeddings_model(api_key)
        
        # Initialize stores
        key_value_store = initialize_key_value_store(client)
        vector_store = initialize_vector_store(client, embeddings)
        
        # Emit products
        logger.info(f"Starting to emit {num_products} products with {interval} second intervals")
        
        for _ in range(num_products):
            product = update_product_data(key_value_store, vector_store, embeddings)
            logger.info(f"Updated Product: {json.dumps(product, indent=2)}")
            time.sleep(interval)
        
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Product Data Emitter")
    parser.add_argument("--use_api_key", help="The API key to be used")
    parser.add_argument("--load_data", default="false", help="Load data or use pre-loaded data")
    parser.add_argument("--json_file", default="data\products.json", help="Path to the JSON file with initial products")
    parser.add_argument("--num_products", type=int, default=20, help="Number of products to emit")
    parser.add_argument("--interval", type=int, default=5, help="Interval between product emissions in seconds")

    args = parser.parse_args()

    api_key = args.use_api_key or input("\nPlease provide your OpenAI API key: ")
    load_data = str2bool(args.load_data)
    
    if load_data:
        logger.info("load_data is true, will be loading the data in the databases")
        data_loader(api_key, args.json_file)
        time.sleep(args.interval)
    
    main(api_key, args.num_products, args.interval)