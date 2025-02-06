import json
import time
import logging
from langchain.schema import Document

from langchain_gridgain.storage import GridGainStore
from langchain_gridgain.vectorstores import GridGainVectorStore
from langchain_openai import OpenAIEmbeddings

from utils import connect_to_gridgain, initialize_embeddings_model, initialize_key_value_store, initialize_vector_store

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = connect_to_gridgain("localhost", 10800)
cache_name ='vector_cache'

def load_json_data(file_path: str) -> list:
    """
    Loading initial products from JSON file
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data.get('products', [])
    except Exception as e:
        logger.error(f"Failed to load products: {e}")
        raise

def load_data_to_vector_store(
    products: list,
    vector_store: GridGainVectorStore, 
    embeddings: OpenAIEmbeddings
):
    """
    Emit initial product data to GridGain cache and vector store
    """
    try:
        combined_texts = []
        metadatas = []

        for product in products:
            product_content = f"Name: {product['product']}, Price: {product['price']}, Availability: {product['current_availability']}, Delivery Time: {product['delivery_time']}"
            
            metadata = {
                'id': product['id']
            }
            print(f"Product content: {product_content}")
            print(f"Metadata: {metadata}")
            
            combined_texts.append(product_content)
            metadatas.append(metadata)

        added_titles = vector_store.add_texts(combined_texts, metadatas=metadatas)
        logger.info(f"GridGain vector store populated with combined review and spec data for {added_titles}")
        
    except Exception as e:
        logger.error(f"Error emitting product data: {e}")
        raise


def data_loader(api_key: str, json_file_path: str):
    """
    Main function to load and emit initial product data
    """
    client = None
    try:
        # Connect to GridGain
        client = connect_to_gridgain("localhost", 10800)
        
        # Initialize embeddings
        embeddings = initialize_embeddings_model(api_key)
        
        # Initialize stores
        vector_store = initialize_vector_store(client, embeddings)
        
        # Load initial products
        products = load_json_data(json_file_path)
        
        # Emit products
        logger.info(f"Starting to load initial products from {json_file_path}")
        load_data_to_vector_store(products, vector_store, embeddings)
        
        logger.info("Initial product data loading complete.")

        
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Initial Product Data Loader")
    parser.add_argument("--use_api_key", help="The OpenAI API key to be used")
    parser.add_argument("--json_file", default="data\products.json", help="Path to the JSON file with initial products")
    args = parser.parse_args()

    api_key = args.use_api_key or input("\nPlease provide your OpenAI API key: ")
    
    data_loader(api_key, args.json_file)