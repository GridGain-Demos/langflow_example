import argparse
import logging
import os
import json

from pygridgain import Client
from langchain_gridgain.storage import GridGainStore
from langchain_gridgain.vectorstores import GridGainVectorStore
from langchain_openai import OpenAIEmbeddings


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_embeddings_model(api_key: str) -> OpenAIEmbeddings:
    """
    Initialize OpenAI embeddings model
    """
    try:
        os.environ["OPENAI_API_KEY"] = api_key
        embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small"
        )
        return embeddings
    except Exception as e:
        logger.error(f"Failed to initialize embeddings: {e}")
        raise

def connect_to_gridgain(host: str, port: int) -> Client:
    """
    Connect to GridGain
    """
    try:
        client = Client()
        client.connect(host, port)
        logger.info("Connected to GridGain successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to GridGain: {e}")
        raise

def initialize_key_value_store(client: Client) -> GridGainStore:
    """
    Initialize GridGain Key-Value Store for products
    """
    try:
        key_value_store = GridGainStore(
            cache_name="vector_cache",
            client=client
        )
        logger.info("GridGainStore for products initialized successfully.")
        return key_value_store
    except Exception as e:
        logger.error(f"Failed to initialize GridGainStore: {e}")
        raise

def initialize_vector_store(client: Client, embeddings: OpenAIEmbeddings) -> GridGainVectorStore:
    """
    Initialize GridGain Vector Store
    """
    try:
        vector_store = GridGainVectorStore(
            cache_name="vector_cache",
            embedding=embeddings,
            client=client
        )
        logger.info("GridGainVectorStore initialized successfully.")
        return vector_store
    except Exception as e:
        logger.error(f"Failed to initialize GridGainVectorStore: {e}")
        raise

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
