# bls_data/data_extraction/api_key.py
"""
This module provides a function to retrieve a random BLS API key from environment variables.
It loads the environment variables from a .env file and selects one of the keys that start with 'BLS_API_KEY_'.
"""
import os
import random
from dotenv import load_dotenv

def get_random_bls_key():
    """
    Loads environment variables, finds all variables prefixed with 'BLS_API_KEY_',
    and returns one of them at random.

    Returns:
        str: A randomly selected BLS API key.
        
    Raises:
        ValueError: If no environment variables with the specified prefix are found.
    """
    # Load variables from .env file into the environment
    load_dotenv()
    
    # Define the prefix for our BLS keys
    key_prefix = 'BLS_API_KEY_'
    
    # Use a list comprehension to find all matching keys in the environment
    bls_keys = [
        value for key, value in os.environ.items() 
        if key.startswith(key_prefix) and value
    ]
    
    if not bls_keys:
        raise ValueError(f"No BLS API keys found in environment with prefix '{key_prefix}'")
        
    # Return a randomly chosen key from the list
    return random.choice(bls_keys)

# --- Main Execution Example ---
if __name__ == "__main__":
    try:
        # Create a dummy .env file for demonstration if it doesn't exist
        if not os.path.exists('.env'):
            print("Creating a dummy .env file for testing...")
            with open('.env', 'w') as f:
                f.write('BLS_API_KEY_0="dummy_key_alpha"\n')
                f.write('BLS_API_KEY_1="dummy_key_beta"\n')
                f.write('BLS_API_KEY_2="dummy_key_gamma"\n')
        
        print("Attempting to get a random BLS key...")
        # Run the function multiple times to see the random selection
        for i in range(5):
            api_key = get_random_bls_key()
            print(f"Run {i+1}: Selected API Key = {api_key}")

    except ValueError as e:
        print(f"Error: {e}")
