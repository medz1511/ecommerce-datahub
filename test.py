from datetime import datetime
from src.dags.common.google_auth import test_connection
from src.dags.common.extract import extract_clients, extract_products, extract_orders

def test_complet():
    print("Test d'authentification Google Drive...")
    
    if not test_connection():
        return False
    
    try:
        print("Test extraction des donnees...")
        date_test = datetime(2024, 5, 10)
        
        # Test clients
        print("Test extraction clients...")
        result_clients = extract_clients(date_test)
        
        # Test produits
        print("Test extraction produits...")
        result_products = extract_products(date_test)
        
        # Test commandes
        print("Test extraction commandes...")
        result_orders = extract_orders(date_test)
        
        print("Tests completes avec succes")
        return True
            
    except Exception as e:
        print(f"Erreur lors des tests: {e}")
        return False

if __name__ == "__main__":
    success = test_complet()
    exit(0 if success else 1)