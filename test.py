from datetime import datetime
from src.dags.common.google_auth import test_connection
from src.dags.common.extract import extract_clients, extract_products, extract_orders
from src.dags.common.clean import clean_all_data
from src.dags.common.enrich import enrich_data

def test_complet():
    print("Test complet du pipeline ETL...")
    
    if not test_connection():
        return False
    
    try:
        date_test = datetime(2024, 5, 15)
        
        print("1. Extraction...")
        extract_clients(date_test)
        extract_products(date_test)
        extract_orders(date_test)
        
        print("2. Nettoyage...")
        clean_all_data(date_test)
        
        print("3. Enrichissement...")
        enrich_data(date_test)
        
        print("Pipeline ETL execute avec succes!")
        return True
            
    except Exception as e:
        print(f"Erreur lors de l'execution: {e}")
        return False

if __name__ == "__main__":
    success = test_complet()
    exit(0 if success else 1)