# debug_structure.py
from datetime import datetime
import pandas as pd
import os  # Ajout de l'import manquant

def debug_data_structure():
    date_test = datetime(2024, 5, 15)
    
    print("=== DEBUG - STRUCTURE DES DONNÉES ===")
    
    # Vérifier les fichiers raw
    print("\n1. FICHIERS RAW:")
    paths = [
        f"data/raw_data/clients/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/raw_data/products/{date_test.year}/{date_test.month}/{date_test.day}.csv", 
        f"data/raw_data/orders/{date_test.year}/{date_test.month}/{date_test.day}.csv"
    ]
    
    for path in paths:
        exists = "EXISTE" if os.path.exists(path) else "MANQUANT"
        print(f"  {path}: {exists}")
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                print(f"    Colonnes: {df.columns.tolist()}")
                print(f"    Shape: {df.shape}")
                print(f"    Preview:")
                print(df.head(2))
                print("    " + "-" * 40)
            except Exception as e:
                print(f"    Erreur lecture: {e}")
    
    # Vérifier les fichiers clean
    print("\n2. FICHIERS CLEAN:")
    clean_paths = [
        f"data/clean_data/clients/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/clean_data/products/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/clean_data/orders/{date_test.year}/{date_test.month}/{date_test.day}.csv"
    ]
    
    for path in clean_paths:
        exists = "EXISTE" if os.path.exists(path) else "MANQUANT"
        print(f"  {path}: {exists}")
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                print(f"    Colonnes: {df.columns.tolist()}")
                print(f"    Shape: {df.shape}")
                print(f"    Preview:")
                print(df.head(2))
                print("    " + "-" * 40)
            except Exception as e:
                print(f"    Erreur lecture: {e}")

if __name__ == "__main__":
    debug_data_structure()