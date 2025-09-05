import os.path
from datetime import datetime
import pandas as pd
import sqlite3
import io
from .google_auth import get_google_drive_service  # Import relatif

# Configuration
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")

def connect_to_drive():
    """Connexion a Google Drive avec PyDrive2"""
    return get_google_drive_service()

def extract_clients(date: datetime, service=None):
    """
    Extrait le fichier clients du jour depuis Google Drive
    """
    if service is None:
        service = connect_to_drive()
    
    FOLDER = "clients"
    filename = f"clients_{date.strftime('%Y-%m-%d')}.csv"
    
    # Recherche du dossier clients
    folder_query = f"title='{FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    folders = service.ListFile({'q': folder_query}).GetList()
    
    if not folders:
        raise FileNotFoundError(f"Dossier '{FOLDER}' non trouve")
    
    folder_id = folders[0]['id']
    
    # Recherche du fichier
    file_query = f"'{folder_id}' in parents and title='{filename}' and trashed=false"
    files = service.ListFile({'q': file_query}).GetList()
    
    if not files:
        print(f"Aucun fichier trouve avec le nom {filename}.")
        return
    
    # Telechargement
    file_obj = service.CreateFile({'id': files[0]['id']})
    local_path = os.path.join(f"{RAW_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    file_obj.GetContentFile(local_path)
    print(f"Fichier telecharge : {local_path}")
    return local_path



def extract_products(date: datetime, service=None):
    """
    Extrait le fichier products.csv et filtre pour la date specifique
    """
    if service is None:
        service = connect_to_drive()
    
    filename = "products.csv"
    
    # Recherche du fichier products.csv
    file_query = f"title='{filename}' and mimeType!='application/vnd.google-apps.folder' and trashed=false"
    files = service.ListFile({'q': file_query}).GetList()
    
    if not files:
        print(f"Aucun fichier trouve avec le nom {filename}.")
        return
    
    # Telechargement en memoire
    file_obj = service.CreateFile({'id': files[0]['id']})
    file_content = file_obj.GetContentString()
    
    # CORRECTION: Utiliser io.StringIO au lieu de pandas.compat.StringIO
    data = pd.read_csv(io.StringIO(file_content))  # ← Ligne corrigée
    final_data = data[data.date == date.strftime("%Y-%m-%d")]
    
    if final_data.shape[0] > 0:
        local_path = os.path.join(f"{RAW_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        final_data.to_csv(local_path, index=False)
        print(f"Produits filtres sauvegardes : {local_path}")


def extract_orders(date: datetime, db_path: str = "ecommerce_orders_may2024.db", table_name: str="ecommerce_orders"):
    """
    Extrait les commandes du jour depuis la base SQLite locale
    """
    conn = sqlite3.connect(db_path)
    try:
        date_str = date.strftime("%Y-%m-%d")
        df = pd.read_sql_query(f'SELECT * FROM {table_name} where order_date="{date_str}" ', conn)
    finally:
        conn.close()
    
    if df.shape[0] > 0:
        local_path = os.path.join(f"{RAW_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_csv(local_path, index=False)
        print(f"Commandes extraites : {local_path}")

if __name__ == "__main__":
    # Tests
    extract_products(datetime.strptime("2024-05-10", "%Y-%m-%d"))