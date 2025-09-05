import pandas as pd
import numpy as np
from datetime import datetime
import os

def ensure_directory_exists(file_path):
    """Crée automatiquement le dossier s'il n'existe pas"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Dossier créé: {directory}")
    return file_path

def clean_clients_data(date):
    """
    Nettoie les données clients - création automatique des dossiers
    """
    try:
        # Lecture
        raw_path = f"data/raw_data/clients/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnée client à nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        
        # Nettoyage des colonnes textuelles
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower().str.strip()
        if 'firstname' in df.columns:
            df['firstname'] = df['firstname'].str.title().str.strip()
        if 'lastname' in df.columns:
            df['lastname'] = df['lastname'].str.upper().str.strip()
        
        # Validation emails si la colonne existe
        if 'email' in df.columns:
            df = df[df['email'].str.contains('@', na=False)]
        
        # Validation des IDs
        if 'customer_id' in df.columns:
            df = df[df['customer_id'].notna()]
            df['customer_id'] = df['customer_id'].astype(int)
        
        # Sauvegarde avec création automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Clients nettoyés : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage clients: {e}")
        return pd.DataFrame()

def clean_products_data(date):
    """
    Nettoie les données produits - création automatique des dossiers
    """
    try:
        raw_path = f"data/raw_data/products/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnée produit à nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        
        # Conversion des types numériques
        if 'product_id' in df.columns:
            df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
        if 'stock' in df.columns:  # Votre colonne s'appelle 'stock'
            df['stock'] = pd.to_numeric(df['stock'], errors='coerce')
        
        # Supprimer valeurs aberrantes et NaN
        df = df.dropna()
        
        # Validation des valeurs
        if 'stock' in df.columns:
            df = df[df['stock'] >= 0]  # Stock ne peut pas être négatif
        
        # Nettoyage des noms de produits
        if 'product_name' in df.columns:
            df['product_name'] = df['product_name'].str.strip()
        
        # Sauvegarde avec création automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Produits nettoyés : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage produits: {e}")
        return pd.DataFrame()

def clean_orders_data(date):
    """
    Nettoie les données commandes - création automatique des dossiers
    """
    try:
        raw_path = f"data/raw_data/orders/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnée commande à nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        
        # Conversion des types
        if 'order_id' in df.columns:
            df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce')
        if 'customer_id' in df.columns:
            df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')
        if 'product_id' in df.columns:
            df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Gestion des dates
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        # Supprimer les lignes avec des valeurs manquantes
        df = df.dropna()
        
        # Validation des valeurs
        if 'quantity' in df.columns:
            df = df[df['quantity'] > 0]  # Quantité doit être positive
        if 'price' in df.columns:
            df = df[df['price'] > 0]     # Prix doit être positif
        
        # Nettoyage des colonnes textuelles
        if 'customer_name' in df.columns:
            df['customer_name'] = df['customer_name'].str.strip()
        if 'product_name' in df.columns:
            df['product_name'] = df['product_name'].str.strip()
        
        # Sauvegarde avec création automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Commandes nettoyées : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage commandes: {e}")
        return pd.DataFrame()

def clean_all_data(date):
    """
    Nettoie toutes les données pour une date donnée
    et retourne un dictionnaire avec les DataFrames
    """
    print(f"Nettoyage des données pour la date: {date}")
    
    clients = clean_clients_data(date)
    products = clean_products_data(date)
    orders = clean_orders_data(date)
    
    # Vérification des résultats
    results = {}
    if not clients.empty:
        results['clients'] = clients
        print(f"Clients nettoyés: {clients.shape[0]} lignes")
    else:
        print("Aucune donnée client nettoyée")
    
    if not products.empty:
        results['products'] = products
        print(f"Produits nettoyés: {products.shape[0]} lignes")
    else:
        print("Aucune donnée produit nettoyée")
    
    if not orders.empty:
        results['orders'] = orders
        print(f"Commandes nettoyées: {orders.shape[0]} lignes")
    else:
        print("Aucune donnée commande nettoyée")
    
    return results

# Fonction utilitaire pour tester le nettoyage
def test_cleaning():
    """Test unitaire du nettoyage"""
    from datetime import datetime
    
    date_test = datetime(2024, 5, 15)
    print("Test du nettoyage de données...")
    
    result = clean_all_data(date_test)
    
    if result:
        print("✓ Nettoyage terminé avec succès")
        for key, df in result.items():
            if not df.empty:
                print(f"  {key}: {df.shape[0]} lignes, {df.shape[1]} colonnes")
    else:
        print("✗ Échec du nettoyage")

if __name__ == "__main__":
    test_cleaning()