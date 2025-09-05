import pandas as pd
import numpy as np
from datetime import datetime
import os

def ensure_directory_exists(file_path):
    """Crée automatiquement le dossier s'il n'existe pas"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Dossier cree: {directory}")
    return file_path

def enrich_data(date):
    """
    Enrichit les données nettoyées - creation automatique des dossiers
    """
    try:
        # Chemins avec verification d'existence
        clients_path = f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv"
        products_path = f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv"
        orders_path = f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv"
        
        # Verification que les fichiers existent
        if not all(os.path.exists(path) for path in [clients_path, products_path, orders_path]):
            print(f"Donnees manquantes pour l'enrichissement du {date}")
            return {}
        
        df_clients = pd.read_csv(clients_path)
        df_products = pd.read_csv(products_path)
        df_orders = pd.read_csv(orders_path)
        
        # ENRICHISSEMENT CLIENTS
        if not df_clients.empty and 'registration_date' in df_clients.columns:
            df_clients['customer_since_days'] = (
                datetime.now() - pd.to_datetime(df_clients['registration_date'])
            ).dt.days
            
        # ENRICHISSEMENT PRODUITS
        if not df_products.empty and 'quantity' in df_products.columns and 'price' in df_products.columns:
            df_products['stock_value'] = df_products['quantity'] * df_products['price']
            df_products['stock_status'] = np.where(
                df_products['quantity'] == 0, 'out_of_stock',
                np.where(df_products['quantity'] < 10, 'low_stock', 'in_stock')
            )
        
        # ENRICHISSEMENT COMMANDES
        df_orders_enriched = df_orders.copy()
        if not df_orders.empty and not df_clients.empty:
            if 'customer_id' in df_orders.columns and 'customer_id' in df_clients.columns:
                df_orders_enriched = pd.merge(
                    df_orders, df_clients[['customer_id', 'firstname', 'lastname', 'email']],
                    on='customer_id', how='left'
                )
            
        if not df_products.empty and 'product_id' in df_orders_enriched.columns:
            df_orders_enriched = pd.merge(
                df_orders_enriched, df_products[['product_id', 'product_name', 'price']],
                on='product_id', how='left'
            )
            if 'quantity' in df_orders_enriched.columns and 'price' in df_orders_enriched.columns:
                df_orders_enriched['total_amount'] = (
                    df_orders_enriched['quantity'] * df_orders_enriched['price']
                )
        
        # Sauvegarde avec creation automatique des dossiers
        enriched_dir = ensure_directory_exists(
            f"data/enriched_data/{date.year}/{date.month}/"
        )
        
        df_clients.to_csv(f"{enriched_dir}clients_{date.day}.csv", index=False)
        df_products.to_csv(f"{enriched_dir}products_{date.day}.csv", index=False)
        df_orders_enriched.to_csv(f"{enriched_dir}orders_{date.day}.csv", index=False)
        
        print(f"Donnees enrichies sauvegardees dans: {enriched_dir}")
        
        return {
            'clients': df_clients,
            'products': df_products,
            'orders': df_orders_enriched
        }
        
    except Exception as e:
        print(f"Erreur lors de l'enrichissement: {e}")
        return {}