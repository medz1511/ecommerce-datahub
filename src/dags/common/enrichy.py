import pandas as pd
from datetime import datetime
import os

def enrich_data(date):
    """
    Enrichit les données nettoyées
    """
    try:
        # Lecture des données nettoyées
        clients_path = f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv"
        products_path = f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv"
        orders_path = f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv"
        
        df_clients = pd.read_csv(clients_path)
        df_products = pd.read_csv(products_path)
        df_orders = pd.read_csv(orders_path)
        
        # ENRICHISSEMENT CLIENTS
        if not df_clients.empty:
            df_clients['customer_since_days'] = (
                datetime.now() - pd.to_datetime(df_clients['registration_date'])
            ).dt.days
            
        # ENRICHISSEMENT PRODUITS
        if not df_products.empty:
            df_products['stock_value'] = df_products['quantity'] * df_products['price']
            df_products['stock_status'] = np.where(
                df_products['quantity'] == 0, 'out_of_stock',
                np.where(df_products['quantity'] < 10, 'low_stock', 'in_stock')
            )
        
        # ENRICHISSEMENT COMMANDES
        df_orders_enriched = df_orders.copy()
        if not df_orders.empty and not df_clients.empty:
            df_orders_enriched = pd.merge(
                df_orders, df_clients[['customer_id', 'firstname', 'lastname', 'email']],
                on='customer_id', how='left'
            )
            
        if not df_products.empty:
            df_orders_enriched = pd.merge(
                df_orders_enriched, df_products[['product_id', 'product_name', 'price']],
                on='product_id', how='left'
            )
            df_orders_enriched['total_amount'] = (
                df_orders_enriched['quantity'] * df_orders_enriched['price']
            )
        
        # Sauvegarde
        enriched_path = f"data/enriched_data/{date.year}/{date.month}/"
        os.makedirs(enriched_path, exist_ok=True)
        
        df_clients.to_csv(f"{enriched_path}clients_{date.day}.csv", index=False)
        df_products.to_csv(f"{enriched_path}products_{date.day}.csv", index=False)
        df_orders_enriched.to_csv(f"{enriched_path}orders_{date.day}.csv", index=False)
        
        print(f"Donnees enrichies sauvegardees dans: {enriched_path}")
        
        return {
            'clients': df_clients,
            'products': df_products,
            'orders': df_orders_enriched
        }
        
    except FileNotFoundError as e:
        print(f"Donnees manquantes pour l'enrichissement: {e}")
        return {}