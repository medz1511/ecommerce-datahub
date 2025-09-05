# src/dags/common/enrich.py
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

def enrich_data(date):
    """
    Enrichit les données nettoyées - adaptée à votre structure
    """
    try:
        # Chemins avec vérification d'existence
        clients_path = f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv"
        products_path = f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv"
        orders_path = f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv"
        
        print(f"Recherche des fichiers:")
        print(f"  Clients: {clients_path} - {'EXISTE' if os.path.exists(clients_path) else 'MANQUANT'}")
        print(f"  Produits: {products_path} - {'EXISTE' if os.path.exists(products_path) else 'MANQUANT'}")
        print(f"  Commandes: {orders_path} - {'EXISTE' if os.path.exists(orders_path) else 'MANQUANT'}")
        
        # Vérification que les fichiers existent
        if not all(os.path.exists(path) for path in [clients_path, products_path, orders_path]):
            missing_files = [path for path in [clients_path, products_path, orders_path] if not os.path.exists(path)]
            print(f"Fichiers manquants pour l'enrichissement: {missing_files}")
            return {}
        
        df_clients = pd.read_csv(clients_path)
        df_products = pd.read_csv(products_path)
        df_orders = pd.read_csv(orders_path)
        
        print(f"\nStructure des données:")
        print(f"Clients - Colonnes: {df_clients.columns.tolist()}")
        print(f"Produits - Colonnes: {df_products.columns.tolist()}")
        print(f"Commandes - Colonnes: {df_orders.columns.tolist()}")
        
        # ENRICHISSEMENT CLIENTS 
        # (pas de registration_date dans vos données, donc on skip)
        print("⏭️ Pas d'enrichissement clients (colonne registration_date manquante)")
            
        # ENRICHISSEMENT PRODUITS 
        if not df_products.empty:
            if 'stock' in df_products.columns:  # ← Votre colonne s'appelle 'stock'
                # Calcul de la valeur du stock
                df_products['stock_value'] = df_products['stock']  # ← À adapter si vous avez un prix
                df_products['stock_status'] = np.where(
                    df_products['stock'] == 0, 'out_of_stock',
                    np.where(df_products['stock'] < 10, 'low_stock', 'in_stock')
                )
                print("✓ Enrichissement produits terminé")
            else:
                print("⏭️ Colonne stock manquante pour produits")
        else:
            print("⏭️ DataFrame produits vide")
        
        # ENRICHISSEMENT COMMANDES
        df_orders_enriched = df_orders.copy()
        
        # Fusion avec clients si les colonnes existent
        if not df_orders.empty and not df_clients.empty:
            if 'customer_id' in df_orders.columns and 'customer_id' in df_clients.columns:
                # Ajouter les informations clients aux commandes
                client_cols = ['customer_id', 'firstname', 'lastname', 'email']
                client_cols = [col for col in client_cols if col in df_clients.columns]
                
                df_orders_enriched = pd.merge(
                    df_orders, df_clients[client_cols],
                    on='customer_id', how='left'
                )
                print("✓ Fusion commandes-clients terminée")
            else:
                print("⏭️ Colonne customer_id manquante pour la fusion clients")
        
        # Calcul du montant total pour les commandes
        if 'quantity' in df_orders_enriched.columns and 'price' in df_orders_enriched.columns:
            df_orders_enriched['total_amount'] = (
                df_orders_enriched['quantity'] * df_orders_enriched['price']
            )
            print("✓ Calcul du montant total terminé")
        else:
            missing_cols = []
            if 'quantity' not in df_orders_enriched.columns:
                missing_cols.append('quantity')
            if 'price' not in df_orders_enriched.columns:
                missing_cols.append('price')
            print(f"⏭️ Colonnes manquantes pour calcul montant: {missing_cols}")
        
        # Sauvegarde avec création automatique des dossiers
        enriched_dir = ensure_directory_exists(
            f"data/enriched_data/{date.year}/{date.month}/"
        )
        
        df_clients.to_csv(f"{enriched_dir}clients_{date.day}.csv", index=False)
        df_products.to_csv(f"{enriched_dir}products_{date.day}.csv", index=False)
        df_orders_enriched.to_csv(f"{enriched_dir}orders_{date.day}.csv", index=False)
        
        print(f"\n✓ Données enrichies sauvegardées dans: {enriched_dir}")
        
        # Aperçu des données enrichies
        print(f"\nAperçu des commandes enrichies:")
        print(df_orders_enriched[['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'total_amount']].head(2))
        
        return {
            'clients': df_clients,
            'products': df_products,
            'orders': df_orders_enriched
        }
        
    except Exception as e:
        print(f"❌ Erreur lors de l'enrichissement: {e}")
        import traceback
        traceback.print_exc()
        return {}