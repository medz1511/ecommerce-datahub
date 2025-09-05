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

def clean_clients_data(date):
    """
    Nettoie les données clients - creation automatique des dossiers
    """
    try:
        # Lecture
        raw_path = f"data/raw_data/clients/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnee client a nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        if 'email' in df.columns:
            df['email'] = df['email'].str.lower().str.strip()
        if 'firstname' in df.columns:
            df['firstname'] = df['firstname'].str.title().str.strip()
        if 'lastname' in df.columns:
            df['lastname'] = df['lastname'].str.upper().str.strip()
        
        # Validation emails si la colonne existe
        if 'email' in df.columns:
            df = df[df['email'].str.contains('@', na=False)]
        
        # Sauvegarde avec creation automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/clients/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Clients nettoyes : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage clients: {e}")
        return pd.DataFrame()

def clean_products_data(date):
    """
    Nettoie les données produits - creation automatique des dossiers
    """
    try:
        raw_path = f"data/raw_data/products/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnee produit a nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        
        # Supprimer valeurs aberrantes
        df = df.dropna()
        if 'price' in df.columns and 'quantity' in df.columns:
            df = df[(df['price'] > 0) & (df['quantity'] >= 0)]
        
        # Sauvegarde avec creation automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/products/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Produits nettoyes : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage produits: {e}")
        return pd.DataFrame()

def clean_orders_data(date):
    """
    Nettoie les données commandes - creation automatique des dossiers
    """
    try:
        raw_path = f"data/raw_data/orders/{date.year}/{date.month}/{date.day}.csv"
        if not os.path.exists(raw_path):
            print(f"Aucune donnee commande a nettoyer pour {date}")
            return pd.DataFrame()
        
        df = pd.read_csv(raw_path)
        
        # Nettoyage
        df = df.drop_duplicates()
        if 'order_date' in df.columns:
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Filtrer dates et montants valides
        df = df.dropna()
        if 'amount' in df.columns:
            df = df[df['amount'] > 0]
        
        # Sauvegarde avec creation automatique du dossier
        clean_path = ensure_directory_exists(
            f"data/clean_data/orders/{date.year}/{date.month}/{date.day}.csv"
        )
        df.to_csv(clean_path, index=False)
        
        print(f"Commandes nettoyees : {clean_path}")
        return df
        
    except Exception as e:
        print(f"Erreur nettoyage commandes: {e}")
        return pd.DataFrame()