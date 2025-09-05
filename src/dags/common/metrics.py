# src/dags/common/metrics.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sqlite3

def calculate_daily_metrics(date):
    """
    Calcule les métriques quotidiennes demandées
    - Stock par magasin/site
    - Nombre de clients par magasin/site
    """
    try:
        # Charger les données enrichies du jour
        enriched_path = f"data/enriched_data/{date.year}/{date.month}/"
        clients_path = f"{enriched_path}clients_{date.day}.csv"
        products_path = f"{enriched_path}products_{date.day}.csv"
        orders_path = f"{enriched_path}orders_{date.day}.csv"
        
        if not all(os.path.exists(p) for p in [clients_path, products_path, orders_path]):
            print(f"Données manquantes pour le {date}")
            return {}
        
        df_clients = pd.read_csv(clients_path)
        df_products = pd.read_csv(products_path)
        df_orders = pd.read_csv(orders_path)
        
        # 1. STOCK DISPONIBLE par magasin
        # (Supposons que 'store_id' existe dans les produits)
        stock_metrics = {}
        if 'store_id' in df_products.columns:
            stock_by_store = df_products.groupby('store_id')['stock'].sum().reset_index()
            for _, row in stock_by_store.iterrows():
                stock_metrics[f"stock_store_{row['store_id']}"] = row['stock']
        else:
            # Si pas de magasins, stock global
            total_stock = df_products['stock'].sum()
            stock_metrics['stock_global'] = total_stock
        
        # 2. NOMBRE DE CLIENTS par magasin
        client_metrics = {}
        if 'store_id' in df_clients.columns:
            clients_by_store = df_clients.groupby('store_id')['customer_id'].nunique().reset_index()
            for _, row in clients_by_store.iterrows():
                client_metrics[f"clients_store_{row['store_id']}"] = row['customer_id']
        else:
            # Clients globaux
            total_clients = df_clients['customer_id'].nunique()
            client_metrics['clients_global'] = total_clients
        
        # 3. CHIFFRE D'AFFAIRES du jour (pour aggregation mensuelle)
        daily_revenue = 0
        if 'total_amount' in df_orders.columns:
            daily_revenue = df_orders['total_amount'].sum()
        
        # Sauvegarder les métriques quotidiennes
        daily_metrics = {
            'date': date.strftime('%Y-%m-%d'),
            **stock_metrics,
            **client_metrics,
            'daily_revenue': daily_revenue
        }
        
        # Sauvegarde en CSV
        metrics_dir = ensure_directory_exists(f"data/metrics/daily/{date.year}/{date.month}/")
        metrics_df = pd.DataFrame([daily_metrics])
        metrics_df.to_csv(f"{metrics_dir}{date.day}.csv", index=False)
        
        print(f"Métriques quotidiennes calculées pour {date}")
        return daily_metrics
        
    except Exception as e:
        print(f"Erreur calcul métriques quotidiennes: {e}")
        return {}

def calculate_monthly_revenue(month_year):
    """
    Calcule le chiffre d'affaires mensuel
    Format month_year: '2024-05'
    """
    try:
        year, month = month_year.split('-')
        month_start = datetime(int(year), int(month), 1)
        
        # Charger toutes les métriques quotidiennes du mois
        daily_metrics_dir = f"data/metrics/daily/{year}/{month}/"
        daily_files = []
        
        if os.path.exists(daily_metrics_dir):
            daily_files = [f for f in os.listdir(daily_metrics_dir) if f.endswith('.csv')]
        
        monthly_revenue = 0
        daily_data = []
        
        for file in daily_files:
            day = file.replace('.csv', '')
            file_path = f"{daily_metrics_dir}{file}"
            df_day = pd.read_csv(file_path)
            
            if 'daily_revenue' in df_day.columns:
                monthly_revenue += df_day['daily_revenue'].sum()
                daily_data.append(df_day.iloc[0].to_dict())
        
        # Agréger les données par magasin si disponible
        monthly_metrics = {'month': month_year, 'total_revenue': monthly_revenue}
        
        # Ajouter les agrégations par magasin
        if daily_data:
            df_month = pd.DataFrame(daily_data)
            store_columns = [col for col in df_month.columns if col.startswith('stock_store_')]
            
            for col in store_columns:
                store_id = col.replace('stock_store_', '')
                monthly_metrics[f'revenue_store_{store_id}'] = df_month[col].mean() if col in df_month.columns else 0
        
        # Sauvegarder les métriques mensuelles
        metrics_dir = ensure_directory_exists(f"data/metrics/monthly/{year}/")
        metrics_df = pd.DataFrame([monthly_metrics])
        metrics_file = f"{metrics_dir}{month_year}.csv"
        
        # Append ou create new
        if os.path.exists(metrics_file):
            existing_df = pd.read_csv(metrics_file)
            metrics_df = pd.concat([existing_df, metrics_df]).drop_duplicates(subset=['month'])
        
        metrics_df.to_csv(metrics_file, index=False)
        
        print(f"Chiffre d'affaires mensuel {month_year}: {monthly_revenue:.2f}€")
        return monthly_metrics
        
    except Exception as e:
        print(f"Erreur calcul CA mensuel: {e}")
        return {}

def ensure_directory_exists(file_path):
    """Crée automatiquement le dossier s'il n'existe pas"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return file_path

def generate_daily_report(date):
    """
    Génère un rapport quotidien complet
    """
    metrics = calculate_daily_metrics(date)
    
    if metrics:
        print(f"\n📊 RAPPORT QUOTIDIEN - {date}")
        print("=" * 40)
        
        # Stock
        stock_keys = [k for k in metrics.keys() if k.startswith('stock_')]
        for key in stock_keys:
            print(f"📦 {key}: {metrics[key]} unités")
        
        # Clients
        client_keys = [k for k in metrics.keys() if k.startswith('clients_')]
        for key in client_keys:
            print(f"👥 {key}: {metrics[key]} clients")
        
        # CA quotidien
        print(f"💰 Chiffre d'affaires journalier: {metrics.get('daily_revenue', 0):.2f}€")
        
        return metrics
    return {}

def generate_monthly_report(month_year):
    """
    Génère un rapport mensuel complet
    """
    metrics = calculate_monthly_revenue(month_year)
    
    if metrics:
        print(f"\n📈 RAPPORT MENSUEL - {month_year}")
        print("=" * 40)
        print(f"💰 Chiffre d'affaires total: {metrics.get('total_revenue', 0):.2f}€")
        
        # CA par magasin
        revenue_keys = [k for k in metrics.keys() if k.startswith('revenue_store_')]
        for key in revenue_keys:
            store_id = key.replace('revenue_store_', '')
            print(f"🏪 Magasin {store_id}: {metrics[key]:.2f}€")
        
        return metrics
    return {}