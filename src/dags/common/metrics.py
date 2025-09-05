import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sqlite3
import shutil

def ensure_directory_exists(file_path):
    """Crée automatiquement le dossier s'il n'existe pas"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return file_path

def fix_directory_names():
    """
    Corrige les noms de dossiers avec zéros en trop
    """
    # Corriger metrics/daily/2024/05/ -> 2024/5/
    old_daily_path = "data/metrics/daily/2024/05/"
    new_daily_path = "data/metrics/daily/2024/5/"
    
    if os.path.exists(old_daily_path) and not os.path.exists(new_daily_path):
        os.makedirs(os.path.dirname(new_daily_path), exist_ok=True)
        shutil.move(old_daily_path, new_daily_path)
        print(f"✅ Dossier daily renommé: {old_daily_path} -> {new_daily_path}")
    
    # Corriger metrics/monthly/2024/05/ -> 2024/5/
    old_monthly_path = "data/metrics/monthly/2024/05/"
    new_monthly_path = "data/metrics/monthly/2024/5/"
    
    if os.path.exists(old_monthly_path) and not os.path.exists(new_monthly_path):
        os.makedirs(os.path.dirname(new_monthly_path), exist_ok=True)
        shutil.move(old_monthly_path, new_monthly_path)
        print(f"✅ Dossier monthly renommé: {old_monthly_path} -> {new_monthly_path}")
    
    return True

def calculate_daily_metrics(date):
    """
    Calcule les métriques quotidiennes demandées
    - Stock par magasin/site
    - Nombre de clients par magasin/site
    """
    try:
        # Utiliser le mois sans zéro (5 au lieu de 05)
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
        
        # 1. STOCK DISPONIBLE (global car pas de store_id dans vos données)
        stock_metrics = {}
        total_stock = df_products['stock'].sum() if 'stock' in df_products.columns else 0
        stock_metrics['stock_global'] = total_stock
        
        # 2. NOMBRE DE CLIENTS (global car pas de store_id)
        client_metrics = {}
        total_clients = df_clients['customer_id'].nunique() if 'customer_id' in df_clients.columns else 0
        client_metrics['clients_global'] = total_clients
        
        # 3. CHIFFRE D'AFFAIRES du jour
        daily_revenue = 0
        if 'total_amount' in df_orders.columns:
            daily_revenue = df_orders['total_amount'].sum()
        elif 'price' in df_orders.columns and 'quantity' in df_orders.columns:
            daily_revenue = (df_orders['price'] * df_orders['quantity']).sum()
        
        # Sauvegarder les métriques quotidiennes
        daily_metrics = {
            'date': date.strftime('%Y-%m-%d'),
            **stock_metrics,
            **client_metrics,
            'daily_revenue': daily_revenue
        }
        
        # Utiliser le mois sans zéro
        metrics_dir = ensure_directory_exists(f"data/metrics/daily/{date.year}/{date.month}/")
        metrics_df = pd.DataFrame([daily_metrics])
        metrics_df.to_csv(f"{metrics_dir}{date.day}.csv", index=False)
        
        print(f"✅ Métriques quotidiennes calculées pour {date}")
        return daily_metrics
        
    except Exception as e:
        print(f"❌ Erreur calcul métriques quotidiennes: {e}")
        return {}

def calculate_monthly_revenue(month_year):
    """
    Calcule le chiffre d'affaires mensuel - Version corrigée
    """
    try:
        year, month_str = month_year.split('-')
        month = str(int(month_str))  # Convertir "05" en "5"
        
        # Essayer avec et sans zéro pour trouver le bon dossier
        possible_paths = [
            f"data/metrics/daily/{year}/{month}/",           # Sans zéro (5)
            f"data/metrics/daily/{year}/{month_str}/",       # Avec zéro (05)
            f"data/metrics/daily/{year}/{month.zfill(2)}/",  # Avec zéro forcé
        ]
        
        daily_metrics_dir = None
        for path in possible_paths:
            if os.path.exists(path):
                daily_metrics_dir = path
                break
        
        if not daily_metrics_dir:
            print(f"❌ Dossier des métriques quotidiennes introuvable")
            print(f"   Chemins testés: {possible_paths}")
            return {'month': month_year, 'total_revenue': 0}
        
        daily_files = [f for f in os.listdir(daily_metrics_dir) if f.endswith('.csv')]
        
        if not daily_files:
            print(f"❌ Aucun fichier de métriques quotidiennes pour {month_year}")
            return {'month': month_year, 'total_revenue': 0}
        
        monthly_revenue = 0
        daily_data = []
        
        print(f"📊 Calcul du CA mensuel pour {month_year}")
        print(f"📁 Dossier utilisé: {daily_metrics_dir}")
        print(f"📁 Fichiers trouvés: {len(daily_files)}")
        
        for file in daily_files:
            try:
                file_path = os.path.join(daily_metrics_dir, file)
                df_day = pd.read_csv(file_path)
                
                if not df_day.empty and 'daily_revenue' in df_day.columns:
                    daily_revenue = float(df_day['daily_revenue'].iloc[0])
                    monthly_revenue += daily_revenue
                    
                    daily_info = {
                        'date': df_day['date'].iloc[0] if 'date' in df_day.columns else file.replace('.csv', ''),
                        'daily_revenue': daily_revenue
                    }
                    daily_data.append(daily_info)
                    
                    print(f"   ➕ {file}: {daily_revenue:.2f}€")
                    
            except Exception as e:
                print(f"   ⚠️  Erreur avec {file}: {e}")
                continue
        
        if monthly_revenue == 0:
            print(f"⚠️  Aucun chiffre d'affaires trouvé pour {month_year}")
            return {'month': month_year, 'total_revenue': 0}
        
        print(f"   ✅ CA MENSUEL TOTAL: {monthly_revenue:.2f}€")
        print(f"   📅 Jours avec données: {len(daily_data)}")
        
        # Créer le résultat
        monthly_metrics = {
            'month': month_year, 
            'total_revenue': monthly_revenue,
            'days_count': len(daily_data),
            'avg_daily_revenue': monthly_revenue / len(daily_data) if daily_data else 0
        }
        
        # Sauvegarder les métriques mensuelles
        metrics_dir = ensure_directory_exists(f"data/metrics/monthly/{year}/")
        metrics_file = f"{metrics_dir}{month_year}.csv"
        
        metrics_df = pd.DataFrame([monthly_metrics])
        metrics_df.to_csv(metrics_file, index=False)
        
        print(f"💾 Fichier sauvegardé: {metrics_file}")
        
        return monthly_metrics
        
    except Exception as e:
        print(f"❌ Erreur calcul CA mensuel: {e}")
        import traceback
        traceback.print_exc()
        return {'month': month_year, 'total_revenue': 0}

def generate_daily_report(date):
    """
    Génère un rapport quotidien complet
    """
    # Corriger les dossiers d'abord
    fix_directory_names()
    
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
    # Corriger les dossiers d'abord
    fix_directory_names()
    
    metrics = calculate_monthly_revenue(month_year)
    
    if metrics and metrics.get('total_revenue', 0) > 0:
        print(f"\n📈 RAPPORT MENSUEL - {month_year}")
        print("=" * 40)
        print(f"💰 Chiffre d'affaires total: {metrics.get('total_revenue', 0):.2f}€")
        print(f"📅 Nombre de jours avec données: {metrics.get('days_count', 0)}")
        print(f"📈 Moyenne quotidienne: {metrics.get('avg_daily_revenue', 0):.2f}€")
        
        return metrics
    else:
        print(f"\n⚠️  Aucune donnée de chiffre d'affaires pour {month_year}")
        return {}

# Fonction utilitaire pour tester
def test_metrics():
    """Test des métriques"""
    from datetime import datetime
    
    # Corriger les dossiers d'abord
    fix_directory_names()
    
    # Test quotidien
    date_test = datetime(2024, 5, 15)
    print("Testing daily metrics...")
    daily_metrics = calculate_daily_metrics(date_test)
    print("Daily metrics:", daily_metrics)
    
    # Test mensuel
    print("\nTesting monthly revenue...")
    monthly_metrics = calculate_monthly_revenue('2024-05')
    print("Monthly metrics:", monthly_metrics)

if __name__ == "__main__":
    test_metrics()