from datetime import datetime
from src.dags.common.google_auth import test_connection
from src.dags.common.extract import extract_clients, extract_products, extract_orders
from src.dags.common.clean import clean_all_data
from src.dags.common.enrich import enrich_data
from src.dags.common.metrics import generate_daily_report, generate_monthly_report
import os

def test_authentication():
    """Teste l'authentification Google Drive"""
    print("🔐 Test d'authentification Google Drive...")
    return test_connection()

def test_extraction(date):
    """Teste l'extraction des données"""
    print("\n📥 Test d'extraction des données...")
    try:
        print("Extraction des clients...")
        extract_clients(date)
        
        print("Extraction des produits...")
        extract_products(date)
        
        print("Extraction des commandes...")
        extract_orders(date)
        
        print("✅ Extraction terminée avec succès")
        return True
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction: {e}")
        return False

def test_cleaning(date):
    """Teste le nettoyage des données"""
    print("\n🧹 Test de nettoyage des données...")
    try:
        result = clean_all_data(date)
        
        if result:
            print("✅ Nettoyage terminé avec succès")
            for key, df in result.items():
                if not df.empty:
                    print(f"  {key}: {df.shape[0]} lignes nettoyées")
            return True
        else:
            print("❌ Aucune donnée nettoyée")
            return False
    except Exception as e:
        print(f"❌ Erreur lors du nettoyage: {e}")
        return False

def test_enrichment(date):
    """Teste l'enrichissement des données"""
    print("\n🎯 Test d'enrichissement des données...")
    try:
        result = enrich_data(date)
        
        if result:
            print("✅ Enrichissement terminé avec succès")
            for key, df in result.items():
                if not df.empty:
                    print(f"  {key}: {df.shape[0]} lignes enrichies")
            return True
        else:
            print("❌ Aucune donnée enrichie")
            return False
    except Exception as e:
        print(f"❌ Erreur lors de l'enrichissement: {e}")
        return False

def test_metrics(date):
    """Teste le calcul des métriques métier"""
    print("\n📊 Test des métriques métier...")
    try:
        # Rapport quotidien
        print("Génération du rapport quotidien...")
        daily_metrics = generate_daily_report(date)
        
        if daily_metrics:
            print("✅ Rapport quotidien généré avec succès")
            
            # Rapport mensuel
            month_year = date.strftime('%Y-%m')
            print(f"\nGénération du rapport mensuel ({month_year})...")
            monthly_metrics = generate_monthly_report(month_year)
            
            if monthly_metrics:
                print("✅ Rapport mensuel généré avec succès")
                return True
            else:
                print("❌ Erreur rapport mensuel")
                return False
        else:
            print("❌ Erreur rapport quotidien")
            return False
            
    except Exception as e:
        print(f"❌ Erreur lors du calcul des métriques: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_complet():
    """Test complet du pipeline ETL avec rapports"""
    print("=" * 60)
    print("🚀 TEST COMPLET DU PIPELINE ECOMMERCE")
    print("=" * 60)
    
    date_test = datetime(2024, 5, 11)
    success = True
    
    # 1. Authentification
    if not test_authentication():
        success = False
        print("❌ Arrêt du test - Authentification échouée")
        return False
    
    # 2. Extraction
    if not test_extraction(date_test):
        success = False
        print("❌ Arrêt du test - Extraction échouée")
        return False
    
    # 3. Nettoyage
    if not test_cleaning(date_test):
        success = False
        print("⚠️  Poursuite du test malgré nettoyage échoué")
    
    # 4. Enrichissement
    if not test_enrichment(date_test):
        success = False
        print("⚠️  Poursuite du test malgré enrichissement échoué")
    
    # 5. Métriques et Rapports
    if not test_metrics(date_test):
        success = False
        print("⚠️  Métriques partiellement échouées")
    
    # Résumé final
    print("\n" + "=" * 60)
    print("📋 RÉSUMÉ DU TEST COMPLET")
    print("=" * 60)
    
    # Vérification des fichiers générés
    print("\n📁 FICHIERS GÉNÉRÉS:")
    
    files_to_check = [
        f"data/raw_data/clients/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/raw_data/products/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/raw_data/orders/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/clean_data/clients/{date_test.year}/{date_test.month}/{date_test.day}.csv", 
        f"data/clean_data/products/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/clean_data/orders/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/enriched_data/{date_test.year}/{date_test.month}/clients_{date_test.day}.csv",
        f"data/enriched_data/{date_test.year}/{date_test.month}/products_{date_test.day}.csv",
        f"data/enriched_data/{date_test.year}/{date_test.month}/orders_{date_test.day}.csv",
        f"data/metrics/daily/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/metrics/monthly/{date_test.year}/2024-05.csv"
    ]
    
    for file_path in files_to_check:
        exists = "✅ EXISTE" if os.path.exists(file_path) else "❌ MANQUANT"
        print(f"  {exists} - {file_path}")
    
    if success:
        print("\n🎉 SUCCÈS - Pipeline ETL complètement fonctionnel!")
        print("📊 Les rapports métier sont générés avec:")
        print("   • Stocks par magasin/site")
        print("   • Nombre de clients par magasin/site") 
        print("   • Chiffre d'affaires quotidien et mensuel")
    else:
        print("\n⚠️  AVERTISSEMENT - Certaines étapes ont échoué")
        print("   Vérifiez les messages d'erreur ci-dessus")
    
    print("=" * 60)
    return success

def test_rapide():
    """Test rapide sans extraction (utilise données existantes)"""
    print("⚡ TEST RAPIDE - Utilisation données existantes")
    
    date_test = datetime(2024, 5, 19)
    
    # Vérifier que les données brutes existent
    required_files = [
        f"data/raw_data/clients/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/raw_data/products/{date_test.year}/{date_test.month}/{date_test.day}.csv",
        f"data/raw_data/orders/{date_test.year}/{date_test.month}/{date_test.day}.csv"
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            print(f"❌ Fichier manquant: {file}")
            print("   Exécutez d'abord le test complet")
            return False
    
    # Exécuter seulement nettoyage + enrichissement + métriques
    success = True
    
    if not test_cleaning(date_test):
        success = False
    
    if not test_enrichment(date_test):
        success = False
    
    if not test_metrics(date_test):
        success = False
    
    return success

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test du pipeline ETL ecommerce")
    parser.add_argument('--rapide', action='store_true', help='Test rapide sans extraction')
    parser.add_argument('--date', help='Date de test (format: YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.date:
        try:
            date_test = datetime.strptime(args.date, '%Y-%m-%d')
            print(f"📅 Date spécifiée: {date_test}")
        except ValueError:
            print("❌ Format de date invalide. Utilisez YYYY-MM-DD")
            exit(1)
    else:
        date_test = datetime(2024, 5, 10)
    
    if args.rapide:
        success = test_rapide()
    else:
        success = test_complet()
    
    exit(0 if success else 1)