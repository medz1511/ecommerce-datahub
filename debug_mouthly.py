# debug_monthly.py
import pandas as pd
import os

def debug_monthly_calculation():
    """Debug du calcul mensuel"""
    print("🔍 DEBUG CALCUL MENSUEL")
    print("=" * 50)
    
    # Vérifier les fichiers quotidiens
    daily_dir = "data/metrics/daily/2024/5/"
    
    if not os.path.exists(daily_dir):
        print(f"❌ Dossier introuvable: {daily_dir}")
        return
    
    daily_files = [f for f in os.listdir(daily_dir) if f.endswith('.csv')]
    print(f"📁 Fichiers quotidiens trouvés: {len(daily_files)}")
    
    total_revenue = 0
    
    for file in daily_files:
        file_path = os.path.join(daily_dir, file)
        try:
            df = pd.read_csv(file_path)
            print(f"\n📄 {file}:")
            print(f"   Colonnes: {df.columns.tolist()}")
            
            if 'daily_revenue' in df.columns:
                daily_rev = df['daily_revenue'].iloc[0]
                total_revenue += daily_rev
                print(f"   CA quotidien: {daily_rev:.2f}€")
            else:
                print(f"   ❌ Colonne daily_revenue manquante")
                
            print(f"   Données: {df.to_dict('records')}")
            
        except Exception as e:
            print(f"   ❌ Erreur lecture: {e}")
    
    print(f"\n💰 CA MENSUEL TOTAL (somme): {total_revenue:.2f}€")

if __name__ == "__main__":
    debug_monthly_calculation()