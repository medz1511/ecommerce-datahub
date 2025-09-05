from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

def get_google_drive_service():
    """
    Authentification réutilisable avec Google Drive
    Retourne une instance GoogleDrive authentifiée
    """
    try:
        gauth = GoogleAuth()
        
        if not os.path.exists('client_secret.json'):
            raise FileNotFoundError("client_secret.json introuvable")
        
        gauth.LoadClientConfigFile('client_secret.json')
        
        if os.path.exists('credentials.json'):
            gauth.LoadCredentialsFile('credentials.json')
        
        if gauth.credentials is None:
            print("Authentification requise - ouverture navigateur...")
            gauth.LocalWebserverAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
        
        gauth.SaveCredentialsFile('credentials.json')
        
        return GoogleDrive(gauth)
        
    except Exception as e:
        print(f"Erreur d'authentification Google Drive: {e}")
        raise

def test_connection():
    """Test simple de connexion"""
    try:
        drive = get_google_drive_service()
        files = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
        print(f"Connecte! {len(files)} fichiers a la racine")
        return True
    except Exception as e:
        print(f"Echec connexion: {e}")
        return False