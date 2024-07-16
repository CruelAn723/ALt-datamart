import datetime
import requests
import calendar

def download_previous_month_data():
    # Récupérer la date du jour
    today = datetime.datetime.today()
    
    while True:
        # Récupérer l'année et le mois actuels
        year = today.year
        month = today.month
        
        # Formater le mois pour qu'il soit toujours sur deux chiffres
        formatted_month = f"{month:02d}"
        
        # Construire l'URL du fichier à télécharger
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{formatted_month}.parquet"
        
        # Nom du fichier local où le fichier sera sauvegardé
        file_name = f"yellow_tripdata_{year}-{formatted_month}.parquet"
        
        # Télécharger le fichier
        response = requests.get(url)
        
        # Vérifier si le fichier est disponible
        if response.status_code == 200:
            with open(file_name, 'wb') as file:
                file.write(response.content)
            print(f"Fichier téléchargé et sauvegardé sous le nom {file_name}")
            break  # Sortir de la boucle si le fichier est téléchargé avec succès
        else:
            print(f"Le fichier pour le mois {formatted_month}-{year} n'est pas disponible.")
            
            # Reculer d'un mois
            today = today - datetime.timedelta(days=today.day)  # Reculer au premier jour du mois précédent
            if today.month == 12:
                today = today.replace(year=today.year - 1, month=12)
            else:
                today = today.replace(month=today.month - 1)
    
    print("Fin du téléchargement.")

# Appeler la fonction pour télécharger les données du mois précédent
download_previous_month_data()
