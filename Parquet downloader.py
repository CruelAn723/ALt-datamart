import requests
from tqdm import tqdm
from datetime import datetime, timedelta

def download_file(url, local_filename):
    """
    Télécharge un fichier depuis une URL et l'enregistre localement avec une barre de progression.

    :param url: URL du fichier à télécharger
    :param local_filename: Chemin local où enregistrer le fichier
    """
    # Envoie une requête GET à l'URL
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Vérifie si la requête a réussi

    # Récupère la taille totale du fichier
    total_size = int(response.headers.get('content-length', 0))

    # Ouvre un fichier en mode binaire pour l'écriture
    with open(local_filename, 'wb') as file:
        # Utilise tqdm pour afficher la barre de progression
        for chunk in tqdm(response.iter_content(chunk_size=8192), total=total_size // 8192, unit='KB'):
            file.write(chunk)

def generate_date_range(start_date, end_date):
    """
    Génère une liste de dates mensuelles entre deux dates données.

    :param start_date: Date de début au format 'YYYY-MM'
    :param end_date: Date de fin au format 'YYYY-MM'
    :return: Liste de dates au format 'YYYY-MM'
    """
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    date_list = []
    while start <= end:
        date_list.append(start.strftime("%Y-%m"))
        start += timedelta(days=31)  # Passe au mois suivant
        start = start.replace(day=1)  # Fixe le jour au 1er du mois
    return date_list

def download_files_in_date_range(base_url, start_date, end_date):
    """
    Télécharge des fichiers sur un intervalle de dates mensuelles donné.

    :param base_url: URL de base sans la partie date
    :param start_date: Date de début au format 'YYYY-MM'
    :param end_date: Date de fin au format 'YYYY-MM'
    """
    dates = generate_date_range(start_date, end_date)
    for date in dates:
        url = f"{base_url}_{date}.parquet"
        local_filename = f"yellow_tripdata_{date}.parquet"
        print(f"Téléchargement de {url}...")
        download_file(url, local_filename)
        print(f"{local_filename} téléchargé avec succès.")

# URL de base pour les fichiers à télécharger
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"

# Dates de début et de fin
start_date = "2018-01"
end_date = "2023-01"

# Appelle la fonction pour télécharger les fichiers dans l'intervalle de dates
download_files_in_date_range(base_url, start_date, end_date)
