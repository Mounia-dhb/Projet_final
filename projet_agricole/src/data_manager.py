import pandas as pd
import os

class AgriculturalDataManager:
    def __init__(self):
        """
        Initialise le gestionnaire de données agricoles avec les chemins absolus.
        """
        # Chemin absolu du répertoire "data" basé sur le répertoire du script
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
        
        # Construire des chemins absolus pour chaque fichier CSV
        self.monitoring_data_path = os.path.join(base_path, "monitoring_cultures.csv")
        self.weather_data_path = os.path.join(base_path, "meteo_detaillee.csv")
        self.soil_data_path = os.path.join(base_path, "sols.csv")
        self.yield_history_path = os.path.join(base_path, "historique_rendements.csv")
        
        # Variables pour les DataFrames
        self.monitoring_data = None
        self.weather_data = None
        self.soil_data = None
        self.yield_history = None

    def load_data(self):
        """
        Charge les données à partir des fichiers CSV.
        """
        try:
            # Charger les fichiers CSV
            self.monitoring_data = pd.read_csv(self.monitoring_data_path, parse_dates=['date'])
            self.weather_data = pd.read_csv(self.weather_data_path, parse_dates=['date'])
            self.soil_data = pd.read_csv(self.soil_data_path)
            self.yield_history = pd.read_csv(self.yield_history_path)
            print("Les données ont été chargées avec succès.")
        except Exception as e:
            print(f"Erreur lors du chargement des données : {e}")


    def _setup_temporal_indices(self):
        """
        Configure les index temporels pour les fichiers qui utilisent des données temporelles.
        """
        try:
            self.monitoring_data.set_index('date', inplace=True)
            self.weather_data.set_index('date', inplace=True)
            print("Index temporels configurés avec succès.")
        except Exception as e:
            print(f"Erreur lors de la configuration des index temporels : {e}")

    def prepare_features(self):
        """
        Prépare les caractéristiques en fusionnant les données de monitoring,
        météo et sol. Enrichit également les données avec des informations pertinentes.
        """
        try:
            # Fusion des données de monitoring et météo
            combined_data = pd.merge_asof(
                self.monitoring_data.sort_index(),
                self.weather_data.sort_index(),
                left_index=True,
                right_index=True,
                direction='nearest'
            )

            # Fusion des données avec les sols
            combined_data = combined_data.merge(
                self.soil_data,
                how='left',
                on='parcelle_id'
            )

            print("Les données ont été fusionnées avec succès.")
            return combined_data
        except Exception as e:
            print(f"Erreur lors de la préparation des données : {e}")
            return None









'''
# Test du script
if __name__ == "__main__":
    data_manager = AgriculturalDataManager()
    data_manager.load_data()
    print("Aperçu des données de monitoring :")
    if data_manager.monitoring_data is not None:
        print(data_manager.monitoring_data.head())
    else:
        print("Les données de monitoring n'ont pas pu être chargées.")
'''
'''
# Test des nouvelles fonctionnalités
if __name__ == "__main__":
    data_manager = AgriculturalDataManager()
    data_manager.load_data()
    data_manager._setup_temporal_indices()
    prepared_data = data_manager.prepare_features()

    if prepared_data is not None:
        print("Aperçu des données préparées :")
        print(prepared_data.head())
'''