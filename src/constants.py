import os

# API params
PATH_LAST_PROCESSED = "./data/last_processed.json"
MAX_LIMIT = 100
OFFSET_LIMIT = 10000 - 1 - MAX_LIMIT

# We have three parameters in the URL:
# 1. MAX_LIMIT: the maximum number of records to be returned by the API
# 2. date_de_publication: the date from which we want to get the data
# 3. offset: the index of the first result
URL_API = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/rappelconso0/records?limit={}&where=date_de_publication%20%3E%20'{}'&order_by=date_de_publication%20DESC&offset={}"
URL_API = URL_API.format(MAX_LIMIT, "{}", "{}")

# POSTGRES PARAMS
user_name = os.getenv("POSTGRES_DOCKER_USER", "localhost")
POSTGRES_URL = f"jdbc:postgresql://{user_name}:5432/postgres"
POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

DB_FIELDS = [
    "reference_fiche",
    "ndeg_de_version",
    "nature_juridique_du_rappel",
    "categorie_de_produit",
    "sous_categorie_de_produit",
    "nom_de_la_marque_du_produit",
    "noms_des_modeles_ou_references",
    "identification_des_produits",
    "conditionnements",
    "date_debut_fin_de_commercialisation",
    "temperature_de_conservation",
    "marque_de_salubrite",
    "informations_complementaires",
    "zone_geographique_de_vente",
    "distributeurs",
    "motif_du_rappel",
    "risques_encourus_par_le_consommateur",
    "preconisations_sanitaires",
    "description_complementaire_du_risque",
    "conduites_a_tenir_par_le_consommateur",
    "numero_de_contact",
    "modalites_de_compensation",
    "date_de_fin_de_la_procedure_de_rappel",
    "informations_complementaires_publiques",
    "liens_vers_les_images",
    "lien_vers_la_liste_des_produits",
    "lien_vers_la_liste_des_distributeurs",
    "lien_vers_affichette_pdf",
    "lien_vers_la_fiche_rappel",
    "rappelguid",
    "date_de_publication",
]
