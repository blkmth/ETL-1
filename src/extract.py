"""
extract.py
extractions des  données brutes de notre fichiers csv source
Responsabilité unique : lire sans modifier
"""

import pandas as pd
import logging
from pathlib import Path

## configuration du logging
logging.basicConfig(
    level = logging.INFO,   ## affiches les messages infos , warnig , error ...
    format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt = "%d-%m-%Y %H:%M:%S"
)

## creer un nom de logger pour identifier la source du message
logger = logging.getLogger("ETL.extract")

## fonction principal : pend un chemin et retourne un dataframe
def extract_from_csv(file_path:str) -> pd.DataFrame:
    path = Path("/home/charles-nguessan/Projects/elt_debutant/data/sales_raw.csv")

    ## gestion des erreurs

    if not path.exists():       ## verifie que le fichiers existe
        raise FileNotFoundError(f"fichier source introuvalble : {file_path}")

    if path.stat().st.size == 0 :       ## verifie que le fichier n'est pas vide
        raise ValueError(f"fichier source vide : {file_path}")

    logger.info(f"extraction des lignes depuis : {file_path}")

    ## journalisation de l'extraction depuis le debut de l'extraction
    df = pd.read_csv(
        file_path ,
        dtype= str, ## lit toutes les colonnes comme du texte
        encoding="utf-8"
    )

    logger.info(f"...")
    return df


