"""
transform.py
Nettoyage, validation et enrichissement des données.
"""
import pandas as pd
import logging
from dataclasses import dataclass, field

## recuperation du logger dans extract.py
logger = logging.getLogger("ETL.transform")

## utilisation de set pour fluidité de recherche O(1)
VALID_CATEGORIES = {"Électronique", "Mobilier", "Logiciels", "Accessoires"}
VALID_COUNTRY  =  {"France", "Belgique", "Suisse", "Canada", "Allemagne", "Espagne"}

@dataclass
## rapport pour stocker les resultats de la transformation
class TransformReport :
    rows_input : int = 0
    rows_output :int = 0
    rows_rejected : int = 0
    issues_fixed: int = 0

    ## faire un resumer en utilisant les chiffres stockés
    def summary (self) -> str :
        rate = (self.rows_rejected / self.rows_input * 100) \
            if self.rows_input else 0   ## gestion des erreurs  ( divisions par 0 )
        return(
            f"Rapport Transform :\n"
            f"Input: {self.rows_input} lignes\n"
            f"Output: {self.rows_output} lignes\n"
            f"Rejetées: {self.rows_rejected} ({rate:.1f}%)\n"
            f"Corrigées : {self.issues_fixed} anomalies"
        )

## fonction qui prend un df brut et retourne un tuple (df + rapport de metrique )
def transform (df: pd.DataFrame) -> tuple[pd.DataFrame, TransformReport]:
    report = TransformReport(rows_input=len(df))    ## initialise le rapport en enregistraant le nombre de ligne en entrée

    ## copie de sauvegarde
    df_clean = df.copy()

    ## nettoyage des espaces parasites
    str_cols = ["transactions_id", "customer_name", "product", "category", "country"]
    for col in str_cols:
       ## .str.strip() supprime les espaces en debut et fin && .ne(df_clean[col]).sum() compte ce qui a hcangé pour le rapport
        fixed = df_clean[col].str.strip().ne(df_clean[col]).sum()
        df_clean[col] = df_clean[col].str.strip()
        if fixed > 0 :
           ## enregistré tout ce qui a changé
            report.issues_fixed += fixed