"""
transform.py
Nettoyage, validation et enrichissement des données.
"""
from unicodedata import category

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

    ## COPIES DE SAUVEGARDE
    df_clean = df.copy()

    ## NETTOYAGE DES ESAPCES PARASITES
    str_cols = ["transactions_id", "customer_name", "product", "category", "country"]
    for col in str_cols:
       ## .str.strip() supprime les espaces en debut et fin && .ne(df_clean[col]).sum() compte ce qui a hcangé pour le rapport
        fixed = df_clean[col].str.strip().ne(df_clean[col]).sum()
        df_clean[col] = df_clean[col].str.strip()
        if fixed > 0 :
           ## enregistré tout ce qui a changé
            report.issues_fixed += fixed

    ## CONVERSION DES TYPES
    df_clean["quantity"] = pd.to_numeric(df_clean["quantity"], errors="coerce")
    df_clean["unit_price"] = pd.to_numeric(df_clean["unit_price"], errors= "coerce")
    df_clean["sale_date"] = pd.to_datetime(df_clean["sale_date"],errors= "coerce",  format="%d-%m-%Y")

    ## NORMALISATION DE LA CASSE
    """
    construction de dictionnaire de correction avec
    la premiere lettre en majiscule.
    """
    category_map = {cat().upper(): cat for cat in VALID_CATEGORIES}
    category_map.update({cat.capitalize(): cat for cat in VALID_CATEGORIES})
    df_clean["category"] = df_clean["category"].map(lambda x : category_map.get(x, x))

    ## VALIDATION ET REJET
    """
    creation de masque booleen pour validation 
    d'une ligne si toutes les conditions dus masque sont remplit.
    """
    mask = (
        df_clean ["quantity"].notna() & (df_clean["quantity"]>0) &  ## pas de vente = 0 ou negative
        df_clean["unit_price"].notna() & (df_clean["unit_price"]>0) &   ## pas de prix negatif
        df_clean["sale_date"].notna()  &                                ## pas de date invalid
        df_clean["transaction_id"].notna() &                            ## transaction di non null clé primaire
        df_clean["category"].isin(VALID_CATEGORIES)&                    ## category dans valid_category
        df_clean["country"].isin(VALID_COUNTRY)                         ## country dans valid_county
    )

    rejected_ligne = df_clean[~mask]       ## ~mask -> inverse le mask et log les lignes invalides
    df_clean = df_clean[mask].copy()
    report.rows_rejected += report.rows_rejected - len(df_clean)

    ## COLONNES DERIVÉES
    df_clean["quantity"] = df_clean["quantity"].astype(int)     ##converti qauntity en entier
    df_clean["unit_price"] = df_clean["unit_price"].round(3)    ## 3 chriffre après la virgule pur le prix
    df_clean["total_amount"] = (df_clean["quantity"] * df_clean["unit_price"]).round(2) ## calcul le prix total
    df_clean["sale_date"] = df_clean["sale_date"].dt.date   ## extrait juste la date sans l'heure

    ## DEDUPLICATION
    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["transaction_id"], keep = "first")  ## supprime les lignes ayant le meme transaction_id
    dupes = before - len(df_clean)

    ## FINALISATION
    cols = [
        "transaction_id", "customer_name", "product", "category",
        "quantity", "unit_price", "total_amount", "sale_date", "country"
    ]
    df_clean = df_clean[cols].reset_index(drop = True)
    report.rows_rejected = len(df_clean)
    return df_clean, report