import os
import logging
from lib2to3.pgen2 import driver
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from dotenv import load_dotenv  ## lit le fichier .env
from sqlalchemy.dialects.postgresql import dialect

load_dotenv()

"""
construction du moteur sqlalchemy depuis les variables d'environnement 
"""
def get_engine() -> Engine:

    ## dialect+driver://user:password@host:port/database
    url = (
        f"postgresql + psycopg2://"
        f"{os.getenv('DB_USER')} : {os.getenv('db_password')}"
        f"@{os.getenv('DB_HOST')} : {os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )

    return create_engine (url, pool_pre_ping=True)  ## creation du moteur & teste la connexion avant chaque utilisation

"""
chargement dans la base de donnée postgresql
"""
def load_to_postgres (
        df : pd.DataFrame,
        table_name : str = "sales_clean",
        if_exists: str = "append" ,
        chunksize: int = 500
)   -> int :
    if df.empty :
        logging.warning ("dataframe vide -- aucun chargement ")
        return 0

    engine = get_engine()  ## engine créé ici, disponible partout en dessous

    logging.info(f"Chargement de {len(df)} lignes vers '{table_name}'")

    try:
        df.to_sql(
            name=table_name,
            con=engine,  # ✅ engine existe
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        with engine.connect() as conn:  # ✅ engine existe
            total = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()   ## counter le nombre de ligne charggée
            logging.info(f"Succès | {len(df)} insérées | Total en base : {total}")
        return len(df)  ## retoruner le nombre de igne chargées

    ## gestion des erreurs
    except Exception as e:
        logging.error(f"erreur de chargement : {e}")
        raise
    finally:
        engine.dispose()