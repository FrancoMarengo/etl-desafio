from sqlalchemy import create_engine
import sqlalchemy as db
import pandas as pd

def cargar_df_a_bdd(cat_df, bdd, table, structure):
    """Carga los datos de 'cat_df' a la tabla recibida por parámetro ubicada en la base de datos 'bdd' dada.

    Precond:
      * Debe existir una tabla con el nombre recibido por parámetro en la base de datos dada.
      * La tabla en la base de datos dada debe tener la misma estructura de datos que 'cat_df'
    
    Args:
      cat_df(Pandas dataframe): Un dataframe de pandas con los datos que se deben cargar.
      bdd(str): Un string que debe indicar la URI para conectar con la base de datos y
                debe tener la siguiente estructura:
                database+dialect://username:password@database_url:port/database_name
      table(str): el nombre de la tabla donde se ingestan los datos
      structure(dict): la estructura de la tabla
    """
    engine = create_engine(bdd)
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        cat_df.to_sql(table, con=connection, index=False, if_exists='append', dtype=structure)