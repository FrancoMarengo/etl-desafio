import environ
from extractS3 import descargar_archivo_s3
from transform_catalogs import limpiar_datos_de_
from transform_catalogs import completar_catalogo_con
from load_db import cargar_df_a_bdd
import pandas as pd
import sqlalchemy as db

# Se realiza la busqueda y lectura de un archivo .env para buscar las credenciales necesarias
env = environ.Env()
environ.Env.read_env()

# Credenciales para realizar la petición a Amazon S3
AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
DB_CONSTR = env("DB_CONSTR")

def eliminar_columnas_de_df(lista_nombres_col, df):
    nuevo_df = df.drop(columns = lista_nombres_col)
    return nuevo_df
 

def main():
    # ----- Extract -----
    
    descargar_archivo_s3("desafio-rkd", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "disney_plus_titles.csv", "disney_plus_titles.csv")
    descargar_archivo_s3("desafio-rkd", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "netflix_titles.csv", "netflix_titles.csv")
    
    # ----- Transform -----

    clean_disneyp_titles_df = limpiar_datos_de_("disney_plus_titles.csv")
    clean_netflix_titles_df = limpiar_datos_de_("netflix_titles.csv", ";")
    filled_disneyp_titles_df = completar_catalogo_con(clean_disneyp_titles_df, clean_netflix_titles_df)
    filled_netlfix_titles_df = completar_catalogo_con(clean_netflix_titles_df, clean_disneyp_titles_df)
    filled_disneyp_titles_df.to_csv("clean_disney_plus_titles.csv")
    filled_netlfix_titles_df.to_csv("clean_netflix_titles.csv")
    
    # ----- Load -----

    # Añade una columna nueva a ambos dataframes, con el valor correspondiente segun el catálogo
    filled_disneyp_titles_df = filled_disneyp_titles_df.assign(catalog_name='Disney plus')
    filled_netlfix_titles_df = filled_netlfix_titles_df.assign(catalog_name= 'Netflix')

    # Listas con columnas a eliminar segun cuál sean las columnas que no cumplen la estructura de la tabla correspondiente
    a_eliminar_para_show = ["type", "ccast", "rating", "director", "country", "date_added", "release_year", "listed_in"]
    a_eliminar_para_team = ["type", "rating", "country", "date_added", "release_year", "listed_in", "duration", "description", "title"]
    a_eliminar_para_year = ["type", "title", "rating", "country", "listed_in", "duration", "description", "listed_in", "director", "ccast"]
    a_eliminar_para_category = ["title", "duration", "director", "ccast", "release_year", "date_added", "description"]
    
    # estructuras de tipo dict que se utilizan para declarar la estructura que tiene el df
    estructura_para_show = {
        'show_id': db.VARCHAR(10),
        'title': db.VARCHAR(180),
        'duration': db.VARCHAR(20),
        'description': db.VARCHAR(350),
        'catalog_name': db.VARCHAR(20)
    }

    estructura_para_team = {
        'show_id': db.VARCHAR(10),
        'catalog_name': db.VARCHAR(20),
        'ccast': db.VARCHAR(950),
        'director': db.VARCHAR(250),
    }

    estructura_para_year = {
        'show_id': db.VARCHAR(10),
        'catalog_name': db.VARCHAR(20),
        'date_added': db.DATE,
        'release_year': db.INTEGER,
    }

    estructura_para_category = {
        'show_id': db.VARCHAR(10),
        'catalog_name': db.VARCHAR(20),
        'listed_in': db.VARCHAR(150),
        'rating': db.VARCHAR(10),
        'type': db.VARCHAR(20),
        'country': db.VARCHAR(150)
    }
    
    # Realiza la eliminación de las columnas que no deben estar
    show_disneyp_df = eliminar_columnas_de_df(a_eliminar_para_show, filled_disneyp_titles_df)
    show_netflix_df = eliminar_columnas_de_df(a_eliminar_para_show, filled_netlfix_titles_df)
    
    # Carga a la BDD
    cargar_df_a_bdd(show_disneyp_df, DB_CONSTR, "show", estructura_para_show)
    cargar_df_a_bdd(show_netflix_df, DB_CONSTR, "show", estructura_para_show)
    
    # Realiza la eliminación de las columnas que no deben estar
    team_disneyp_df = eliminar_columnas_de_df(a_eliminar_para_team, filled_disneyp_titles_df)
    team_netflix_df = eliminar_columnas_de_df(a_eliminar_para_team, filled_netlfix_titles_df)
    
    # Carga a la BDD
    cargar_df_a_bdd(team_disneyp_df, DB_CONSTR, "team", estructura_para_team)
    cargar_df_a_bdd(team_netflix_df, DB_CONSTR, "team", estructura_para_team)

    # Realiza la eliminación de las columnas que no deben estar
    year_disneyp_df = eliminar_columnas_de_df(a_eliminar_para_year, filled_disneyp_titles_df)
    year_netflix_df = eliminar_columnas_de_df(a_eliminar_para_year, filled_netlfix_titles_df)

    # Carga a la BDD
    cargar_df_a_bdd(year_disneyp_df, DB_CONSTR, "year", estructura_para_year)
    cargar_df_a_bdd(year_netflix_df, DB_CONSTR, "year", estructura_para_year)
    
    # Realiza la eliminación de las columnas que no deben estar
    category_disneyp_df = eliminar_columnas_de_df(a_eliminar_para_category, filled_disneyp_titles_df)
    category_netflix_df = eliminar_columnas_de_df(a_eliminar_para_category, filled_netlfix_titles_df)
    
     # Carga a la BDD
    cargar_df_a_bdd(category_disneyp_df, DB_CONSTR, "category", estructura_para_category)
    cargar_df_a_bdd(category_netflix_df, DB_CONSTR, "category", estructura_para_category)


if __name__ == "__main__":
    main()