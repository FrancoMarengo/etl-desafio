import pandas as pd

def limpiar_datos_de_(cat_csv, delimitador=','):
    """Limpia los datos de 'cat_csv', elimina datos incorrectos y realiza una mejor inferencia de tipos.

    Precond: El csv debe tener los siguientes campos en el siguiente orden:
                'show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added',
                'release_year', 'rating', 'duration', 'listed_in' y 'description'.

    Args:
      cat_csv(str): Indica la ruta y nombre del archivo csv a limpiar.
      delimitador(str): Indica el delimitador que utiliza el archivo csv dado, normalmente
                        se utiliza ',' aunque también puede ser ';' dependiendo del csv dado.

    Returns:
      Pandas dataframe: Retorna un dataframe con todos los datos de 'cat_csv' limpios.

    Notes:
      Pandas por defecto establece el delimitador de un archivo csv como ',', esta función realiza el
      mismo trabajo, por lo tanto, se debe especificar el delimitador en caso de ser diferente a ','.
    """
    # Conversión de archivos csv a dataframes de Pandas
    cat_df = pd.read_csv(cat_csv, sep=delimitador)

    # Se eliminan las filas que en "show_id" no comiencen con 's', ya que son completamente erróneas.
    # ".index" devuelve el indice de las filas que cumplan con la condición, mientras que "inplace" realiza la eliminación en el mismo
    # dataframe y no hay que reasignarlo a la misma u otra variable
    cat_df.drop(cat_df[~cat_df.show_id.str.startswith('s')].index, inplace=True)
    
    # Se rellenan los datos del campo "date_added" que son "NaN" con la fecha 
    # estándar "January, 1, 0"
    cat_df.date_added = cat_df.date_added.fillna("January, 1, 0")
    cat_df.date_added = pd.to_datetime(cat_df.date_added)

    # Se rellenan los datos del campo "release_year" con 0
    cat_df.release_year = cat_df.release_year.fillna(0)

    # Arregla la inferencia de datos de Pandas si corresponde
    cat_df.release_year = cat_df.release_year.astype('int64')
    
    return cat_df

def completar_catalogo_con(cat_df1, cat_df2):
    """Completa los campos 'director', 'cast' y 'description' que sean 'NaN' en 'cat_df1'
       con los mismos campos de 'cat_df2' que si tengan un valor válido

       Precond: Ambos dataframes deben tener los siguientes campos en el siguiente orden:
                'show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added',
                'release_year', 'rating', 'duration', 'listed_in' y 'description'.

       Args:
          cat_df1(Pandas dataframe): El catalogo a completar con valores de 'cat_df2'.
          cat_df2(Pandas dataframe): El catalogo que se va a utilizar para completar datos 
                                    de 'cat_df1'.

       Returns:
         Pandas dataframe: Retorna un dataframe de Pandas con los datos de 'cat_df1' pero
                           con campos que antes eran 'NaN' y se completaron con datos
                           de 'cat_df2' que resultaron útiles para 'cat_df1'.

       Notes:
         'cat_df1' obtiene datos de 'cat_df2' siempre y cuando el valor del campo 'title'
         coincida entre los dataframes.
         
    """
    # Realiza un left join entre "cat_df1" y "cat_df2", si "title" de "cat_df1" no coincide con
    # ningun campo "title" de "cat_df2", entonces se hace un join de los datos de "cat_df1"
    # con valores "NaN"
    df_merge = pd.merge(cat_df1, cat_df2, on='title', how='left')

    # Se rellenan los campos "NaN" de "cat_df1" cuando sea posible recuperar datos de "cat_df2"
    df_merge["director_x"].fillna(df_merge["director_y"], inplace=True)
    df_merge["cast_x"].fillna(df_merge["cast_y"], inplace=True)
    df_merge["description_x"].fillna(df_merge["description_y"], inplace=True)

    # Se borran las celdas que eran de "cat_df2"
    df_merge.drop("show_id_y", axis=1, inplace=True)
    df_merge.drop("type_y", axis=1, inplace=True)
    df_merge.drop("director_y", axis=1, inplace=True)
    df_merge.drop("cast_y", axis=1, inplace=True)
    df_merge.drop("country_y", axis=1, inplace=True)
    df_merge.drop("date_added_y", axis=1, inplace=True)
    df_merge.drop("release_year_y", axis=1, inplace=True)
    df_merge.drop("rating_y", axis=1, inplace=True)
    df_merge.drop("duration_y", axis=1, inplace=True)
    df_merge.drop("listed_in_y", axis=1, inplace=True)
    df_merge.drop("description_y", axis=1, inplace=True)

    # Se renombran las celdas que eran de "cat_df1" de manera correcta
    df_merge.rename(columns={'show_id_x': 'show_id',
                               'type_x': 'type',
                               'director_x': 'director',
                               'cast_x': 'ccast',
                               'country_x': 'country',
                               'date_added_x': 'date_added',
                               'release_year_x': 'release_year',
                               'rating_x': 'rating',
                               'duration_x': 'duration',
                               'listed_in_x': 'listed_in',
                               'description_x': 'description'}, inplace=True)
    return df_merge