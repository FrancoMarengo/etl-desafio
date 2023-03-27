import boto3 
import botocore
import os

def descargar_archivo_s3(bucket, access_key, secret_access_key, nombre_archivo, ruta_local):
    """Descarga y almacena archivos de Amazon S3
    
    Args:
      bucket(str): Nombre del bucket del que descargar el archivo
      access_key(str): Key para conectarse
      secret_access_key(str): Key secreta para conectarse
      nombre_archivo(str): Nombre del archivo que se quiere descargar
      ruta_local(str): Ruta en la que se va a alojar el archivo descargado, debe incluir el nombre del archivo
      
    Raises:
      ClientError: Se puede ocasionar si el archivo a descargar no existe
    """
    s3 = boto3.client(
        service_name='s3', 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_access_key
    ) 
    try:
        s3.download_file(bucket, nombre_archivo, ruta_local) # Descarga el archivo del bucket especificado
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404": # Si el archivo no existe retorna un error
            print("El archivo no existe.")