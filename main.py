import os
import io
import tempfile
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient import http
from google.cloud import storage
from google.cloud import secretmanager

BUCKET_NAME = os.environ.get('BUCKET_NAME')
DRIVE_FOLDER_ID_YELP = os.environ.get('DRIVE_FOLDER_ID_YELP')
DRIVE_FOLDER_ID_GOOGLE_METADATA = os.environ.get('DRIVE_FOLDER_ID_GOOGLE_METADATA')
DRIVE_FOLDER_ID_GOOGLE_REVIEWS = os.environ.get('DRIVE_FOLDER_ID_GOOGLE_REVIEWS')
SERVICE_ACCOUNT_FILE = os.environ.get('SERVICE_ACCOUNT_FILE')

def access_secret_version(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{client.project}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def upload_file_to_gcs(file_path, dest_file_name, bucket_name):
    """Carga un archivo a Google Cloud Storage."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(dest_file_name)
        blob.upload_from_filename(file_path)
        print(f'Archivo {dest_file_name} cargado a {bucket_name}.')
    except Exception as e:
        print(f'Error al cargar el archivo {dest_file_name}: {e}')

def list_drive_files(folder_id):
    """Lista los archivos y carpetas en una carpeta de Google Drive."""
    query = f"'{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    return results.get('files', [])

def download_file_from_drive(file_id, file_name):
    """Descarga un archivo de Google Drive."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = http.MediaIoBaseDownload(fh, request)
    done = False

    while not done:
        status, done = downloader.next_chunk()
        print(f'Descargando {file_name} {int(status.progress() * 100)}%.')

    fh.seek(0)
    temp_file_path = os.path.join(tempfile.gettempdir(), file_name)
    with open(temp_file_path, 'wb') as f:
        f.write(fh.read())
    print(f'Archivo {file_name} descargado de Google Drive.')
    return temp_file_path

def file_exists_in_gcs(blob_name, bucket_name):
    """Verifica si un archivo existe en Google Cloud Storage."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()

def process_drive_folder(folder_id, gcs_folder):
    """Procesa los archivos y subcarpetas en una carpeta de Google Drive y los carga a GCS."""
    items = list_drive_files(folder_id)
    for item in items:
        item_id = item['id']
        item_name = item['name']
        
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # Si es una subcarpeta, llamar recursivamente

            process_drive_folder(item_id, f"{gcs_folder}/{item_name}")
        else:
            # Si es un archivo, descargarlo y verificar si ya existe en GCS
            dest_file_name = f'{gcs_folder}/{item_name}'

            if not file_exists_in_gcs(dest_file_name, BUCKET_NAME):
                temp_file_path = download_file_from_drive(item_id, item_name)
                upload_file_to_gcs(temp_file_path, dest_file_name, BUCKET_NAME)
                os.remove(temp_file_path)
            else:
                print(f'El archivo {dest_file_name} ya existe en {BUCKET_NAME}, omitiendo la carga.')

def main():
    # Autenticación con Google Drive
    service_account_info = access_secret_version("service-account-key")
    credentials = service_account.Credentials.from_service_account_info(json.loads(service_account_info))
    global drive_service, storage_client

    drive_service = build('drive', 'v3', credentials=credentials)

    # Autenticación con Google Cloud Storage
    storage_client = storage.Client(credentials=credentials)
    # Procesar la carpeta "yelp"
    process_drive_folder(DRIVE_FOLDER_ID_YELP, "yelp")
    
    # Procesar la carpeta "metadata"
    process_drive_folder(DRIVE_FOLDER_ID_GOOGLE_METADATA, "metadata")

    # Procesar la carpeta "reviews"
    process_drive_folder(DRIVE_FOLDER_ID_GOOGLE_REVIEWS, "reviews")

if __name__ == '__main__':
    main()