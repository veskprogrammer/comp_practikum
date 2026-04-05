import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Загружаем ключи из файла .env
load_dotenv()

class S3StorageManager:
    def __init__(self):
        """Подключение к Яндекс Облаку"""
        self.bucket_name = os.getenv('BUCKET_NAME')
        session = boto3.session.Session()
        self.s3_client = session.client(
            service_name='s3',
            endpoint_url=os.getenv('ENDPOINT_URL'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='ru-central1'
        )

    def list_files(self):
        """Показать все файлы в бакете"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' not in response:
                print("В бакете нет файлов.")
                return []
            
            file_list = [item['Key'] for item in response['Contents']]
            print(f"Найдено файлов: {len(file_list)}")
            for file_name in file_list:
                print(f"   - {file_name}")
            return file_list
        except ClientError as e:
            print(f"! - Ошибка: {e}")
            return []

    def upload_file(self, local_file_path, object_name=None):
        """Загрузить файл в облако"""
        if object_name is None:
            object_name = os.path.basename(local_file_path)
        
        try:
            self.s3_client.upload_file(
                local_file_path, 
                self.bucket_name, 
                object_name,
                ExtraArgs={'StorageClass': 'STANDARD'}
            )
            print(f"Файл '{local_file_path}' загружен как '{object_name}'")
            return True
        except FileNotFoundError:
            print(f"! - Файл '{local_file_path}' не найден!")
            return False
        except ClientError as e:
            print(f"! - Ошибка загрузки: {e}")
            return False

    def download_file(self, object_name, local_file_path=None):
        """Скачать файл из облака"""
        if local_file_path is None:
            local_file_path = object_name
            
        try:
            self.s3_client.download_file(self.bucket_name, object_name, local_file_path)
            print(f"Файл '{object_name}' скачан как '{local_file_path}'")
            return True
        except ClientError as e:
            print(f"! - Ошибка скачивания: {e}")
            return False

    def delete_file(self, object_name):
        """Удалить файл из облака"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_name)
            print(f"Файл '{object_name}' удалён")
            return True
        except ClientError as e:
            print(f"! - Ошибка удаления: {e}")
            return False

def main():
    print("=" * 50)
    print("---РАБОТА С YANDEX OBJECT STORAGE---")
    print("=" * 50)
    
    storage = S3StorageManager()
    
    # 1. Создаём тестовый файл
    print("\n1️. Создаём тестовый файл...")
    with open("test.txt", "w", encoding="utf-8") as f:
        f.write("Привет! Это тестовый файл.")
    print("Файл test.txt создан")
    
    # 2. Загружаем файл
    print("\n2️. Загружаем файл в облако...")
    storage.upload_file("test.txt")
    
    # 3. Смотрим список файлов
    print("\n3️. Список файлов в облаке:")
    storage.list_files()
    
    # 4. Скачиваем файл
    print("\n4️. Скачиваем файл из облака...")
    storage.download_file("test.txt", "downloaded_test.txt")
    
    # 5. Удаляем файл
    print("\n5️. Удаляем файл из облака...")
    storage.delete_file("test.txt")
    
    # 6. Проверяем, что удалилось
    print("\n6️. Проверяем список после удаления:")
    storage.list_files()
    
    # Удаляем локальные файлы
    os.remove("test.txt")
    if os.path.exists("downloaded_test.txt"):
        os.remove("downloaded_test.txt")
    
    print("\n" + "=" * 50)
    print("---ВСЕ ОПЕРАЦИИ ВЫПОЛНЕНЫ УСПЕШНО!---")
    print("=" * 50)

if __name__ == "__main__":
    main()