import io
from datetime import datetime
from typing import Tuple, Iterator, Mapping, Any

import boto3
import pandas as pd


class S3Manager:
    def __init__(self, *, endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str, bucket: str):
        self.s3_client = boto3.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.bucket = bucket

    def get_files(self, prefix: str) -> Iterator[Mapping[str, Any]]:
        """
        Возвращает генератор со всей метаинформацией по каждому файлу,
        найденному по указанному prefix
        """

        prefix = prefix.rstrip('/') + '/'

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        yield from (file for page in pages for file in page.get('Contents', []))

    def get_file_keys(self, prefix: str) -> Iterator[str]:
        """
        Возвращает генератор с названиями файлов,
        найденных по указанному prefix
        """

        yield from (file['Key'] for file in self.get_files(prefix))

    def get_last_modified_file(self, prefix: str) -> Tuple[str, datetime] | None:
        """
        Возвращает последний измененный файл по указанному prefix.
        Возвращаемое значение является кортежем, состоящим из пути до файла (без бакета) и даты изменения.
        Если не находится ни одного файла, то будет возвращено None
        """

        files = [(file['Key'], file['LastModified']) for file in self.get_files(prefix)]
        last_modified_file = max(files, key=lambda file: file[1]) if files else None
        return last_modified_file

    def file_exists(self, prefix: str, filename: str) -> bool:
        """
        Проверяет, есть ли по указанному prefix файл с названием filename.
        Возвращает булев результат
        """
        return any(file.endswith(filename) for file in self.get_file_keys(prefix))

    def get_subprefix_last_segments(self, prefix: str, delimiter: str = '/') -> Iterator[str]:
        """
        Возвращает генератор с названиям "подпапок" по указанному prefix.
        Например, есть путь 'api/parent/child/file.txt'.
        Если в prefix передать 'api/parent/', то по очереди вернутся все child "подпапки"
        """

        prefix = prefix.rstrip('/') + '/'

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter=delimiter)

        for page in pages:
            for subprefix in page.get('CommonPrefixes', []):
                *_, subprefix_last_segment, _ = subprefix['Prefix'].split('/')
                yield subprefix_last_segment

    def save_to_parquet(self, *, df: pd.DataFrame, key: str):
        """
        Сохраняет датафрейм df в S3 в parquet-формате по указанному префиксу key
        """

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue()
        )
