import os, zlib, json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobType


class BlobInterface:
    def __init__(self):
        load_dotenv()
        CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        CONTAINER_NAME = os.getenv("AZURE_BLOB_CONTAINER_NAME")

        if CONNECTION_STRING is None or CONTAINER_NAME is None:
            raise Exception(
                "NO ENV variables found. AZURE_STORAGE_CONNECTION_STRING or AZURE_BLOB_CONTAINER_NAME are missing"
            )

        self.blob_service_client = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )
        self.blob_container_client = self.blob_service_client.get_container_client(
            CONTAINER_NAME
        )

    def upload_json_block_blob(
        self,
        blob_name: str,
        blob_json: dict[any:any],
        blob_tags: dict[str:str],
        compression_level: int = 9,
    ) -> None:
        """
        Compresses the given json and upload it on azure blob storage
        """
        content_string = json.dumps(blob_json).replace(" ", "")
        content_bytes = content_string.encode("utf-8")
        blob_content = zlib.compress(content_bytes, level=compression_level)

        self.blob_container_client.upload_blob(
            blob_name,
            blob_content,
            BlobType.BLOCKBLOB,
            len(blob_content),
            tags=blob_tags,
        )

    def download_json_block_blob(self, blob_name: str) -> dict[any, any]:
        """
        Download a block blob containing a compressed json, uncompresses it and return it as a dict with tags
        """
        blob_client = self.blob_container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        uncompressed = zlib.decompress(blob_data)
        string_data = uncompressed.decode("utf-8")
        dict_data = dict(string_data)
        tags = blob_client.get_blob_tags()
        dict_data["tags"] = tags
        return dict_data

    def find_blobs_by_tags(self, query: str):
        """
        Find all blobs with a given tag and returns them as an iterable
        """
        return self.blob_container_client.find_blobs_by_tags(query)
