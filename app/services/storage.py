import os
from typing import Optional, List
import boto3
from botocore.client import Config


class StorageClient:
    def __init__(self):
        self.bucket = os.getenv("R2_BUCKET")
        self.public_base = (os.getenv("R2_PUBLIC_BASE") or "").rstrip("/")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("R2_ENDPOINT"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )

    # ----------- internal helper -------------
    def _list_all_objects(self, prefix: str) -> List[dict]:
        """Отримати всі об’єкти з префіксом (підтримка пагінації)."""
        all_items: List[dict] = []
        continuation = None

        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix}
            if continuation:
                params["ContinuationToken"] = continuation

            resp = self.s3.list_objects_v2(**params)
            all_items.extend(resp.get("Contents", []) or [])

            if not resp.get("IsTruncated"):
                break
            continuation = resp.get("NextContinuationToken")

        return all_items

    # ----------- public API -----------------
    def latest_key(self, prefix: str) -> Optional[str]:
        items = self._list_all_objects(prefix)
        if not items:
            return None
        return max(items, key=lambda o: o["LastModified"])["Key"]

    def url_for(self, key: Optional[str], expires_sec: int = 3600) -> Optional[str]:
        if not key:
            return None
        if self.public_base:
            return f"{self.public_base}/{key}"
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_sec,
        )

    def upload_file(
            self,
            local_path: str,
            key: str,
            content_type: Optional[str] = None,
            cleanup_prefix: Optional[str] = None,
            keep_last: int = 7,
    ) -> str:
        """Завантажити файл і (опціонально) прибрати старі під cleanup_prefix."""
        extra = {"ContentType": content_type} if content_type else None
        self.s3.upload_file(local_path, self.bucket, key, ExtraArgs=extra)

        # опційне прибирання
        if cleanup_prefix:
            try:
                self.cleanup_old_files(cleanup_prefix, keep=keep_last)
            except Exception as e:
                print(f"⚠️ Cleanup failed for {cleanup_prefix}: {e}")

        return self.url_for(key)

    def cleanup_old_files(self, prefix: str, keep: int = 7) -> None:
        """Видалити всі старі файли у префіксі, залишивши лише N останніх."""
        items = self._list_all_objects(prefix)
        if not items or len(items) <= keep:
            return

        items.sort(key=lambda o: o["LastModified"], reverse=True)
        to_delete = items[keep:]

        print(f"🧹 Cleanup {prefix}: keeping {keep}, deleting {len(to_delete)} old files")

        for obj in to_delete:
            try:
                self.s3.delete_object(Bucket=self.bucket, Key=obj["Key"])
            except Exception as e:
                print(f"⚠️ Failed to delete {obj['Key']}: {e}")
