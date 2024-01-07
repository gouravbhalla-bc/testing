import base64
import boto3
import time

from altonomy.ace import config


class S3Ctrl(object):

    def __init__(self):
        self.elwood_bucket_name = config.S3_BUCKET_ELWOOD
        self.session = boto3.Session(
            aws_access_key_id=config.S3_ACCESS_KEY,
            aws_secret_access_key=config.S3_SECRET_KEY,
        )
        self.s3 = self.session.resource('s3')
        self.s3_client = boto3.client('s3', aws_access_key_id=config.S3_ACCESS_KEY, aws_secret_access_key=config.S3_SECRET_KEY)

    def get_bucket_files(self):
        try:
            location = self.s3_client.get_bucket_location(Bucket=self.elwood_bucket_name)['LocationConstraint']
            files = []
            for item in self.s3.Bucket(self.elwood_bucket_name).objects.all():
                item_obj = item.Object()
                files.append({
                    "name": item.key,
                    "url": "https://%s.s3%s.amazonaws.com/%s" % (self.elwood_bucket_name, f"-{location}" if location is not None else "", item.key),
                    "content_length": item_obj.content_length,
                    "content_type": item_obj.content_type,
                    "timestamp": int(item_obj.last_modified.timestamp())
                })
            return None, files
        except Exception as e:
            return str(e), None

    def upload_elwood_base64(self, name, content_type, base64_str):
        try:
            obj = self.s3.Object(self.elwood_bucket_name, name)
            obj.put(
                ACL="bucket-owner-full-control",
                Body=base64.b64decode(base64_str, validate=True),
                ContentType=content_type
            )
            # obj.Acl().put(ACL="public-read")
            location = self.s3_client.get_bucket_location(Bucket=self.invoice_bucket_name)['LocationConstraint']
            file_url = "https://%s.s3.%s.amazonaws.com/%s" % (self.invoice_bucket_name, f"{location}" if location is not None else "", name)
            return None, {
                "name": name,
                "url": file_url,
                # "content_length": size,
                "timestamp": int(time.time())
            }
        except Exception as e:
            return str(e), None

    def delete_file(self, name):
        try:
            self.s3.Bucket(self.elwood_bucket_name).delete_objects(Delete={
                "Objects": [
                    {
                        "Key": name
                    }
                ]
            })
            return None, {
                "deleted": name
            }
        except Exception as e:
            return str(e), None

    def get_elwood_csv_bytes(self, name):
        try:
            obj = self.s3.Object(self.elwood_bucket_name, name)
            return None, obj.get()['Body'].read()
        except Exception as e:
            return str(e), None
