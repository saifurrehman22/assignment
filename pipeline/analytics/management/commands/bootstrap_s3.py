"""Create the S3 bronze bucket and Firehose delivery stream in LocalStack."""
from django.core.management.base import BaseCommand

from analytics.pipeline.aws import ensure_bucket, ensure_firehose_stream


class Command(BaseCommand):
    help = "Provision the LocalStack S3 bucket + Firehose delivery stream."

    def handle(self, *args, **opts):
        bucket = ensure_bucket()
        self.stdout.write(self.style.SUCCESS(f"  S3 bucket ready: {bucket}"))
        stream = ensure_firehose_stream()
        self.stdout.write(self.style.SUCCESS(f"  Firehose stream ready: {stream}"))
