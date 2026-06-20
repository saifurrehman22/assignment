"""LocalStack/S3 + Firehose helpers (boto3 imported lazily)."""
from django.conf import settings


def _session_kwargs() -> dict:
    aws = settings.AWS
    return dict(
        endpoint_url=aws["endpoint_url"],
        aws_access_key_id=aws["access_key"],
        aws_secret_access_key=aws["secret_key"],
        region_name=aws["region"],
    )


def s3_client():
    import boto3
    return boto3.client("s3", **_session_kwargs())


def firehose_client():
    import boto3
    return boto3.client("firehose", **_session_kwargs())


def ensure_bucket() -> str:
    """Create the bronze bucket if it does not yet exist. Returns the bucket name."""
    bucket = settings.AWS["bronze_bucket"]
    s3 = s3_client()
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
    return bucket


def ensure_firehose_stream() -> str:
    """
    Create a Firehose delivery stream that lands records in the bronze bucket.

    NOTE: in this project the extractor writes Parquet *directly* to S3 (the format
    the brief asks for). Firehose's native Parquet conversion needs Glue, which is a
    LocalStack-Pro feature, so we provision the stream as the conceptual delivery
    mechanism and keep the runnable path as direct S3 Parquet delivery. The stream is
    still usable for raw JSON delivery via ``put_record``.
    """
    stream = settings.AWS["firehose_stream"]
    bucket = settings.AWS["bronze_bucket"]
    fh = firehose_client()
    try:
        names = fh.list_delivery_streams().get("DeliveryStreamNames", [])
    except Exception:
        names = []
    if stream in names:
        return stream
    try:
        fh.create_delivery_stream(
            DeliveryStreamName=stream,
            DeliveryStreamType="DirectPut",
            S3DestinationConfiguration={
                "RoleARN": "arn:aws:iam::000000000000:role/firehose-role",
                "BucketARN": f"arn:aws:s3:::{bucket}",
                "Prefix": "firehose/",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 60},
            },
        )
    except Exception:
        # Firehose may be unavailable on community LocalStack; non-fatal because the
        # direct-to-S3 Parquet path is what the pipeline actually uses.
        pass
    return stream
