import json
import logging
import os
import typer
from dotenv import load_dotenv

from src.awss3 import (
    init_client,
    list_buckets,
    create_bucket,
    delete_bucket,
    bucket_exists,
    download_file_and_upload_to_s3,
    set_object_access_policy,
    generate_public_read_policy,
    create_bucket_policy,
    read_bucket_policy
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = typer.Typer(help="S3 Manager CLI tool for working with S3-compatible storage")
s3_client = None

def get_client():
    global s3_client
    if s3_client is None:
        s3_client = init_client(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION_NAME', 'us-east-1'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL'),
            aws_session_token=os.getenv('AWS_SESSION_TOKEN')
        )
    return s3_client

@app.callback()
def callback():
    get_client()

@app.command()
def ls():
    buckets = list_buckets(get_client())
    if buckets:
        typer.echo("Available buckets:")
        for bucket in buckets:
            typer.echo(f"  - {bucket}")
    else:
        typer.echo("No buckets found")

@app.command()
def create(bucket_name, region=None):
    try:
        create_bucket(bucket_name, region, get_client())
        typer.echo(f"Bucket '{bucket_name}' created successfully")
    except Exception as e:
        typer.echo(f"Error creating bucket: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def delete(bucket_name, force=False):
    if not force:
        confirm = typer.confirm(f"Are you sure you want to delete bucket '{bucket_name}'?")
        if not confirm:
            typer.echo("Operation cancelled")
            return
    
    try:
        delete_bucket(bucket_name, get_client())
        typer.echo(f"Bucket '{bucket_name}' deleted successfully")
    except Exception as e:
        typer.echo(f"Error deleting bucket: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def exists(bucket_name):
    if bucket_exists(bucket_name, get_client()):
        typer.echo(f"Bucket '{bucket_name}' exists")
    else:
        typer.echo(f"Bucket '{bucket_name}' does not exist")
        raise typer.Exit(1)

@app.command()
def upload_from_url(url, bucket_name, object_key):
    try:
        download_file_and_upload_to_s3(url, bucket_name, object_key, get_client())
        typer.echo(f"File from {url} uploaded to s3://{bucket_name}/{object_key}")
    except Exception as e:
        typer.echo(f"Error uploading file: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def set_access(bucket_name, object_key, acl="public-read"):
    try:
        set_object_access_policy(bucket_name, object_key, acl, get_client())
        typer.echo(f"Access policy for s3://{bucket_name}/{object_key} set to {acl}")
    except Exception as e:
        typer.echo(f"Error setting access policy: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def make_public(bucket_name, prefix=""):
    try:
        policy = generate_public_read_policy(bucket_name, prefix)
        create_bucket_policy(bucket_name, policy, get_client())
        
        prefix_msg = f"with prefix '{prefix}'" if prefix else ""
        typer.echo(f"Public read policy applied to bucket '{bucket_name}'{prefix_msg}")
    except Exception as e:
        typer.echo(f"Error applying public policy: {e}", err=True)
        raise typer.Exit(1)

@app.command()
def get_policy(bucket_name):
    try:
        policy = read_bucket_policy(bucket_name, get_client())
        if policy:
            typer.echo(json.dumps(policy, indent=2))
        else:
            typer.echo(f"No policy found for bucket '{bucket_name}'")
    except Exception as e:
        typer.echo(f"Error reading bucket policy: {e}", err=True)
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
