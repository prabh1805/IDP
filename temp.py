import boto3, tempfile
s3 = boto3.client("s3")
with tempfile.NamedTemporaryFile() as f:
    s3.download_file("awsidpdocs", "sample1.pdf", f.name)
    print("Download OK, size =", f.tell())