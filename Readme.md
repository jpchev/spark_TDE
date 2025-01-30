# Transparent Data Encryption for Spark

first, create a ```setenv``` file with the following content

```shell
export S3_ENDPOINT=<your s3 endpoint>
export S3_ACCESS_KEY=<your s3 access key>
export S3_SECRET_KEY=<your s3 secret key>
export S3_BUCKET_NAME=<your s3 bucket name>
```

then create a python virtual environment
```bash
python3 -m venv venv
```

now, source the virtual environment and install the pip components

```bash
source pyspark_env/bin/activate
pip install -r requirements.txt
```

compile the java code
```bash
mvn clean install
```

now, source the environment variables and launch the write and read spark jobs

```bash
source setenv
./write.sh
./read.sh
```

see https://www.baeldung.com/java-aes-encryption-decryption

- implement GCM 
- implement snappy compatibility
