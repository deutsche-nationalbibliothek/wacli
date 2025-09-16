"""This is the S3 storage module."""

from io import TextIOBase
from os import listdir, walk
from os.path import isdir
from pathlib import Path

from boto3.session import Session
from botocore.exceptions import NoCredentialsError
from loguru import logger

from wacli.plugin_manager import ConfigurationError
from wacli_plugins.catalog.graph import RDF, RDFS, WASE

from .directory import DirectoryStorage
from .utils import BinaryIOWrapper


class S3Storage(DirectoryStorage):
    """This is the S3 storage plugin based on boto3."""

    def configure(self, configuration):
        self.path = Path("")
        """Set an empty string path as base path, since it is used as path prefix
        by the DirectoryStorage base class"""

        bucket_name = configuration.get("bucket_name")
        if bucket_name is None or bucket_name == "":
            raise ConfigurationError(
                "A bucket_name needs to be set to a not empty value for an s3 storage."
            )
        self.bucket_name = bucket_name
        self.catalog = configuration.get("catalog", None)

        credentials = configuration.get("credentials", None)
        endpoint_url = configuration.get("endpoint_url", None)

        # Create a boto3 session
        session_config = {}
        if credentials:
            session_config["aws_access_key_id"] = credentials.get("access_key")
            session_config["aws_secret_access_key"] = credentials.get("secret_key")
        session = Session(**session_config)

        resource_config = {}
        if endpoint_url:
            resource_config["endpoint_url"] = endpoint_url

        self.s3 = session.resource("s3", **resource_config)

    def _store_data(self, path: Path, data, metadata, callback=None):
        callbacks = []
        logger.debug(f"source metadata: {metadata}")
        if source_callback := metadata.get("callback", False):
            logger.debug("register source_callback")
            callbacks.append(source_callback)

        def combined_callback(advance):
            for callback in callbacks:
                callback(
                    advance=advance,
                    total=metadata["size"],
                    name=path,
                )

        with data() as source_io_in:
            if isinstance(source_io_in, TextIOBase):
                mode = "w"
            else:
                mode = getattr(source_io_in, "mode", "wb")

            if "b" not in mode:
                source_io = BinaryIOWrapper(source_io_in)
            else:
                source_io = source_io_in

            try:
                # Upload the file to MinIO
                bucket = self.s3.Bucket(self.bucket_name)
                logger.debug(f"Bucket: {bucket}")
                if not bucket.creation_date:
                    logger.info(f"Bucket: {bucket} created")
                    bucket.create()
                bucket.upload_fileobj(source_io, str(path), Callback=combined_callback)
            except NoCredentialsError:
                logger.error("Error: Invalid credentials.")
            except UnicodeEncodeError as e:
                logger.debug(f"Report Exception for id={path}: {e}")
                if self.catalog:
                    # TODO get from path to id/IRI again
                    self.catalog.report(
                        path,
                        [
                            (RDF.type, WASE.Report),
                            (RDF.type, WASE["Exception"]),
                            (RDF.type, WASE[f"Exception-{type(e).__name__}"]),
                            (RDFS.comment, f"{e}"),
                        ],
                    )

    def _retrieve(self, path, mode, callback):
        if "w" in mode:
            path.parent.mkdir(parents=True, exist_ok=True)
        return lambda: open(path, mode), {"callback": callback}

    def list(self) -> list:
        return [d for d in listdir(self.path) if isdir(self.path / d)]

    def list_files(self, filter_fn=None) -> list:
        for subdir, dirs, files in walk(self.path):
            subdir_p = Path(subdir)
            files = filter(filter_fn, files) if filter_fn else files
            for file in files:
                yield subdir_p / file


export = S3Storage
