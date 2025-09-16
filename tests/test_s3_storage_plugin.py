from io import BytesIO, StringIO, TextIOWrapper

import boto3
import pytest
from loguru import logger
from moto import mock_aws

from wacli.plugin_manager import PluginManager

BUCKET_NAME = "test_example"


@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def s3_resource():
    with mock_aws():
        yield boto3.resource("s3")


def get_plugin_config():
    return {
        "test_storage": [
            {
                "module": "wacli_plugins.storage.s3",
                "bucket_name": BUCKET_NAME,
            }
        ]
    }


def test_store_text_io(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial text data"
    stream = StringIO(content)

    test_storage.store(example_file, lambda: stream)

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == content
    assert len(list(bucket.objects.all())) == 1


def test_store_binary_io_in_text_io_wrapper(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial binary text data"
    binary_content = content.encode("utf-8")
    stream = TextIOWrapper(BytesIO(binary_content))

    test_storage.store(example_file, lambda: stream)

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == content
    assert len(list(bucket.objects.all())) == 1


def test_store_binary_io(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial binary text data"
    binary_content = content.encode("utf-8")
    stream = BytesIO(binary_content)

    test_storage.store(example_file, lambda: stream)

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == content
    assert len(list(bucket.objects.all())) == 1


def test_get_stream_text_io(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "Hallo"

    with test_storage.retrieve(example_file, mode="w")[0]() as stream:
        stream.write(content)

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == content
    assert len(list(bucket.objects.all())) == 1


def test_get_stream_binary_io(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"

    content = "Hallo binary"
    binary_content = content.encode("utf-8")
    logger.debug(binary_content)

    with test_storage.retrieve(example_file, mode="wb")[0]() as stream:
        stream.write(binary_content)

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == content
    assert len(list(bucket.objects.all())) == 1


def test_retrieve_str(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_file = "example_file.txt"
    content = "some initial text data"

    bucket = s3_resource.Bucket(BUCKET_NAME)
    example_file_s3 = bucket.Object(example_file)
    example_file_s3.put(Body=content.encode("utf-8"), ContentType="text/plain")

    result_content, _ = test_storage.retrieve(example_file)

    with result_content() as result_data:
        assert result_data.read() == content


def test_store_stream(s3_resource):
    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config())

    test_storage = plugin_manager.get("test_storage")
    example_text_file = "example_text_file.txt"
    text_content = "some initial text data"
    text_stream = StringIO(text_content)

    example_binary_file = "example_binary_file.txt"
    binary_content = "some initial binary text data"
    binary_content_encoded = binary_content.encode("utf-8")
    binary_stream = BytesIO(binary_content_encoded)

    storage_stream = [
        (example_text_file, lambda: text_stream, {}),
        (example_binary_file, lambda: binary_stream, {}),
    ]

    test_storage.store_stream(storage_stream)

    bucket = s3_resource.Bucket(BUCKET_NAME)

    example_file_s3 = bucket.Object(example_text_file)
    assert example_file_s3.get()["Body"].read().decode("utf-8") == text_content

    example_file_s3 = bucket.Object(example_binary_file)
    assert example_file_s3.get()["Body"].read() == binary_content_encoded

    assert len(list(bucket.objects.all())) == 2
