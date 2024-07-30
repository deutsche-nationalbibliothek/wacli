from httpx import Response
from loguru import logger
from textwrap import dedent
import pytest

from wacli.plugin_manager import PluginManager

aras_rest_base_url = "http://dnb-test-aras/"
aras_repository = "example_warc"


def get_plugin_config(path):
    return {
        "test_storage": [
            {
                "module": "wacli_plugins.storage.aras",
                "rest_base": aras_rest_base_url,
                "repo": aras_repository
            }
        ]
    }

@pytest.mark.respx(base_url=aras_rest_base_url)
def test_retrieve_stream(respx_mock, tmp_path):
    idn = "1234567890"

    example_content_0 = "One example WARC content"
    example_content_1 = "Another example WARC content"

    mets_file = """
    <file ID="{id}" MIMETYPE="{mime_type}" CREATED="2018-10-01T07:15:08" SIZE="{size}">
        <FLocat LOCTYPE="URL" xlink:href="{href}"/>
    </file>
    """

    mock_files = [
        mets_file.format(
            id=0,
            mime_type="text/plain",
            size=len(example_content_0.encode("utf8")),
            href="example_0.txt",
        ),
        mets_file.format(
            id=1,
            mime_type="text/plain",
            size=len(example_content_1.encode("utf8")),
            href="example_1.txt",
        ),
    ]

    # /access/repositories/example_warc/artifacts/1234567890/objects
    respx_mock.get(f"/access/repositories/{aras_repository}/artifacts/{idn}/objects").mock(
        return_value=Response(
            200,
            text=dedent(f"""
        <?xml version='1.0' encoding='UTF-8'?>
        <mets xmlns="http://www.loc.gov/METS/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xsi:schemaLocation="http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd">
            <metsHdr CREATEDATE="2024-07-29T12:32:44" RECORDSTATUS="draft">
                <agent ROLE="CREATOR" TYPE="ORGANIZATION">
                    <name>Deutsche Nationalbibliothek</name>
                </agent>
            </metsHdr>
            <fileSec>
                <fileGrp>
                    {"".join({file for file in mock_files})}
                </fileGrp>
            </fileSec>
            <structMap>
                <div/>
            </structMap>
        </mets>
        """).strip(),
        )
    )

    respx_mock.get(f"/access/repositories/{aras_repository}/artifacts/{idn}/objects/0").mock(
        return_value=Response(200, text=example_content_0)
    )
    respx_mock.get(f"/access/repositories/{aras_repository}/artifacts/{idn}/objects/1").mock(
        return_value=Response(200, text=example_content_1)
    )

    plugin_manager = PluginManager()
    plugin_manager.register_plugins(get_plugin_config(tmp_path))

    test_storage = plugin_manager.get("test_storage")
    archive_stream = test_storage.retrieve_stream([1234567890])
    for idn, container, _ in archive_stream:
        name, stream, metadata = next(container)
        with stream() as bytes_io:
            bytes_io.read() == example_content_0.encode("utf-8")
        name, stream, metadata = next(container)
        with stream() as bytes_io:
            bytes_io.read() == example_content_1.encode("utf-8")
