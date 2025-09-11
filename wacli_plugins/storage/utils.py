from io import DEFAULT_BUFFER_SIZE, RawIOBase
from loguru import logger
from io import DEFAULT_BUFFER_SIZE, TextIOBase, TextIOWrapper

class BinaryIOWrapper(RawIOBase):
    """Implement a wrapper to convert a TextIO into a BinaryIO.
    It is the counterpart to io.TextIOWrapper that does not exist."""

    def __init__(self, file, encoding="utf-8", errors="strict"):
        self.file, self.encoding, self.errors = file, encoding, errors
        self.buf = b""

    def readinto(self, buf):
        if not self.buf:
            self.buf = self.file.read(DEFAULT_BUFFER_SIZE).encode(
                self.encoding, self.errors
            )
            if not self.buf:
                return 0
        length = min(len(buf), len(self.buf))
        buf[:length] = self.buf[:length]
        self.buf = self.buf[length:]
        return length

    def readable(self):
        return True



class TextToAnyStreamReaderWriter:
    """A reader and writer that can copy from a TextIO to a TextIO or BinaryIO."""
    def __init__(self, source_io, target_io):
        self.source_io, self.target_io = source_io, target_io

    def copy(self):
        self.source_io.wacli_read = self.source_io.read
        while chunk := self.source_io.wacli_read(DEFAULT_BUFFER_SIZE):
            try:
                self.target_io.write(chunk)
            except TypeError:
                logger.debug("overwrite source_io.wacli_read")
                self.target_io.write(chunk.encode("utf-8"))
                self.source_io.wacli_read = lambda buffer_size: self.source_io.read(
                    buffer_size
                ).encode("utf-8")


class UniversalStreamReaderWriter(TextToAnyStreamReaderWriter):
    """A reader and writer that can copy from """
    def __init__(self, source_io, target_io):
        if isinstance(source_io, TextIOBase):
            mode = "w"
        else:
            mode = getattr(source_io, "mode", "wb")

        if "b" not in mode:
            self.source_io = BinaryIOWrapper(source_io)
        else:
            self.source_io = source_io
        self.target_io = target_io
