from io import DEFAULT_BUFFER_SIZE, RawIOBase


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
