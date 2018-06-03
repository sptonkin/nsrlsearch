"""
Module supporting NSRL RDS ingest.
"""
from __future__ import absolute_import, print_function
import time
import os
import zipfile
import sys
import tempfile
from io import open, TextIOWrapper
import isoparser
import six
import chardet


# Use the backports of CSV if using Python 2.
if six.PY2:
    import backports.csv as csv
else:
    import csv


class InvalidNsrlRdsContent(Exception):
    """
    Exception class for when the NSRL RDS CSV being ingested is not in the
    the expected format.
    """
    pass


def _zipped_file_readlines(zf, filename, skip_first=False):
    """
    Iterator for reading lines from a zip compressed files.
    """
    fh = zf.open(filename)
    first = True
    try:
        line = fh.readline()
        while line:
            if skip_first and first:
                first = False
                line = fh.readline()
                continue
            try:
                # ZipFile opens files in bytes mode, handle it.
                yield detect_and_decode(line)
            except UnicodeDecodeError:
                print(line)
                print([hex(ord(c)) for c in line])
                raise
            line = fh.readline()
    finally:
        fh.close()


def case_insensitive_file_match(wanted_files, files):
    """
    Retruns a dictionary which looks for the wanted_files in the list of
    files in a case-insensitive manner. Returns a dictionary which maps the
    entry in the wanted file to the case-insensitive match from the list
    of files.

    :param list wanted_files: list of wanted file names to find in file list
    :param list files: list of file names
    :returns: dict with keys are wanted filenames, values actual filenames
    :rtype: dict
    """
    file_mappings = {}
    for wf in wanted_files:
        for f in files:
            if six.PY3 and isinstance(f, six.binary_type):
                # In Python 3 isoparser will return bytes not strings.
                if wf.lower() == f.decode("latin-1").lower():
                    file_mappings[wf] = f
            else:
                # Otherwise, compare assuming all good.
                if wf.lower() == f.lower():
                    file_mappings[wf] = f
    return file_mappings


def detect_and_decode(line):
    """
    Attempts to employ exhaustive checks against encoding of string
    fields. This is specifically for the legacy set and the filename fields
    of the NSRL data.

    :param str line: string, probably line from csv file of unknown encoding
    :returns: unicode string
    :rtype: unicode or str in Python 2 or 3 respectively
    """
    try:
        # Try straight utf-8 decode.
        return line.decode("utf-8")
    except UnicodeEncodeError:
        # Already unicode.
        return line
    except UnicodeDecodeError:
        # Could not decode.
        # Try to detect encoding. If that fails, replace problem characters.
        try:
            utf8_line = line.decode(chardet.detect(line)["encoding"])
            return utf8_line
        except UnicodeDecodeError:
            return line.decode("utf-8", errors="replace")


def binfile_utf8_readlines(fh):
    """
    Iterator which yields decoded unicode strings for each line in the passed
    file.
    """
    line = fh.readline()
    while line:
        yield detect_and_decode(line)
        line = fh.readline()


def iso_utf8_readlines(ir):
    """
    Iterator which yields decoded unicode strings for each line in the passed
    iso record.
    """
    for line in ir.content.splitlines():
        yield detect_and_decode(line)


class NsrlIngestor(object):
    """
    Object which ingests NIST NSRL RDS CSV files. Can work with any objects
    which implement the same ``put_*`` methods as
    `:py:class:client.EsClient` and `:py:class:client.RestClient`.
    """

    DIR_EXPECTED_FILES = ["NSRLMfg.txt", "NSRLOs.txt", "NSRLProd.txt",
                          "NSRLFile.txt.zip"]

    ISO_EXPECTED_FILES = ["NSRLMFG.TXT", "NSRLOS.TXT", "NSRLPROD.TXT",
                          "NSRLFILE.ZIP"]

    def __init__(self, client, verbose=True):
        self.client = client
        self._verbose = verbose

    def print(self, s, *args, **kwargs):
        """
        Will print the string (same API as built-in :py:func:`print` if
        the ingestor object was created with the verbose keyword argument
        set to True. Also forces flushing of stdout after printing.
        """
        if self._verbose:
            res = print(s, *args, **kwargs)
            sys.stdout.flush()
            return res

    def _get_nsrl_ingest_filenames(self, files, expected_files):
        fmap = case_insensitive_file_match(expected_files, files)
        for ef in expected_files:
            if ef not in fmap:
                raise InvalidNsrlRdsContent("%s not found in %s" % (ef, files))

        return fmap

    def _get_dir_ingest_filenames(self, files):
        return self._get_nsrl_ingest_filenames(files, self.DIR_EXPECTED_FILES)

    def _get_iso_ingest_filenames(self, files):
        return self._get_nsrl_ingest_filenames(files, self.ISO_EXPECTED_FILES)

    def ingest_from_directory(self, path):
        """
        Ingests NSRL CSV files from the passed directory path which have
        been extracted from extracted from a NSRL ISO image.
        This includes reading from the compressed csv of the NSRLFile.txt.zip
        file.

        :param str path: path to directory containing files to ingest
        """
        filenames = os.listdir(path)
        fmap = self._get_dir_ingest_filenames(filenames)

        # Ingest mfg, os and prod (in that order).
        for label, key, meth in [
                    ("mfg", "NSRLMfg.txt", "put_manufacturers"),
                    ("os", "NSRLOs.txt", "put_oss"),
                    ("prod", "NSRLProd.txt", "put_products")]:
            self.print("Inserting %s info..." % label, end=" ")
            s = time.time()
            with open(os.path.join(path, fmap[key]), "rb") as fh:
                reader = csv.reader(binfile_utf8_readlines(fh))
                res = getattr(self.client, meth)(reader)
            e = time.time()
            self.print("done! Put %d in %fs" % (len(res), e - s))

        # Finally, ingest file information.
        self.print("Inserting file info...")
        s = time.time()
        with zipfile.ZipFile(
                os.path.join(path, fmap["NSRLFile.txt.zip"])) as zf:
            reader = csv.reader(
                _zipped_file_readlines(zf, "NSRLFile.txt", skip_first=True))
            count = self.client.put_files(reader, verbose=self._verbose)
        e = time.time()
        self.print("File ingest done! Put %d in %fs" % (count, e - s))

    def ingest_from_iso(self, path):
        """
        Ingests NSRL CSV directly from NSRL ISO image.
        file.

        :param str path: path to NSRL ISO image
        """
        with isoparser.parse(path) as iso:
            # Check that required filenames are there.
            filenames = [c.name for c in iso.root.children]
            fmap = self._get_iso_ingest_filenames(filenames)

            # Ingest mfg, os and prod (in that order).
            for label, key, meth in [
                        ("mfg", "NSRLMFG.TXT", "put_manufacturers"),
                        ("os", "NSRLOS.TXT", "put_oss"),
                        ("prod", "NSRLPROD.TXT", "put_products")]:
                self.print("Inserting %s info..." % label, end=" ")
                s = time.time()
                record = \
                    [r for r in iso.root.children if r.name == fmap[key]][0]
                reader = csv.reader(iso_utf8_readlines(record))
                res = getattr(self.client, meth)(reader)
                e = time.time()
                self.print("done! Put %d in %fs" % (len(res), e - s))

            # Copy NSRLFILE.ZIP to tmp (FileStream from isoparser has no seek)
            self.print("Creating temporary copy of file info...")
            key = "NSRLFILE.ZIP"
            file_record = \
                [r for r in iso.root.children if r.name == fmap[key]][0]
            file_stream = file_record.get_stream()
            temp_fd, temp_fp = tempfile.mkstemp(suffix="NSRLFILE.ZIP")
            os.close(temp_fd)
            try:
                with open(temp_fp, "wb") as temp_nsrlfile:
                    while file_stream.cur_offset < file_record.length:
                        temp_nsrlfile.write(file_stream.read(1048576))
                        mb_count = file_stream.cur_offset / 1048576
                        self.print("    copied %dMb" % mb_count)

                self.print("File copy done!")

                # Finally, ingest file information.
                self.print("Inserting file info...")
                s = time.time()
                with zipfile.ZipFile(temp_fp) as zf:
                    reader = \
                        csv.reader(_zipped_file_readlines(zf, "NSRLFile.txt",
                                                          skip_first=True))
                    count = \
                        self.client.put_files(reader, verbose=self._verbose)
                e = time.time()
                self.print("File ingest done! Put %d in %fs" % (count, e - s))

            finally:
                # Alyways clean up temporary copy.
                try:
                    os.unlink(temp_fp)
                except OSError:
                    pass
