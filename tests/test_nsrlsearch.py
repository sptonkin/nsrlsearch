"""
There's a few things that could be done to tidy this up here. Here they are:

    * test all ingest methods against all client classes dynamically.
    * perhaps, at least, split out a base test class rather than overriding?
"""

from __future__ import absolute_import, print_function

import os
import unittest
import time
import pprint
import subprocess

import webtest.http

from nsrlsearch.client import EsClient, HttpClient
from nsrlsearch.ingest import NsrlIngestor
import nsrlsearch.config
import nsrlsearch.server


TEST_DATA_BASE = os.path.join(os.path.dirname(__file__), "data")
TEST_DATA_DIRECTORY = os.path.join(TEST_DATA_BASE, "directory_ingest")
TEST_ISO_PATH = os.path.join(TEST_DATA_BASE, "iso_ingest", "test_set.iso")


class TestNsrlSearch(unittest.TestCase):
    """
    Tests data ingest and query methods on client class.
    """

    TEST_INGESTOR_CLASS = NsrlIngestor

    TEST_INGESTOR_METHOD_NAME = "ingest_from_directory"

    TEST_INGESTOR_METHOD_KWARGS = {"path": TEST_DATA_DIRECTORY}

    TEST_CLIENT_CLASS = EsClient

    TEST_CLIENT_KWARGS = {
        "eskwargs": {"hosts": ["localhost:9200"]},
        "connection_check": True,
        "index_base": "test_nsrl",
        "create_indices": False,
    }

    def setUp(self):
        # Create client and ensure clean setup of es.
        self.client = self.TEST_CLIENT_CLASS(**self.TEST_CLIENT_KWARGS)
        self.client.create_indices(recreate=True)
        self.assertTrue(self.client.indices_exist, "indices do not exist")

        # Create ingestor class.
        self.ingestor = self.TEST_INGESTOR_CLASS(self.client, verbose=False)

    def tearDown(self):
        if self.client.indices_exist:
            self.client.delete_indices()
        self.assertFalse(self.client.indices_exist)

    def ingest(self):
        method = getattr(self.ingestor, self.TEST_INGESTOR_METHOD_NAME)
        method(**self.TEST_INGESTOR_METHOD_KWARGS)
        time.sleep(2)

    def assertIngest(self):
        # Assert mfg info.
        res = self.client.get_manufacturer(1)
        self.assertEqual(res["code"], "1")
        self.assertEqual(res["name"], "Nsrl Manufacturer")
        self.assertIsNotNone(self.client.get_manufacturer(2))
        self.assertIsNotNone(self.client.get_manufacturer(3))
        self.assertIsNotNone(self.client.get_manufacturer(4))
        self.assertIsNone(self.client.get_manufacturer(54321))

        # Assert os info.
        res = self.client.get_os(1)
        self.assertEqual(res["code"], "1")
        self.assertEqual(res["name"], "NsrlOS 1.0")
        self.assertEqual(res["version"], "1.0")
        self.assertEqual(res["mfg_code"], "1")
        self.assertIsNotNone(self.client.get_os(2))
        self.assertIsNotNone(self.client.get_os(3))
        self.assertIsNotNone(self.client.get_os(4))
        self.assertIsNone(self.client.get_os(54321))

        # Assert product info.
        res = self.client.get_product(1)
        self.assertEqual(res["code"], "1")
        self.assertEqual(res["name"], "Nsrl Product 1")
        self.assertEqual(res["version"], "1.0")
        self.assertEqual(res["os_code"], "1")
        self.assertEqual(res["mfg_code"], "1")
        self.assertEqual(res["language"], "Unknown")
        self.assertEqual(res["application_type"], "Business")
        self.assertIsNotNone(self.client.get_product(2))
        res = self.client.get_product(3)
        self.assertEqual(res["code"], "3")
        self.assertEqual(res["name"], "Not Nsrl Product")
        self.assertEqual(res["version"], "1.1.1")
        self.assertEqual(res["os_code"], "1")
        self.assertEqual(res["mfg_code"], "2")
        self.assertEqual(res["language"], "Unknown")
        self.assertEqual(res["application_type"], "Business")
        self.assertIsNone(self.client.get_product(54321))

        # Assert file info.
        self.assertFalse(self.client.get_digest_exists("Z" * 40))
        self.assertFalse(self.client.get_digest_exists("Z" * 32))
        self.assertFalse(self.client.get_digest_exists("Z" * 8))
        with self.assertRaises(ValueError):
            self.client.get_digest_exists("Z")
        # NOTE: to match test data, use letter strings -> affects scalars!
        for dg in ["AA", "BB", "CC", "DD", "EE", "FF", "11", "22", "12"]:
            for digest_scalar in [20, 16, 4]:
                digest = dg * digest_scalar
                self.assertTrue(self.client.get_digest_exists(digest),
                                "digest %s did not exist" % digest)

        # Pick a specific file and check some details.
        res = self.client.get_digest("A" * 40)
        self.assertNotIn("filename", res)
        self.assertNotIn("prod_code", res)
        res = self.client.get_digest("A" * 40, include_filename=True)
        self.assertEqual(res["sha1"], "a" * 40)
        self.assertEqual(res["md5"], "a" * 32)
        self.assertEqual(res["crc32"], "a" * 8)
        self.assertEqual(res["filename"], "fileA")
        res = self.client.get_digest("12121212", include_filename=True)
        self.assertEqual(res["sha1"], "12" * 20)
        self.assertEqual(res["md5"], "12" * 16)
        self.assertEqual(res["crc32"], "12" * 4)
        self.assertEqual(res["filename"], "file2")

        # Check file-to-product details.
        res = {e["code"]:e for e in self.client.get_digest_products("a" * 40)}
        self.assertEqual(len(res), 2)
        self.assertIn("1", res.keys(), msg="could not find digest in prod 1")
        self.assertIn("2", res.keys(), msg="could not find digest in prod 2")

        # Check file-to-product details with limit.
        res = self.client.get_digest_products("a" * 40, limit=1)
        self.assertEqual(len(res), 1)

        # Check product-to-file details.
        res = self.client.get_product_files(1)
        self.assertEqual(res["code"], "1")
        self.assertEqual(len(res["files"]), 6)
        digests = [f["md5"] for f in res["files"]]
        for letter in ["a", "b", "c", "d", "e", "f"]:
            self.assertIn(letter * 32, digests)

    def test_bulk_ingest(self):
        self.ingest()
        self.assertIngest()

    def server_refresh(self):
        self.client.es.indices.refresh(
            ",".join(self.client._index_names.values()))

    def test_single_ingest(self):
        # Assert mfg info.
        self.assertIsNone(self.client.get_manufacturer(100))
        self.client.put_manufacturer(100, "New Mfg Co")
        self.server_refresh()
        res = self.client.get_manufacturer(100)
        self.assertIsNotNone(res)
        self.assertEqual(res["code"], "100")
        self.assertEqual(res["name"], "New Mfg Co")

        # Assert os info.
        self.assertIsNone(self.client.get_os("200"))
        self.client.put_os("200", "SuperDuperOs", "99.99", "100")
        self.server_refresh()
        res = self.client.get_os(200)
        self.assertEqual(res["code"], "200")
        self.assertEqual(res["name"], "SuperDuperOs")
        self.assertEqual(res["version"], "99.99")
        self.assertEqual(res["mfg_code"], "100")

        # Assert product info.
        self.assertIsNone(self.client.get_product("300"))
        self.client.put_product("300", "Thermopylae", "0.480",
                                "200", "100", "Greek", "Battle")
        self.server_refresh()
        res = self.client.get_product("300")
        self.assertEqual(res["code"], "300")
        self.assertEqual(res["name"], "Thermopylae")
        self.assertEqual(res["version"], "0.480")
        self.assertEqual(res["os_code"], "200")
        self.assertEqual(res["mfg_code"], "100")
        self.assertEqual(res["language"], "Greek")
        self.assertEqual(res["application_type"], "Battle")
        # Test resolution of names.
        self.assertEqual(res["os_name"], "SuperDuperOs")
        self.assertEqual(res["mfg_name"], "New Mfg Co")

        # Assert file info.
        self.assertFalse(self.client.get_digest_exists("a" * 32))
        self.client.put_product_file("300", "a" * 40, "a" * 32, "a" * 8,
            "history text", "1024", "200")
        self.server_refresh()
        res = self.client.get_digest("a" * 32, include_filename=True)
        self.assertEqual(res["sha1"], "a" * 40)
        self.assertEqual(res["md5"], "a" * 32)
        self.assertEqual(res["crc32"], "a" * 8)
        self.assertEqual(res["filename"], "history text")
        self.assertEqual(
            len(self.client.get_product_files("300")["files"]), 1)
        self.assertEqual(len(self.client.get_digest_products("a" * 40)), 1)

        # Check doc count.
        res = self.client.get_counts()
        for k, v in res.items():
            self.assertGreater(v, 0, msg="<1 doc in %s index" % k)


class TestNsrlSearchIsoPath(TestNsrlSearch):
    """
    Tests data ingest and query methods on client class.
    """

    TEST_INGESTOR_CLASS = NsrlIngestor

    TEST_INGESTOR_METHOD_NAME = "ingest_from_iso"

    TEST_INGESTOR_METHOD_KWARGS = {"path": TEST_ISO_PATH}


class TestNsrlSearchHttpInterface(TestNsrlSearch):
    """
    Tests data ingest and query methods on client class.
    """

    TEST_INGESTOR_CLASS = NsrlIngestor

    TEST_INGESTOR_METHOD_NAME = "ingest_from_directory"

    TEST_INGESTOR_METHOD_KWARGS = {"path": TEST_DATA_DIRECTORY}

    TEST_CLIENT_CLASS = HttpClient

    TEST_CLIENT_KWARGS = {
        "uri": "http://localhost:11541",
    }

    def setUp(self):
        # Force HttpServer's EsClient's re-configuration.
        nsrlsearch.server._CLIENT = None
        nsrlsearch.server._get_client(TestNsrlSearch.TEST_CLIENT_KWARGS)

        # Set CONFIG with defaults (which should pass tests).
        nsrlsearch.server.CONFIG = nsrlsearch.config.DEFAULT_CONFIG
        
        # Create rest server.
        self.server = webtest.http.StopableWSGIServer.create(
                nsrlsearch.server.HttpServer,
                host="localhost", port=11541)
        self.server.wait()

        # Call super's implementation.
        super(TestNsrlSearchHttpInterface, self).setUp()

    def tearDown(self):
        super(TestNsrlSearchHttpInterface, self).tearDown()
        self.server.shutdown()
        time.sleep(1)
        nsrlsearch.server.HttpServer._CLIENT = None

    def server_refresh(self):
        time.sleep(1)


class TestNsrlSearchIsoPathHttpInterface(TestNsrlSearchHttpInterface):
    """
    Tests data ingest and query methods on client class.
    """

    TEST_INGESTOR_CLASS = NsrlIngestor

    TEST_INGESTOR_METHOD_NAME = "ingest_from_iso"

    TEST_INGESTOR_METHOD_KWARGS = {"path": TEST_ISO_PATH}
