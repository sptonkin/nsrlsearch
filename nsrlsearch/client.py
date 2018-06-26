"""
Module containing client classes.
"""
from __future__ import absolute_import, print_function
import pprint
import time
import sys
import six

from elasticsearch import Elasticsearch, helpers, NotFoundError
from elasticsearch.transport import Transport
import requests


def get_digest_type(digest):
    """
    Returns one of 'md5', 'sha1', 'crc32' for the specified digest.
    Check is purely performed on length of digest. No other checks are
    performed.
    Raises ValueError if cannot identify digest type.

    :param str digest: string of digest to check
    :returns: string of either 'md5', 'sha1', 'crc32'
    :rtype: str:
    """
    if len(digest) == 32:
        return "md5"
    elif len(digest) == 40:
        return "sha1"
    elif len(digest) == 8:
        return "crc32"
    else:
        raise ValueError("Unknown digest type with len %d" % len(digest))


class InvalidNsrlRdsContent(Exception):
    pass


class OperationalError(Exception):
    pass


class InvalidOperation(Exception):
    pass


class EsClient(object):
    """
    Client class for storing/querying the NSRL data set in an
    Elasticsearch instance.

    This class provides two ways of configuring the raw Elasticsearch
    client class it uses to communicate with Elasticsearch:

       * By passing :py:class:`elasticsearch.Elasticsearch` instance
         in as the ```es``` keyword argument, or
       * By passing a dict in with the keyword arguments used to instantiate
         a :py:class:`elasticsearch.Elasticsearch` instance.

    Indicies are named "<index_base>_<index_type>". The index_base
    defaults to "nsrl", although this is user configurable so that
    multiple sets of NsrlSearch data can exist in the same cluster. For
    example, a "test" instance could notionally be hosted in the same
    cluster as a "production" instance by setting the "test" instances'
    index_base to something like "test_nsrl". The index_type is used
    internally and cannot be changed.

    Note: Instances of this class assume that all input data to its
          methods (eg. the put_) methods are utf-8 encoded already.

    :param :py:class:`elasticsearch.Elasticsearch` es: ES client
    :param dict eskwargs: keyword arguments optionally used to create ES client
    :param bool connection_check: whether to test connectivity on
                                  instantiation (default: True)
    :param str index_base: prefix to use for name of Elasticsearch indices
    :param bool create_indices: whether to create indices on instantiation
                                (default: False)
    :param int shards: number of shards for index creation (default: 4)
    :param int replicas: number of replicas for index creation (default: 1)
    """

    MFG_INDEX_MAPPINGS = {
        "mfg": {
            "properties": {
                "code": {"type": "keyword"},
                "name": {"type": "text"},
            }
        }
    }

    OS_INDEX_MAPPINGS = {
        "os": {
            "properties": {
                "code": {"type": "keyword"},
                "name": {"type": "text"},
                "version": {"type": "keyword"},
                "mfg_code": {"type": "keyword"}
            }
        }
    }

    PRODFILE_INDEX_MAPPINGS = {
        "product": {
            "_all": {"enabled": False},
            "properties": {
                "code": {"type": "keyword"},
                "name": {"type": "keyword"},
                "version": {"type": "keyword"},
                "os_code": {"type": "keyword"},
                "os_name": {"type": "text"},  # take from os file
                "mfg_code": {"type": "keyword"},
                "mfg_name": {"type": "text"},  # take form mfg file
                "language": {"type": "keyword"},
                "application_type": {"type": "keyword"}
            },
        },
        "file": {
            "_parent": {"type": "product"},
            "_all": {"enabled": False},
            "properties": {
                "md5": {"type": "keyword"},
                "sha1": {"type": "keyword"},
                "crc32": {"type": "keyword"},
                "size": {"type": "long"},
                "filename": {"type": "keyword"},
                "os_code": {"type": "keyword"},
                "prod_code": {"type": "keyword"}
            }
        }
    }

    def __init__(self,
                 connection_check=True,
                 index_base="nsrl",
                 create_indices=False,
                 shards=4,
                 replicas=1,
                 es=None,
                 eskwargs=None):

        if es is None and eskwargs is None:
            raise ValueError("exactly one of es or eskwargs must be set")
        elif es is not None:
            self.es = es
        else:
            self.es = Elasticsearch(**eskwargs)

        if connection_check:
            if not self.es.ping():
                raise OperationalError("Could not connect to es hosts.")

        # Configure the names and mappings of the indices this will need.
        self._index_names = {
            "os": "%s_os" % index_base,
            "mfg": "%s_mfg" % index_base,
            "prodfile": "%s_prodfile" % index_base}
        self._index_mappings = {
            "os": self.OS_INDEX_MAPPINGS,
            "mfg": self.MFG_INDEX_MAPPINGS,
            "prodfile": self.PRODFILE_INDEX_MAPPINGS}

        # Create if told to do so and needed.
        if create_indices and not self.indices_exist:
            self.create_indices(shards, replicas)

    @property
    def indices_exist(self):
        """Return False if any indices do not exists. True otherwise."""
        for index_name, status in self.indices.items():
            if not status["exists"]:
                return False
        return True

    @property
    def indices(self):
        """Query the ES cluster for the status of the NSRL indices."""
        res = {}
        for index, index_name in self._index_names.items():
            res[index] = dict(name=index_name,
                              exists=self.es.indices.exists(index_name))

            # Continue if this index doesn't exist. Otherwise, query stats.
            if not res[index]["exists"]:
                continue

        return res

    def create_indices(self, shards=4, replicas=1, recreate=False):
        # Handle recreation logic.
        if self.indices_exist and not recreate:
            msg = "Indicies already exist. Cannot create. See recreate kwarg?"
            raise InvalidOperation(msg)
        elif self.indices_exist and recreate:
            self.delete_indices()

        # Create the indices.
        for index, index_name in self._index_names.items():
            # Create index with appropriate mappings.
            body = {
                "settings": {
                    "number_of_shards": shards,
                    "number_of_replicas": replicas
                },
                "mappings": self._index_mappings[index]
            }
            self.es.indices.create(index=index_name, body=body)

    def delete_indices(self):
        for index, index_name in self._index_names.items():
            self.es.indices.delete(index=index_name)

    def _format_results(self, results, raw):
        if raw:
            # Return raw results, including with ES metadata.
            return results
        else:
            # Need to strip out es metadata.
            if type(results) is dict:
                if "_source" in results:
                    return results["_source"]
                else:
                    raise KeyError("Expected key \"_source\" not in results"
                                   "%s" % results)
            elif type(results) is list:
                return [e["_source"] for e in results]

    def get_counts(self):
        res = \
            self.es.indices.stats(index=",".join(self._index_names.values()))
        return {index: res["indices"][name]["primaries"]["docs"]["count"]
                for index, name in self._index_names.items()}

    def put_manufacturer(self, code, name):
        doc = {
            "code": str(code),
            "name": name
        }
        self.es.index(index=self._index_names["mfg"], doc_type="mfg",
                      id=code, body=doc)

    def put_manufacturers(self, manufacturers, chunk_size=1000):
        """Warning: this will read all manufacturers in to memory if they
        aren't there already."""
        actions = []
        mfg = {}
        for code, name in manufacturers:
            mfg[code] = {"code": code, "name": name}
            action = {
                "_index": self._index_names["mfg"],
                "_type": "mfg",
                "_id": code,
                "_source": mfg[code]
            }
            actions.append(action)

        helpers.bulk(self.es, actions, chunk_size=chunk_size)

        return mfg

    def put_os(self, code, name, version, mfg_code):
        doc = {
            "code": str(code),
            "name": name,
            "version": version,
            "mfg_code": mfg_code
        }
        self.es.index(index=self._index_names["os"], doc_type="os",
                      id=code, body=doc)

    def put_oss(self, oss, chunk_size=1000):
        """Warning: this will read all os info in to memory if they
        aren't there already."""
        actions = []
        opsys = {}
        for code, name, ver, mfg_code in oss:
            opsys[code] = dict(code=code, name=name,
                               version=ver, mfg_code=mfg_code)
            action = {
                "_index": self._index_names["os"],
                "_type": "os",
                "_id": code,
                "_source": opsys[code]
            }
            actions.append(action)

        helpers.bulk(self.es, actions, chunk_size=chunk_size)

        return opsys

    def put_product(self, code, name, version, os_code, mfg_code,
                    language, application_type, os_name=None, mfg_name=None):

        if os_name is None:
            try:
                os_name = self.get_os(os_code, raw=False)["name"]
            except ValueError:
                raise InvalidNsrlRdsContent("Could not resolve name for "
                                            "os with code '%s'" % os_code)
        if mfg_name is None:
            try:
                mfg_name = self.get_manufacturer(mfg_code, raw=False)["name"]
            except ValueError:
                raise InvalidNsrlRdsContent("Could not resolve name for "
                                            "manufacturer with code "
                                            "'%s'" % mfg_code)

        doc = {
            "code": str(code),
            "name": name,
            "version": version,
            "os_code": os_code,
            "os_name": os_name,
            "mfg_code": mfg_code,
            "mfg_name": mfg_name,
            "language": language,
            "application_type": application_type
        }
        self.es.index(index=self._index_names["prodfile"], doc_type="product",
                      id=code, body=doc)

    def put_products(self, products, mfgs=None, oss=None, chunk_size=1000):
        actions = []
        prods = {}
        for code, name, ver, os_code, mfg_code, lang, apptype in products:

            # Lookup mfgs and os if available.
            if mfgs is not None:
                mfg_name = mfgs[mfg_code]["name"]
            else:
                try:
                    mfg_name = self.get_manufacturer(mfg_code)["name"]
                except TypeError:
                    raise ValueError("Manufacturer %s not in data store" %
                                     mfg_code)

            if oss is not None:
                os_name = oss[os_code]["name"]
            else:
                os_name = self.get_os(os_code)["name"]

            prods[code] = {
                "code": code,
                "name": name,
                "version": ver,
                "os_code": os_code,
                "os_name": os_name,
                "mfg_code": mfg_code,
                "mfg_name": mfg_name,
                "language": lang,
                "application_type": apptype
            }
            action = {
                "_index": self._index_names["prodfile"],
                "_type": "product",
                "_id": code,
                "_source": prods[code]
            }
            actions.append(action)

        helpers.bulk(self.es, actions, chunk_size=chunk_size)

        return prods

    def put_product_file(self, prod_code, sha1, md5, crc32, fn, size,
                         os_code):
        doc_id = "%s_%s" % (prod_code, sha1.lower())
        doc = {
            "parent": prod_code,
            "md5": md5.lower(),
            "sha1": sha1.lower(),
            "crc32": crc32.lower(),
            "size": int(size),
            "filename": fn,
            "os_code": os_code,
            "prod_code": prod_code
        }
        self.es.index(index=self._index_names["prodfile"], doc_type="file",
                      id=doc_id, body=doc, parent=prod_code)

    def put_files(self, files, chunk_size=1000, verbose=False):
        actions = []
        count = 0
        for sha1, md5, crc32, fn, size, prod_code, os_code, _ in files:
            if count % chunk_size == 0 and count != 0:
                helpers.bulk(self.es, actions, chunk_size=chunk_size)
                actions = []
                if verbose and count % 1000000 == 0:
                    print("    files inserted: %d" % count)

            doc_id = "%s_%s" % (prod_code, sha1.lower())
            doc = {
                "md5": md5.lower(),
                "sha1": sha1.lower(),
                "crc32": crc32.lower(),
                "filename": fn,
                "size": int(size),
                "prod_code": prod_code,
                "os_code": os_code
            }

            action = {
                "_index": self._index_names["prodfile"],
                "_type": "file",
                "_id": doc_id,
                "_parent": prod_code,
                "_source": doc
            }
            actions.append(action)
            count += 1

        if len(actions):
            helpers.bulk(self.es, actions, chunk_size=chunk_size)

        return count

    def get_digest(self, digest,
                   include_filename=False, include_prod_code=False,
                   raw=False):
        doc = {
            'query': {
                'term': {
                    get_digest_type(digest): digest.lower()
                }
            },
            'size': 1
        }
        res = self.es.search(index=self._index_names["prodfile"],
                             doc_type="file", body=doc)
        if res["hits"]["total"] == 0:
            return None
        else:
            if not include_filename:
                res["hits"]["hits"][0]["_source"].pop("filename")
            if not include_prod_code:
                res["hits"]["hits"][0]["_source"].pop("prod_code")
            return self._format_results(res["hits"]["hits"][0], raw)

    def get_digest_exists(self, digest):
        return self.get_digest(digest) is not None

    def get_digest_products(self, digest, limit=10000, raw=False):
        doc = {
            'query': {
                'has_child': {
                    'type': 'file',
                    'query': {
                        'term': {
                            get_digest_type(digest): digest.lower()
                        }
                    }
                }
            },
            'size': limit
        }
        res = self.es.search(index=self._index_names["prodfile"],
                             doc_type="product", body=doc)
        if res["hits"]["total"] == 0:
            return None
        else:
            return self._format_results(res["hits"]["hits"], raw)

    def get_os(self, code, raw=False):
        try:
            res = self.es.get(
                index=self._index_names["os"], id=code, doc_type="os")
            return self._format_results(res, raw)
        except NotFoundError:
            return None

    def get_manufacturer(self, code, raw=False):
        try:
            res = self.es.get(
                index=self._index_names["mfg"], id=code, doc_type="mfg")
            return self._format_results(res, raw)
        except NotFoundError:
            return None

    def get_product(self, code, raw=False):
        try:
            res = self.es.get(index=self._index_names["prodfile"],
                              id=code, doc_type="product")
            return self._format_results(res, raw)
        except NotFoundError:
            return None

    def get_product_files(self, code, limit=10000, raw=False):
        # See if the product exists.
        res = self.get_product(code, raw=True)
        if res is None:
            return res

        # Product exists - do query for files.
        doc = {
            "query": {
                "has_parent": {
                    "type": "product",
                    "query": {
                        "match": {
                            "code": code
                        }
                    }
                }
            },
            "size": limit
        }
        files = self.es.search(index=self._index_names["prodfile"],
                               doc_type="file", body=doc)["hits"]["hits"]

        # Format results.
        if raw:
            res["_source"]["files"] = files
        else:
            res = self._format_results(res, False)
            res["files"] = [self._format_results(f, False) for f in files]
        return res


class HttpClient(object):
    """
    Client for the HTTP server.

    Note: Instances of this class assume that all input data to its
          methods (eg. the put_) methods are utf-8 encoded already.

    :param str uri: URI of the nsrlsearch HttpServer to access
    """

    def __init__(self, uri="http://localhost:8080"):
        self.uri_base = uri

    @property
    def indices_exist(self):
        """Return False if any indices do not exists. True otherwise."""
        uri = "%s/status" % self.uri_base
        res = requests.get(uri)
        res.raise_for_status()
        js = res.json()
        if "indices" in js and \
                "prodfile" in js["indices"] and \
                "os" in js["indices"] and \
                "mfg" in js["indices"] and \
                js["indices"]["prodfile"]["exists"] and \
                js["indices"]["os"]["exists"] and \
                js["indices"]["mfg"]["exists"]:
            return True
        return False

    @property
    def indices(self):
        """Query the ES cluster for the status of the NSRL indices."""
        uri = "%s/status" % self.uri_base
        res = requests.get(uri)

    def create_indices(self, shards=4, replicas=1, recreate=False):
        params = dict(shards=shards, replicas=replicas, recreate=recreate)
        uri = "%s/indices" % self.uri_base
        res = requests.put(uri, params=params)
        res.raise_for_status()

    def delete_indices(self):
        uri = "%s/indices" % self.uri_base
        res = requests.delete(uri)
        res.raise_for_status()

    def get_counts(self):
        uri = "%s/counts" % self.uri_base
        res = requests.get(uri)
        return self.handle_response(res)

    def put_manufacturer(self, code, name):
        params = dict(name=name)
        uri = "%s/manufacturers/%s" % (self.uri_base, code)
        res = requests.put(uri, params=params)
        res.raise_for_status()

    def put_manufacturers(self, manufacturers, chunk_size=1000):
        uri = "%s/manufacturers" % self.uri_base
        data = {}
        mfg = {}
        for code, name in manufacturers:
            mfg[code] = {"code": code, "name": name}
            data[code] = mfg[code]
            if len(data) >= chunk_size:
                res = requests.post(uri, json=data)
                res.raise_for_status()
                data = {}
        if data:
            res = requests.post(uri, json=data)
            res.raise_for_status()
        return mfg

    def put_os(self, code, name, version, mfg_code):
        params = dict(name=name, version=version, mfg_code=mfg_code)
        uri = "%s/os/%s" % (self.uri_base, code)
        res = requests.put(uri, params=params)
        res.raise_for_status()

    def put_oss(self, oss, chunk_size=1000):
        uri = "%s/os" % self.uri_base
        data = {}
        opsys = {}
        for code, name, ver, mfg_code in oss:
            opsys[code] = dict(code=code, name=name,
                               version=ver, mfg_code=mfg_code)
            data[code] = opsys[code]
            if len(data) >= chunk_size:
                res = requests.post(uri, json=data)
                res.raise_for_status()
                data = {}
        if data:
            res = requests.post(uri, json=data)
            res.raise_for_status()
        return opsys

    def put_product(self, code, name, version, os_code, mfg_code,
                    language, application_type):
        params = dict(name=name, version=version, os_code=os_code,
                      mfg_code=mfg_code, language=language,
                      application_type=application_type)
        uri = "%s/products/%s" % (self.uri_base, code)
        res = requests.put(uri, params=params)
        res.raise_for_status()

    def put_products(self, products, mfgs=None, oss=None, chunk_size=1000):
        uri = "%s/products" % self.uri_base
        data = {}
        prods = {}
        for code, name, ver, os_code, mfg_code, lang, apptype in products:
            prods[code] = {
                "code": code,
                "name": name,
                "version": ver,
                "os_code": os_code,
                "mfg_code": mfg_code,
                "language": lang,
                "application_type": apptype
            }
            data[code] = prods[code]
            if len(data) >= chunk_size:
                res = requests.post(uri, json=data)
                res.raise_for_status()
                data = {}
        if data:
            res = requests.post(uri, json=data)
            res.raise_for_status()
        return prods

    def put_product_file(self, prod_code, sha1, md5, crc32, fn, size,
                         os_code):
        params = dict(prod_code=prod_code, sha1=sha1, md5=md5, crc32=crc32,
                      filename=fn, size=size, os_code=os_code)
        uri = "%s/products/%s/files" % (self.uri_base, prod_code)
        res = requests.put(uri, params=params)
        res.raise_for_status()

    def put_files(self, files, chunk_size=1000, verbose=False):
        uri = "%s/files" % self.uri_base
        data = {}
        count = 0
        for sha1, md5, crc32, fn, size, prod_code, os_code, _ in files:
            md5 = md5.lower()
            sha1 = sha1.lower()
            crc32 = crc32.lower()
            key = "%s_%s_%s" % (md5, sha1, prod_code)
            data[key] = {
                "md5": md5,
                "sha1": sha1,
                "crc32": crc32,
                "filename": fn,
                "size": int(size),
                "prod_code": prod_code,
                "os_code": os_code
            }
            count += 1
            if count % chunk_size == 0 and count != 0:
                res = requests.post(uri, json=data)
                res.raise_for_status()
                data = {}
                if verbose and count % 1000000 == 0:
                    print("    files inserted: %d" % count)
        if data:
            res = requests.post(uri, json=data)
            res.raise_for_status()
        return count

    def handle_response(self, res):
        if res.ok:
            # All good.
            return res.json()
        elif res.status_code == 404 and not res.content:
            # No entry for this digest.
            return None
        else:
            res.raise_for_status()

    def get_digest(self, digest,
                   include_filename=False, include_prod_code=False):
        get_digest_type(digest)  # Check digest is valid length.
        uri = "%s/files/%s" % (self.uri_base, digest.lower())
        params = {}
        if include_filename:
            params["include_filename"] = True
        if include_prod_code:
            params["include_prod_code"] = True
        res = requests.get(uri, params=params)
        return self.handle_response(res)

    def get_digest_exists(self, digest):
        get_digest_type(digest)  # Check digest is valid length.
        params = dict(exists=True)
        uri = "%s/files/%s" % (self.uri_base, digest.lower())
        res = requests.get(uri, params=params)
        return self.handle_response(res) is not None

    def get_digest_products(self, digest, limit=10000):
        get_digest_type(digest)  # Check digest is valid length.
        params = dict(limit=limit)
        uri = "%s/files/%s/products" % (self.uri_base, digest.lower())
        res = requests.get(uri, params=params)
        return self.handle_response(res)

    def get_os(self, code):
        uri = "%s/os/%s" % (self.uri_base, code)
        res = requests.get(uri)
        return self.handle_response(res)

    def get_manufacturer(self, code):
        uri = "%s/manufacturers/%s" % (self.uri_base, code)
        res = requests.get(uri)
        return self.handle_response(res)

    def get_product(self, code):
        uri = "%s/products/%s" % (self.uri_base, code)
        res = requests.get(uri)
        return self.handle_response(res)

    def get_product_files(self, code, limit=10000, raw=False):
        params = dict(limit=limit, include_files=True)
        uri = "%s/products/%s" % (self.uri_base, code)
        res = requests.get(uri, params=params)
        return self.handle_response(res)
