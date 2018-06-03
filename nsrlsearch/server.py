from __future__ import print_function, absolute_import
import bottle
import json

from .client import EsClient
from .config import CONFIG


# Create server application for all routes.
bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024
HttpServer = bottle.Bottle()


@HttpServer.error(404)
def route_not_found(error):
    return "bad route - please look at nsrlsearch server documentation"


def not_exists_or_result(f):
    def wrapped_function(*args, **kwargs):
        res = f(*args, **kwargs)
        if res is None:
            bottle.response.status = 404
            return None
        else:
            # Handle returning list because bottle won't.
            if isinstance(res, list):
                res = json.dumps(res)
                bottle.response.content_type = "application/json"
            return res
    return wrapped_function


# Placeholder for client class.
_CLIENT = None


def _get_client(config=None):
    global _CLIENT
    if _CLIENT is None:
        # Configure the server's client object.
        if config is None:
            config = dict(
                eskwargs={"hosts": [CONFIG.get("elasticsearch", "hosts")]},
                create_indices=CONFIG.get("elasticsearch", "createindices"),
                index_base=CONFIG.get("elasticsearch", "indexbase"),
                connectioncheck=CONFIG.get("elasticsearch", "connectioncheck")
            )
        _CLIENT = EsClient(**config)
    return _CLIENT


def deny_if_server_not_writable(func):
    def wrapped_func(*args, **kwargs):
        if not CONFIG.get("web", "writable").lower() in ("true", "True"):
            bottle.response.status = 403
            return "Server not configured to be writable."
        else:
            return func(*args, **kwargs)
    return wrapped_func


@HttpServer.get("/")
def get_root():
    return "HTTP API for NSRLsearch (doco: TODO)."


@HttpServer.get("/status")
def get_indices():
    client = _get_client()
    res = {}

    # Get indices information.
    res["indices"] = client.indices
    res["indices"]["mfg"].pop("name")
    res["indices"]["os"].pop("name")
    res["indices"]["prodfile"].pop("name")

    return res


@HttpServer.put("/indices")
@deny_if_server_not_writable
def create_indices():
    """
    Puts a single manufacturer. Requires query params:
        * shards (number of shards in index)
        * replicas (number of replicas in index)
        * recreate (boolean for whether to recreate index)
    """
    client = _get_client()
    client.create_indices(shards=bottle.request.query.shards,
                          replicas=bottle.request.query.replicas,
                          recreate=bottle.request.query.recreate)


@HttpServer.delete("/indices")
@deny_if_server_not_writable
def delete_indices():
    client = _get_client()
    client.delete_indices()


@HttpServer.get("/counts")
def counts():
    client = _get_client()
    return client.get_counts()


@HttpServer.put("/manufacturers/<code>")
@deny_if_server_not_writable
def put_manufacturer(code):
    """
    Puts a single manufacturer. Requires query params:
        * name (manufacturer name)
    """
    client = _get_client()
    client.put_manufacturer(code, bottle.request.query.name)


@HttpServer.post("/manufacturers")
@deny_if_server_not_writable
def put_manufacturers():
    json = bottle.request.json
    data = [(k, v["name"]) for k, v in json.items()]
    client = _get_client()
    client.put_manufacturers(data)


@HttpServer.put("/os/<code>")
@deny_if_server_not_writable
def put_os(code):
    """
    Puts a single manufacturer. Requires query params:
        * name (os name)
        * version (os version)
        * mfg_code (manufacturer code)
    """
    client = _get_client()
    client.put_os(code,
                  bottle.request.query.name,
                  bottle.request.query.version,
                  bottle.request.query.mfg_code)


@HttpServer.post("/os")
@deny_if_server_not_writable
def post_oss():
    json = bottle.request.json
    data = [(k, v["name"], v["version"], v["mfg_code"])
            for k, v in json.items()]
    client = _get_client()
    client.put_oss(data)


@HttpServer.put("/products/<code>")
@deny_if_server_not_writable
def put_product(code):
    """
    Puts a single product. Requires query params:
        * name (product name)
        * version (product version)
        * os_code (product os code)
        * mfg_code (product manufacturer code)
        * language (product language)
        * application_type (product application type)

    NOTE: REQUIRES MANUFACTURER AND OS TO EXIST ON SERVER FIRST!"
    """
    client = _get_client()
    client.put_product(code,
                       bottle.request.query.name,
                       bottle.request.query.version,
                       bottle.request.query.os_code,
                       bottle.request.query.mfg_code,
                       bottle.request.query.language,
                       bottle.request.query.application_type)


@HttpServer.post("/products")
@deny_if_server_not_writable
def post_products():
    json = bottle.request.json
    data = []
    for k, v in json.items():
        data.append((v["code"],
                     v["name"],
                     v["version"],
                     v["os_code"],
                     v["mfg_code"],
                     v["language"],
                     v["application_type"]))
    client = _get_client()
    client.put_products(data)


@HttpServer.put("/products/<prod_code>/files")
@deny_if_server_not_writable
def put_product_file(prod_code):
    """
    Puts a single product. Requires query params to describe the file:
        * sha1
        * md5
        * crc32
        * filename
        * size
        * os_code

    NOTE: REQUIRES MANUFACTURER AND OS TO EXIST ON SERVER FIRST!"
    """
    client = _get_client()
    client.put_product_file(prod_code,
                            bottle.request.query.sha1.lower(),
                            bottle.request.query.md5.lower(),
                            bottle.request.query.crc32.lower(),
                            bottle.request.query.filename,
                            bottle.request.query.size,
                            bottle.request.query.os_code)


@HttpServer.post("/files")
@deny_if_server_not_writable
def put_files():
    json = bottle.request.json
    data = []
    for k, v in json.items():
        data.append((v["sha1"].lower(),
                     v["md5"].lower(),
                     v["crc32"].lower(),
                     v["filename"],
                     int(v["size"]),
                     v["prod_code"],
                     v["os_code"],
                     "_"))
    client = _get_client()
    client.put_files(data)


@HttpServer.get("/files/<digest>")
@not_exists_or_result
def get_digest(digest):
    client = _get_client()

    # Handle exists check.
    if bottle.request.query.get("exists", False):
        if client.get_digest_exists(digest):
            return {"exists": True}
        else:
            return None

    # Otherwise, get the digest details.
    include_filename = bottle.request.query.get("include_filename", False)
    include_prod_code = bottle.request.query.get("include_prod_code", False)
    return client.get_digest(digest,
                             include_filename=include_filename,
                             include_prod_code=include_prod_code)


@HttpServer.get("/files/<digest>/products")
@not_exists_or_result
def get_digest_products(digest):
    limit = bottle.request.query.get("limit", 10000)
    client = _get_client()
    res = client.get_digest_products(digest, limit=limit)
    return client.get_digest_products(digest, limit=limit)


@HttpServer.get("/os/<code>")
@not_exists_or_result
def get_os(code):
    client = _get_client()
    return client.get_os(code)


@HttpServer.get("/manufacturers/<code>")
@not_exists_or_result
def get_manufacturer(code):
    client = _get_client()
    return client.get_manufacturer(code)


@HttpServer.get("/products/<code>")
@not_exists_or_result
def get_product(code):
    limit = bottle.request.query.get("limit", 10000)
    include_files = bool(bottle.request.query.get("include_files", False))
    client = _get_client()
    if include_files:
        return client.get_product_files(code, limit=limit)
    else:
        return client.get_product(code)


@HttpServer.get("/products/<code>/files")
@not_exists_or_result
def get_product_files_only(code):
    limit = bottle.request.query.get("limit", 10000)
    client = _get_client()
    res = client.get_product_files(code, limit=limit)
    if res is None:
        return None
    else:
        return res["files"]
