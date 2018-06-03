"""
Module containing the command-line interface (CLI).
"""
from __future__ import print_function
import os
import sys
import begin
import pprint
import readline
try:
    # Python 3.x
    from configparser import ConfigParser
except ImportError:
    # Python 2.x
    from ConfigParser import ConfigParser
    input = raw_input

from .config import CONFIG, CONFIGURED, CONFIG_PATH, CONFIG_KEYORDER, \
                    DEFAULT_CONFIG
from .client import EsClient, HttpClient
from .ingest import NsrlIngestor
from . import server


@begin.subcommand
def configure(configpath=CONFIG_PATH):
    """
    Prompts for web and Elasticsearch configuration options.
    """
    # Already configured?
    if os.path.exists(configpath):
        res = input(
            "Configuration file %s exists. Overwrite (y/N)? " % configpath)
        if res.lower() in ("n", ""):
            # Chose not to do anything.
            return
        elif res.lower() == "y":
            print("Note: When overriding, default values will be taken\n"
                  "      from the existing existing configuration file.\n"
                  "      If an entirely new configuration file is desired,\n"
                  "      delete the existing file (%s) first." % configpath)

    # Read the CONFIG_KEYORDER and ask configuration questions.
    for section, field, getter, question in CONFIG_KEYORDER:
        if CONFIGURED:
            default = getattr(CONFIG, getter)(section, field)
        else:
            default = getattr(DEFAULT_CONFIG, getter)(section, field)
        res = input("%s (default: %s)? " % (question, default))
        if res != "":
            CONFIG.set(section, field, res)

    with open(configpath, "w") as configfile:
        CONFIG.write(configfile)
        print("New configuration written to %s." % configpath)


# Optionally define subcommands which require configuration.
if CONFIGURED:
    @begin.subcommand
    def ingest(source,
               server_type=CONFIG.get("query_and_ingest_client",
                                      "servertype"),
               server=CONFIG.get("query_and_ingest_client",
                                 "server"),
               recreate=CONFIG.getboolean("query_and_ingest_client",
                                          "recreate")):
        """
        Ingest from specified source. Argument defaults read from config file.
        """
        # Create the specified type of client.
        if server_type == "http":
            client = HttpClient(uri="http://%s" % server)
        elif server_type == "elasticsearch":
            client = EsClient(eskwargs={"hosts": [server]})
        else:
            print("server_type '%s' not one of 'http' or 'elasticsearch'" %
                  server_type, file=sys.stderr)
            return 1

        # Delete whatever already exists if told to do so.
        if recreate and client.indices_exist:
            print("Deleting indices...", end=" ")
            client.delete_indices()
            print("done.")
            print("Creating indices...", end=" ")
            client.create_indices()
            print("done.")

        # Create ingestor.
        ingestor = NsrlIngestor(client, verbose=True)
        if os.path.isdir(source):
            ingestor.ingest_from_directory(source)
        elif os.path.isfile(source):
            ingestor.ingest_from_iso(source)
        else:
            raise NotImplementedError("TODO - implement other ingest formats")


    @begin.subcommand
    def count(server_type=CONFIG.get("query_and_ingest_client", "servertype"),
              server=CONFIG.get("query_and_ingest_client", "server")):
        """
        Display count of documents in indices. 
        """
        # Create the specified type of client.
        if server_type == "http":
            client = HttpClient(uri="http://%s" % server)
        elif server_type == "elasticsearch":
            client = EsClient(eskwargs={"hosts": [server]})
        else:
            print("server_type '%s' not one of 'http' or 'elasticsearch'" %
                  server_type, file=sys.stderr)
            return 1

        header = "%-12s:%12s" % ("index name", "count")
        print("%s\n%s" % (header, len(header) * "-"))
        for k, v in client.get_counts().items():
            print("%-12s:%12d" % (k, v))


    @begin.subcommand
    def query(details=False,
              server_type=CONFIG.get("query_and_ingest_client", "servertype"),
              server=CONFIG.get("query_and_ingest_client", "server"),
              *digests):
        """
        Returns either details or exists checks for specified digests.
        """

        # Create the specified type of client.
        if server_type == "http":
            client = HttpClient(uri="http://%s" % server)
        elif server_type == "elasticsearch":
            client = EsClient(eskwargs={"hosts": [server]})
        else:
            print("server_type '%s' not one of 'http' or 'elasticsearch'" %
                  server_type, file=sys.stderr)
            return 1

        for d in digests:
            if len(d) not in (32, 40, 8):
                # Reject digests with invalid length.
                print("%s: unknown digest type" % d)
                continue

            if details:
                print("%s: %s" % (d, client.get_digest(d)))
            else:
                print("%s: %s" % (d, client.get_digest_exists(d)))


    @begin.subcommand
    def web(host=CONFIG.get("web", "host"),
            port=CONFIG.getint("web", "port"),
            writable=CONFIG.get("web", "writable"),
            wsgiserver=CONFIG.get("web", "wsgiserver"),
            gunicornworkers=CONFIG.getint("web", "gunicornworkers"),
            eshosts=CONFIG.get("elasticsearch", "hosts"),
            esconnectioncheck=CONFIG.get("elasticsearch", "connectioncheck"),
            escreateindices=CONFIG.get("elasticsearch", "createindices"),
            esindexbase=CONFIG.get("elasticsearch", "indexbase")):
        """
        Runs the HttpServer.
        """
        # Set the writable status, just for this run (read by the server module).
        CONFIG.set("web", "writable", writable)

        # Configure the server's configuration options.
        esclient_config = {
            "eskwargs": {"hosts": eshosts.split(",")},
            "connection_check": esconnectioncheck,
            "create_indices": escreateindices,
            "index_base": esindexbase}
        print("Server's client will connect to Elasticsearch with config:")
        print("\n".join([" %s: %s" % (k, v) for k, v in esclient_config.items()]))

        server._get_client(config=esclient_config)
        if esconnectioncheck:
            print("Server's client passed Elasticsearch connection check.")

        if wsgiserver == "gunicorn":
            print("Starting server (%s)..." % wsgiserver)
            server.HttpServer.run(host=host, port=port,
                                  server=wsgiserver, workers=gunicornworkers)
        elif wsgiserver == "bjoern":
            print("Warning: bjoern is untested and has system dependencies.")
            print("Starting server (%s)..." % wsgiserver)
            server.HttpServer.run(host=host, port=port, server='bjoern')
        else:
            print("Warning: Unknown wsgiserver '%s' in config; " % wsgiserver +
                  "try 'gunicorn', 'bjoern', or submitting a pull request. ")
            print("Starting server (wsgiref)...")
            server.HttpServer.run(host=host, port=port)


@begin.start
def main():
    """
    Ingest, search and serve NIST NSRL data in an Elasticsearch instance.
    Please run "nsrlsearch configure" before using.
    """
    pass
