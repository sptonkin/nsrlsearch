"""
Module containing constants used when initially setting up nsrlsearch's config.
"""
import os
try:
    # Python 3.x
    from configparser import ConfigParser
except ImportError:
    # Python 2.x
    from ConfigParser import ConfigParser


# Default path for configuration file.
CONFIG_PATH = os.path.join(os.environ["HOME"], ".nsrlsearch.cfg")
DEFAULT_CONFIG_PATH = \
    os.path.join(os.path.dirname(__file__), "default_config.cfg")
DEFAULT_CONFIG = ConfigParser()
DEFAULT_CONFIG.read(DEFAULT_CONFIG_PATH)


# Figure out if we have a configuration file.
if os.path.exists(CONFIG_PATH):
    CONFIG = ConfigParser()
    CONFIG.read(CONFIG_PATH)
    CONFIGURED = True
else:
    CONFIG = DEFAULT_CONFIG
    CONFIGURED = False


# Order in which keys will be queried during configuration, consisting of
# (config section, config field, ConfigParser get method,
# question for configuring user) tuples.
CONFIG_KEYORDER = [
        ("web", "host", "get",
	 "Interface for web server to listen on"),
	("web", "port", "getint",
	 "Port for web server to listen on"),
	("web", "writable", "getboolean",
	 "Specify whether web server should support ingest"),
	("web", "wsgiserver", "get",
	 "Which wsgi server should be used (gunicorn, bjoern, etc.)"),
	("web", "gunicornworkers", "getint",
	 "If using gunicorn wsgi server, how many workers should run"),
        ("elasticsearch", "hosts", "get",
	 "Elasticsearch server(s), seperated by commas if multiple"),
	("elasticsearch", "connectioncheck", "getboolean",
	 "Test Elasticsearch connectivity on startup"),
	("elasticsearch", "createindices", "getboolean",
	 "Create Elasticsearch indices on startup if not present"),
	("elasticsearch", "indexbase", "get",
	 "Index prefix of Elasticsearch indices to use"),
	("query_and_ingest_client", "servertype", "get",
	 "Server type query and ingest subcommands should  " +
	 "communicate with, options are 'elasticsearch' and 'http'"),
        ("query_and_ingest_client", "server", "get",
	 "Server host:port ingest client should communciate with."),
	("query_and_ingest_client", "recreate", "get",
	 "Recreate Elasticsearch indices on ingest (dangerous!)"),
    ]

