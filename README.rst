nsrlsearch
==========

.. contents:: Table of Contents


Introduction
============

The National Institute of Standards and Technology (NIST) maintain
and distribute a set of known file hashes called the National Software
Reference Library (NSRL) Reference Data Sets (RDS). This data set 
has several applications, including malware analysis and computer forensics.

This library has everything you need to:

-  Ingest NIST NSRL RDS formatted CSV files into an Elasticsearch instance,
   including from the ISO images distributed by NIST,
-  A conventient Python library for accessing the data from Elasticsearch,
-  A HTTP microservice which lets you expose the indexed data without
   unsafely exposing your Elasticsearch instance,
-  Another convenient Python library for accessing the data from the 
   HTTP service (which exposes the same API as the the Elasticsearch client),
   and
-  A Command Line Interface (CLI) for ingesting the NIST NSRL RDS data which
   can use either of the previously described clients.


Usage - CLI
===========

After installation, it may be desired to configure the HTTP server and
the CLI tool. To do so, run the ```configure``` subcommand.
This will prompt for configuration options along with a description of what
they do. For testing purposes, the defaults should be fine.

Once configured, the CLI tool can be used to run ingest NSRL data,
run the HTTP server and query the data set.

Examples of each of these are provided below.

::

   $ nsrlsearch configure
   Interface for web server to listen on (default: localhost)? 
   Port for web server to listen on (default: 8080)?
   .
   . (edited out for brevity)
   .
   New configuration written to /home/user/.nsrlsearch.cfg.
   
   $ nsrlsearch ingest RDS_ios.iso
   Inserting mfg info... done! Put 80456 in 7.739879s
   Inserting os info... done! Put 913 in 0.157383s
   .
   . (edited out for brevity)
   .
   File ingest done! Put 14390472 in 2481.755694s

   $ nsrlsearch web
   Server's client will connect to Elasticsearch with config:
     create_indices: true
     eskwargs: {'hosts': ['localhost:9200']}
     index_base: nsrl
     connection_check: true
   Server's client passed Elasticsearch connection check.
   Starting server (gunicorn)...
   Bottle v0.12.13 server starting up (using GunicornServer(workers=8))...
   Listening on http://localhost:8080/
   Hit Ctrl-C to quit.

   $ nsrlsearch query aaaaaaaa bbbbbbbb cccccccc
   aaaaaaaa: False
   bbbbbbbb: False
   cccccccc: False


The ```nsrlsearch``` command and all its subcommands have help available
via the ```-h/--help``` options.


Usage - Python
==============

Typically, when using this library from Python the main goal will be to
either ingest or query hashes from the Elasticsearch server. This is achieved
by the client classes of :py:class:`nsrlsearch.client.EsClient` (which works
directly with Elasticsearch) and :py:class:`nsrlsearch.client.HttpClient`
(which works directly with the HTTP server).

Once instantiated, these two clients expose the same interface for all
querying and indexing operations.

::

   In [1]: from elasticsearch import Elasticsearch
   
   In [2]: from nsrlsearch.client import EsClient, HttpClient
   
   In [3]: # Create a Elasticsearch client for nsrlsearch's EsClient to use.

   In [4]: es = Elasticsearch(hosts=["localhost:9200"])

   In [5]: # Create nsrlsearch's EsClient to query the the Elasticsearch server.

   In [6]: client = EsClient(es=es)

   In [7]: client.get_digest_exists("00000000") # can be crc32, sha1, md5
   Out[7]: False

   In [8]: # Similarly, the HttpClient can be used to talk to the Http server (if it is running)

   In[9]: client = HttpClient(uri="http://localhost:8080")

   In[10]: client.get_digest_exists("00000000") # can be crc32, sha1, md5
   Out[10]: False


Future Work & Contributions
===========================

There is much that could be done here, including:

- a properly RESTful HTTP interface on the microservice
- better support for alternate sets of data
- support/testing against Elasticsearch 6.x

Contributions on the above or other useful features will be appreciated.


Change Log
==========


Version 0.0.2.60 - Initial release:

- ingest and query NSRL directly into Elasticsearch
- ingest and query NSRL data through and intermediary HTTP microservice
- clients for direct communication with Elasticsearch and the intermediary
  HTTP microservice expose identical interfaces for both querying and
  ingest
- support ingest direct from ISO image
- tested against NSRL RDS 2.60 on Elasticsearch 5.6 (Python 2 and 3)


License and Source Availability
===============================

The nsrlsearch library and package is licensed under APLv2.


Other Thanks
============

The National Institute of Standards and Technology for their ongoing work
building and curating the NSRL data set.
