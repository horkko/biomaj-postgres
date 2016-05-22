BioMAJ3 - PostgreSQL
=====

This project is a workaround to try to use BioMAJ3 with PostgreSQL as a backend
instead of MongDB.
This project is a very very early dev stage. It is not working at all, it is under
development.

Dependencies
============

Python:
 * BioMAJ3
 Linux tools: tar, unzip, gunzip, bunzip

Database:
 * PostgreSQL (local or remote)

Installation
============

After dependencies installation, go in BioMAJ source directory:

    python setup.py install


You should consider using a Python virtual environment (virtualenv) to install BioMAJ.

In tools/examples, copy the global.properties and update it to match your local
installation.

The tools/process contains example process files (python and shell).

License
=======

A-GPL v3+

