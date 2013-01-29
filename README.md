A trivial Twitter clone. A set of example apps demonstrating asynchronous
Python web frameworks with MongoDB.

Installation
------------

git clone git://github.com/ajdavis/chirp.git
cd chirp
pip install -I -r requirements.txt

Examples
--------

* `sync/chirp_sync.py`: Use [PyMongo](http://pypi.python.org/pypi/pymongo/), the
  standard blocking driver, with Tornado. See the tragic consequences of mixing
  a blocking driver with a non-blocking web framework.
* `motor/chirp_motor.py`: Use [Motor](https://github.com/mongodb/motor), my
  non-blocking driver for MongoDB and Tornado.
* `gevent/chirp_gevent.py`: Just for completeness, use PyMongo with Gevent.
