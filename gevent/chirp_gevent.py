#!/usr/bin/env python

import datetime
import json
import logging
import os
import re
import sys
import time
from collections import deque

import gevent
import pymongo
from bson.objectid import ObjectId
from socketio import socketio_manage
from socketio.server import SocketIOServer
from socketio.namespace import BaseNamespace

# Global state: new chirps on the right, old ones fall off the left
chirps = deque([], maxlen=20)
sessions = set()
cursor_manager = None
db = None


def create_collection():
    db.create_collection('chirps', size=10000, capped=True)
    logging.info('Created capped collection "chirps" in database "test"')


def json_default(obj):
    """
    Convert non-JSON-serializable obj to something serializable
    """
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return str(obj)
    else:
        raise TypeError("%s is not JSON-serializable" % repr(obj))


class TailingHandler(BaseNamespace):
    def on_get_chirps(self):
        """Client starts waiting for new chirps"""
        logging.info('get chirps %s')

        # Don't know how to catch disconnections, so sessions grows forever
        sessions.add(self)


class ChirpsHandler(object):
    def get(self, env, start_response):
        """Get all recent chirps at once"""
        logging.debug('Getting %d chirps' % len(chirps))
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [json.dumps(list(chirps), default=json_default)]


class CursorManager(gevent.greenlet.Greenlet):
    """Manage the single cursor tailing the "chirps" collection"""
    def __init__(self, db):
        super(CursorManager, self).__init__()
        self.db = db

    def emit(self, name, value):
        for handler in sessions:
            handler.emit(name, value)

    def run(self):
        while True:
            if len(chirps):
                last_chirp = chirps[-1]
                query = {
                    'ts': {'$gte': last_chirp['ts']},
                    '_id': {'$ne': last_chirp['_id']}
                }
            else:
                query = {}

            response, error = None, None
            try:
                response = list(self.db.chirps.find(
                    query,
                    tailable=True
                ))
            except Exception, e:
                logging.exception('chirps.find()')
                error = e

            if error:
                # Something's wrong with this cursor, wait 1 second before trying
                # again
                gevent.sleep(1)

                # Ignore errors from dropped collections
                if not error.message.endswith('not valid at server'):
                    self.emit('app_error', error.message)

                return

            elif response:
                print 'CursorManager got', response
                chirps.extend(response)

                # We have new data for the client.
                logging.debug('New data: ' + str(response)[:150])

                self.emit(
                    'chirps',
                    json.dumps(
                        {'chirps': response},
                        default=json_default
                    )
                )

            gevent.sleep(.1)


class NewChirpHandler(object):
    def post(self, env, start_response):
        """
        Insert a new chirp in the capped collection
        """
        msg = env['wsgi.input'].read()

        # Note that this is a fire-and-forget insert, for speed. To be certain
        # your inserts succeed, pass safe=True
        db.chirps.insert({
            'msg': msg,
            'ts': datetime.datetime.utcnow(),
            '_id': ObjectId(),
        })

        start_response('200 OK', [])
        return []


class ClearChirpsHandler(object):
    def post(self, env, start_response):
        """
        Delete everything in the collection
        """
        db.chirps.drop()
        chirps.clear()
        create_collection()
        cursor_manager.emit('cleared', {})
        start_response('200 OK', [])
        return []


class StaticFileHandler(object):
    def get(self, env, start_response):
        # No, this is not secure.
        path = env['PATH_INFO']
        if not path or path == '/':
            path = 'static'

        path = os.path.join(os.getcwd(), path.strip('/'))

        if os.path.isdir(path):
            indexpath = os.path.join(path, 'index.html')
            if os.path.exists(indexpath):
                path = indexpath

        if os.path.isfile(path):
            start_response('200 OK', [('Content-Type', 'text/html')])
            with open(path) as f:
                return [f.read()]

        start_response('404 NOT FOUND', [('Content-Type', 'text/html')])
        return [path, ' not found']


if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_socket() # patch_thread() is unnecessary w/ PyMongo 2.2
    db = pymongo.Connection(use_greenlets=True).test
    try:
        create_collection()
    except pymongo.errors.CollectionInvalid:
        if 'capped' not in db.chirps.options():
            print >> sys.stderr, (
                'test.chirps exists and is not a capped collection,\n'
                'please drop the collection and start this example app again.'
            )
            sys.exit(1)

    cursor_manager = CursorManager(db)
    cursor_manager.start()

    def application(env, start_response):
        path = env['PATH_INFO']

        if path.startswith('/socket.io'):
            socketio_manage(env, {'': TailingHandler})
            return

        for urlpattern, handler_class in [
            (r'/chirps', ChirpsHandler),
            (r'/new', NewChirpHandler),
            (r'/clear', ClearChirpsHandler),
            (r'/(.*)', StaticFileHandler),
        ]:
            # No, this is not efficient.
            match = re.match(urlpattern, path)
            if match:
                handler = handler_class()
                methodname = env['REQUEST_METHOD'].lower()
                method = getattr(handler, methodname)
                return method(env, start_response)

    print 'listening on port 8000'
    SocketIOServer(
        ('', 8000), application, namespace='socket.io', policy_server=False
    ).serve_forever()
