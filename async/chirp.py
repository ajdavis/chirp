#!/usr/bin/env python

import datetime
import json
import logging
import sys
import time
from collections import deque

import tornado.ioloop
import tornado.web
import tornado.options
import tornadio2
import pymongo
import pymongo.objectid
import asyncmongo


# Global state: new chirps on the right, old ones fall off the left
chirps = deque([], maxlen=20)
session2handler = {}
sync_db = None

def create_collection():
    sync_db.create_collection('chirps', size=10000, capped=True)
    logging.info('Created capped collection "chirps" in database "test"')


def json_default(obj):
    """
    Convert non-JSON-serializable obj to something serializable
    """
    if isinstance(obj, pymongo.objectid.ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return str(obj)
    else:
        raise TypeError("%s is not JSON-serializable" % repr(obj))


class TailingHandler(tornadio2.SocketConnection):
    @tornadio2.event
    def get_chirps(self):
        """Client starts waiting for new chirps"""
        logging.debug('get chirps')
        session2handler[self.session.session_id] = self

    def on_close(self):
        """Client disconnected"""
        session2handler.pop(self.session.session_id, None)


class ChirpsHandler(tornado.web.RequestHandler):
    def get(self):
        """Get all recent chirps at once"""
        logging.debug('Getting %d chirps' % len(chirps))
        self.write(json.dumps(list(chirps), default=json_default))


class CursorManager(object):
    """Manage the single cursor tailing the "chirps" collection"""
    def __init__(self, async_db):
        self.async_db = async_db

    def start(self):
        self._find()

    def emit(self, name, value):
        for handler in session2handler.values():
            handler.emit(name, value)

    def _find(self):
        if chirps:
            last_chirp = chirps[-1]
            query = {
                'ts': {'$gte': last_chirp.get('ts')},
                '_id': {'$ne': last_chirp.get('_id')}
            }
        else:
            query = {}

        # Relies on asyncmongo being patched with
        # https://github.com/bitly/asyncmongo/pull/39
        self.cursor = self.async_db.chirps.find(
            query,
            tailable=True, await_data=True,
            callback=self._on_response
        )

    def _remove_dead_cursor(self):
        logging.warn('dead cursor')
        self.cursor = None

        # Wait 1 second before trying again
        tornado.ioloop.IOLoop.instance().add_timeout(
            time.time() + 1,
            self._find
        )

    def _on_response(self, response, error):
        """
        Asynchronous callback when find() or get_more() completes. Sends result
        to the client.
        """
        if error:
            # Something's wrong with this cursor, remove it
            self._remove_dead_cursor()

            # Ignore errors from dropped collections
            if not error.message.endswith('not valid at server'):
                self.emit('app_error', error.message)

        elif response:
            chirps.extend(response)

            # We have new data for the client.
            logging.debug('New data: ' + str(response)[:50])

            self.emit(
                'chirps',
                json.dumps(
                    {'chirps': response},
                    default=json_default
                )
            )

        # Response is empty list if no data came in during the seconds while
        # we waited for more data. get_more() does *not* block indefinitely
        # for more data, it only blocks for a few seconds.
        if self.cursor:
            if self.cursor.alive:
                self._get_more()
            else:
                self._remove_dead_cursor()
        else:
            self._find()

    def _get_more(self):
        # Continue tailing. The await_data=True option we passed in when we
        # created the cursor means that if there's no more data, the cursor
        # will wait about a second for more data to come in before returning
        try:
            self.cursor.get_more(
                callback=self._on_response
            )
        except Exception, e:
            logging.exception('iterating cursor')
            self._remove_dead_cursor()


class NewChirpHandler(tornado.web.RequestHandler):
    # This method will exit before the request is complete, thus "asynchronous"
    @tornado.web.asynchronous
    def post(self):
        """
        Insert a new chirp in the capped collection
        """
        msg = self.request.body
        self.settings['async_db'].chirps.insert(
            {
                'msg': msg,
                'ts': datetime.datetime.utcnow(),
            },
            callback=self._on_response
        )

    def _on_response(self, response, error):
        if error:
            raise error
        self.finish()


class ClearChirpsHandler(tornado.web.RequestHandler):
    def post(self):
        """
        Delete everything in the collection
        """
        sync_db.chirps.drop()
        chirps.clear()
        create_collection()
        self.settings['cursor_manager'].emit('cleared', {})


if __name__ == '__main__':
    sync_db = pymongo.Connection().test
    try:
        create_collection()
    except pymongo.errors.CollectionInvalid:
        if 'capped' not in sync_db.chirps.options():
            print >> sys.stderr, (
                'test.chirps exists and is not a capped collection,\n'
                'please drop the collection and start this example app again.'
            )
            sys.exit(1)

    tornado.options.define('debug', default=False, type=bool, help=(
        "Turn on autoreload"
    ))

    tornado.options.parse_command_line()

    async_db = asyncmongo.Client(
        pool_id='test', host='127.0.0.1', port=27017, mincached=5,
        maxcached=15, maxconnections=30, dbname='test'
    )

    cursor_manager = CursorManager(async_db)
    cursor_manager.start()

    router = tornadio2.TornadioRouter(TailingHandler)

    application = tornado.web.Application(
        router.apply_routes([
            (r'/chirps', ChirpsHandler),
            (r'/new', NewChirpHandler),
            (r'/clear', ClearChirpsHandler),
            (r'/()', tornado.web.StaticFileHandler, {'path': 'static/index.html'}),
        ]),

        static_path='static',
        socket_io_port = 8001,

        async_db=async_db,
        cursor_manager=cursor_manager,
        debug=tornado.options.options.debug,
    )

    tornadio2.SocketServer(application)
