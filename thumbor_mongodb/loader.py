#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/dhardy92/thumbor

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 figarocms dhardy@figarocms.fr

import base64
from bson.errors import InvalidId as InvalidIdError
from pymongo import Connection
from bson.objectid import ObjectId


def __conn__(context):
    connection = Connection(context.config.MONGO_LOADER_CNX_STRING)
    db = connection[context.config.MONGO_LOADER_SERVER_DB]
    storage = db[context.config.MONGO_LOADER_SERVER_COLLECTION]
    return connection, db, storage


def load(context, url, callback):
    connection, db, storage = __conn__(context)
    search = url

    if context.config.MONGO_LOADER_ID_FIELD == '_id':
        try:
            search = ObjectId(url)
        except (TypeError, ValueError, InvalidIdError):
            callback(None)
            return

    document = storage.find_one(
        {context.config.MONGO_LOADER_ID_FIELD: search},
        {context.config.MONGO_LOADER_DOC_FIELD: True}
    )

    if not document:
        callback(None)
        return

    try:
        body = document[context.config.MONGO_LOADER_DOC_FIELD]
    except KeyError:
        callback(None)
        return

    if context.config.MONGO_LOADER_IMAGE_FMT == 'base64':
        try:
            file_data = base64.b64decode(body)
        except TypeError:
            callback(None)
            return
        else:
            callback(file_data)
    else:
        callback(body)
