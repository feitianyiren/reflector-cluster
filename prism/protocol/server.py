import json
import os
import random
import logging
from twisted.internet import defer, error
from twisted.internet.protocol import Protocol
from twisted.python import failure
from twisted.internet.error import ConnectionDone

from prism.constants import BLOB_HASH, RECEIVED_BLOB, RECEIVED_SD_BLOB, SEND_BLOB, SEND_SD_BLOB
from prism.constants import BLOB_SIZE, MAXIMUM_QUERY_SIZE, SD_BLOB_HASH, SD_BLOB_SIZE, VERSION
from prism.constants import NEEDED_BLOBS, REFLECTOR_V1, REFLECTOR_V2
from prism.error import DownloadCanceledError, InvalidBlobHashError, ReflectorRequestError
from prism.error import ReflectorClientVersionError
from prism.protocol.blob import is_valid_blobhash
from prism.protocol.task import enqueue_blob
from prism.config import get_settings

random.seed(None)

settings = get_settings()
BLOB_DIR = os.path.expandvars(settings['blob directory'])
SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS) - 1

log = logging.getLogger(__name__)


class ReflectorServerProtocol(Protocol):
    def connectionMade(self):
        peer_info = self.transport.getPeer()
        log.debug('Connected to %s:%i', peer_info.host, peer_info.port)
        self.protocol_version = self.factory.protocol_version
        self.blob_storage = self.factory.storage
        self.client_factory = self.factory.client_factory
        self.peer = peer_info
        self.received_handshake = False
        self.peer_version = None
        self.receiving_blob = False
        self.incoming_blob = None
        self.blob_write = None
        self.blob_finished_d = None
        self.cancel_write = None
        self.request_buff = ""

    def connectionLost(self, reason=failure.Failure(error.ConnectionDone())):
        log.debug("upload from %s finished", self.peer.host)

    def handle_error(self, err):
        log.error(err.getTraceback())
        self.transport.loseConnection()

    def send_response(self, response_dict):
        self.transport.write(json.dumps(response_dict))

    ############################
    # Incoming blob file stuff #
    ############################

    def clean_up_failed_upload(self, err, blob):
        if err.check(ConnectionDone):
            return
        elif err.check(DownloadCanceledError):
            log.warning("Failed to receive %s", blob)
            return self.blob_storage.delete(blob.blob_hash)
        else:
            log.exception(err)
            return self.blob_storage.delete(blob.blob_hash)

    @defer.inlineCallbacks
    def _on_completed_blob(self, blob, response_key):
        blob_hash = blob.blob_hash
        yield self.blob_storage.completed(blob.blob_hash, blob.length)
        if response_key == RECEIVED_SD_BLOB:
            yield self.blob_storage.load_sd_blob(blob)
        yield self.close_blob()
        yield self.send_response({response_key: True})
        log.info("Received %s from %s", blob, self.peer.host)
        enqueue_blob(blob_hash, self.blob_storage, self.client_factory)

    @defer.inlineCallbacks
    def _on_failed_blob(self, err, response_key):
        yield self.clean_up_failed_upload(err, self.incoming_blob)
        yield self.send_response({response_key: False})

    def handle_incoming_blob(self, response_key):
        """
        Open blob for writing and send a response indicating if the transfer was
        successful when finished.

        response_key will either be received_blob or received_sd_blob
        """
        blob = self.incoming_blob

        self.blob_finished_d, self.blob_write, self.cancel_write = blob.open_for_writing(self.peer)
        self.blob_finished_d.addCallback(self._on_completed_blob, response_key)
        self.blob_finished_d.addErrback(self._on_failed_blob, response_key)

    def close_blob(self):
        self.blob_finished_d = None
        self.blob_write = None
        self.cancel_write = None
        self.incoming_blob = None
        self.receiving_blob = False

    ####################
    # Request handling #
    ####################

    def dataReceived(self, data):
        if self.receiving_blob:
            self.blob_write(data)
        else:
            log.debug('Not yet recieving blob, data needs further processing')
            self.request_buff += data
            msg, extra_data = self._get_valid_response(self.request_buff)
            if msg is not None:
                self.request_buff = ''
                d = self.handle_request(msg)
                d.addErrback(self.handle_error)
                if self.receiving_blob and extra_data:
                    log.debug('Writing extra data to blob')
                    self.blob_write(extra_data)

    def _get_valid_response(self, response_msg):
        extra_data = None
        response = None
        curr_pos = 0
        while not self.receiving_blob:
            next_close_paren = response_msg.find('}', curr_pos)
            if next_close_paren != -1:
                curr_pos = next_close_paren + 1
                try:
                    response = json.loads(response_msg[:curr_pos])
                except ValueError:
                    if curr_pos > MAXIMUM_QUERY_SIZE:
                        raise ValueError("Error decoding response: %s" % str(response_msg))
                    else:
                        pass
                else:
                    extra_data = response_msg[curr_pos:]
                    break
            else:
                break
        return response, extra_data

    def need_handshake(self):
        return self.received_handshake is False

    def is_descriptor_request(self, request_dict):
        if SD_BLOB_HASH not in request_dict or SD_BLOB_SIZE not in request_dict:
            return False
        if not is_valid_blobhash(request_dict[SD_BLOB_HASH]):
            raise InvalidBlobHashError(request_dict[SD_BLOB_HASH])
        return True

    def is_blob_request(self, request_dict):
        if BLOB_HASH not in request_dict or BLOB_SIZE not in request_dict:
            return False
        if not is_valid_blobhash(request_dict[BLOB_HASH]):
            raise InvalidBlobHashError(request_dict[BLOB_HASH])
        return True

    def handle_request(self, request_dict):
        if self.need_handshake():
            return self.handle_handshake(request_dict)
        if self.is_descriptor_request(request_dict):
            return self.handle_descriptor_request(request_dict)
        if self.is_blob_request(request_dict):
            return self.handle_blob_request(request_dict)
        raise ReflectorRequestError("Invalid request")

    def handle_handshake(self, request_dict):
        """
        Upon connecting, the client sends a version handshake:
        {
            'version': int,
        }

        The server replies with the same version if it is supported
        {
            'version': int,
        }
        """

        if VERSION not in request_dict:
            raise ReflectorRequestError("Client should send version")

        if int(request_dict[VERSION]) not in [REFLECTOR_V1, REFLECTOR_V2]:
            raise ReflectorClientVersionError("Unknown version: %i" % int(request_dict[VERSION]))

        self.peer_version = int(request_dict[VERSION])
        log.debug('Handling handshake for client version %i', self.peer_version)
        self.received_handshake = True
        return self.send_handshake_response()

    def send_handshake_response(self):
        d = defer.succeed({VERSION: self.peer_version})
        d.addCallback(self.send_response)
        return d

    def handle_descriptor_request(self, request_dict):
        """
        If the client is reflecting a whole stream, they send a stream descriptor request:
        {
            'sd_blob_hash': str,
            'sd_blob_size': int
        }

        The server indicates if it's aware of this stream already by requesting (or not requesting)
        the stream descriptor blob. If the server has a validated copy of the sd blob, it will
        include the needed_blobs field (a list of blob hashes missing from reflector) in the
        response. If the server does not have the sd blob the needed_blobs field will not be
        included, as the server does not know what blobs it is missing - so the client should send
        all of the blobs in the stream.
        {
            'send_sd_blob': bool
            'needed_blobs': list, conditional
        }


        The client may begin the file transfer of the sd blob if send_sd_blob was True.
        If the client sends the blob, after receiving it the server indicates if the
        transfer was successful:
        {
            'received_sd_blob': bool
        }
        """

        sd_blob_hash = request_dict[SD_BLOB_HASH]
        sd_blob_size = request_dict[SD_BLOB_SIZE]

        if self.blob_write is None:
            d = self.get_descriptor_response(sd_blob_hash, sd_blob_size)
            d.addCallback(self.send_response)
        else:
            self.receiving_blob = True
            d = self.blob_finished_d
        return d

    @defer.inlineCallbacks
    def get_descriptor_response(self, sd_hash, sd_size):
        needed = yield self.blob_storage.get_needed_blobs_for_stream(sd_hash)

        if needed is not None:
            response = {
                SEND_SD_BLOB: False,
                NEEDED_BLOBS: needed
            }
        else:
            sd_blob = yield self.blob_storage.get_blob(sd_hash, sd_size)
            self.incoming_blob = sd_blob
            self.receiving_blob = True
            self.handle_incoming_blob(RECEIVED_SD_BLOB)
            response = {SEND_SD_BLOB: True}
        defer.returnValue(response)

    def handle_blob_request(self, request_dict):
        """
        A client queries if the server will accept a blob
        {
            'blob_hash': str,
            'blob_size': int
        }

        The server replies, send_blob will be False if the server has a validated copy of the blob:
        {
            'send_blob': bool
        }

        The client may begin the raw blob file transfer if the server replied True.
        If the client sends the blob, the server replies:
        {
            'received_blob': bool
        }
        """

        blob_hash = request_dict[BLOB_HASH]
        blob_size = request_dict[BLOB_SIZE]

        if self.blob_write is None:
            log.debug('Received info for blob: %s', blob_hash[:16])
            d = self.get_blob_response(blob_hash, blob_size)
            d.addCallback(self.send_response)
        else:
            log.debug('blob is already open')
            self.receiving_blob = True
            d = self.blob_finished_d
        return d

    @defer.inlineCallbacks
    def get_blob_response(self, blob_hash, blob_size):
        in_cluster = yield self.blob_storage.blob_has_been_forwarded_to_host(blob_hash)
        if in_cluster:
            response = {SEND_BLOB: False}
        else:
            exists_locally = yield self.blob_storage.blob_exists(blob_hash)
            if exists_locally:
                response = {SEND_BLOB: False}
            else:
                blob = yield self.blob_storage.get_blob(blob_hash, blob_size)
                self.incoming_blob = blob
                self.receiving_blob = True
                self.handle_incoming_blob(RECEIVED_BLOB)
                response = {SEND_BLOB: True}
        defer.returnValue(response)
