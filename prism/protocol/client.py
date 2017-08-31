import json
import logging

from twisted.protocols.basic import FileSender
from twisted.internet.protocol import Protocol
from twisted.internet import defer, error, reactor
from prism.error import IncompleteResponse


log = logging.getLogger(__name__)


class BlobReflectorClient(Protocol):
    #  Protocol stuff

    def connectionMade(self):
        self.response_buff = ''
        self.outgoing_buff = ''
        self.next_blob_to_send = None
        self.blob_read_handle = None
        self.received_handshake_response = False
        self.file_sender = None
        self.producer = None
        self.streaming = False
        self.sent_blobs = False
        d = self.send_handshake()
        d.addErrback(lambda err: log.warning("An error occurred immediately: %s", err.getTraceback()))

    def dataReceived(self, data):
        log.debug('Received %s', data)
        self.response_buff += data
        try:
            msg = self.parse_response(self.response_buff)
        except IncompleteResponse:
            pass
        else:
            self.response_buff = ''
            d = self.handle_response(msg)
            d.addCallback(lambda _: self.send_next_request())
            d.addErrback(self.response_failure_handler)

    def connectionLost(self, reason):
        self.factory.on_connection_lost_d.callback(None)
        if reason.check(error.ConnectionDone):
            log.debug('Reflector finished: %s', reason)
        else:
            raise reason

    # IConsumer stuff
    def registerProducer(self, producer, streaming):
        self.producer = producer
        self.streaming = streaming
        if self.streaming is False:
            reactor.callLater(0, self.producer.resumeProducing)

    def unregisterProducer(self):
        self.producer = None

    def write(self, data):
        self.transport.write(data)
        if self.producer is not None and self.streaming is False:
            reactor.callLater(0, self.producer.resumeProducing)

    def send_handshake(self):
        log.debug('Sending handshake')
        self.write(json.dumps({'version': self.protocol_version}))
        return defer.succeed(None)

    def parse_response(self, buff):
        try:
            return json.loads(buff)
        except ValueError:
            raise IncompleteResponse()

    def response_failure_handler(self, err):
        log.warning("An error occurred handling the response: %s", err.getTraceback())
        return self.disconnect(err)

    def handle_response(self, response_dict):
        if self.received_handshake_response is False:
            return self.handle_handshake_response(response_dict)
        else:
            return self.handle_normal_response(response_dict)

    def set_not_uploading(self):
        if self.next_blob_to_send is not None:
            self.next_blob_to_send.close_read_handle(self.read_handle)
            self.read_handle = None
            self.next_blob_to_send = None
        self.file_sender = None
        return defer.succeed(None)

    @defer.inlineCallbacks
    def start_transfer(self):
        if self.read_handle is None:
            raise Exception("self.read_handle was None when trying to start the transfer")
        yield self.file_sender.beginFileTransfer(self.read_handle, self)
        self.sent_blobs = True
        self.blob_hashes_sent.append(self.next_blob_to_send.blob_hash)

    def handle_handshake_response(self, response_dict):
        if 'version' not in response_dict:
            raise ValueError("Need protocol version number!")
        server_version = int(response_dict['version'])

        if self.protocol_version != server_version:
            raise ValueError("I can't handle protocol version {}!".format(self.protocol_version))
        self.received_handshake_response = True
        return defer.succeed(True)

    def handle_normal_response(self, response_dict):
        if self.file_sender is None:  # Expecting Server Info Response
            if 'send_blob' not in response_dict:
                raise ValueError("I don't know whether to send the blob or not!")
            if response_dict['send_blob'] is True:
                self.file_sender = FileSender()
                return defer.succeed(True)
            else:
                return self.set_not_uploading()
        else:  # Expecting Server Blob Response
            if 'received_blob' not in response_dict:
                raise ValueError("I don't know if the blob made it to the intended destination!")
            else:
                return self.set_not_uploading()

    def open_blob_for_reading(self, blob):
        if blob.is_validated():
            read_handle = blob.open_for_reading()
            if read_handle is not None:
                log.debug('Getting ready to send %s', blob.blob_hash)
                self.next_blob_to_send = blob
                self.read_handle = read_handle
                return None
        raise ValueError(
            "Couldn't open that blob for some reason. blob_hash: {}".format(blob.blob_hash))

    def send_blob_info(self):
        log.debug("Send blob info for %s", self.next_blob_to_send.blob_hash)
        assert self.next_blob_to_send is not None, "need to have a next blob to send at this point"
        self.write(json.dumps({
            'blob_hash': self.next_blob_to_send.blob_hash,
            'blob_size': self.next_blob_to_send.length
        }))

    def disconnect(self, err):
        self.transport.loseConnection()

    @defer.inlineCallbacks
    def send_next_request(self):
        if self.file_sender is not None:
            # send the blob
            log.debug('Sending the blob')
            yield self.start_transfer()
        elif self.blob_hashes_to_send:
            # open the next blob to send
            blob_hash = self.blob_hashes_to_send[0]
            log.debug('No current blob, sending the next one: %s', blob_hash)
            self.blob_hashes_to_send = self.blob_hashes_to_send[1:]
            blob = yield self.blob_storage.get_blob(blob_hash)
            try:
                self.open_blob_for_reading(blob)
                # send the server the next blob hash + length
                self.send_blob_info()
            except ValueError:
                yield self.send_next_request()
        else:
            # close connection
            log.debug('No more blob hashes, closing connection')
            self.transport.loseConnection()
