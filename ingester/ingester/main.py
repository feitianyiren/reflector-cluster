from twisted.internet import reactor
import server


def main():
    reflector_server = server.ReflectorClusterServer()

    def stop(*_):
        def _stop():
            reactor.fireSystemEvent("shutdown")
        reactor.callLater(30, _stop)

    d = reflector_server.start()
    d.addCallback(stop)

    reactor.run()


if __name__ == "__main__":
    main()
