from twisted.internet import selectreactor, error
try:
    selectreactor.install()
except error.ReactorAlreadyInstalledError:
    from twisted.internet import reactor
    if reactor.__class__.__name__ != "SelectReactor":
        print "Failed to install SelectReactor because %s is already installed" % \
              reactor.__class__.__name__

__version__ = "0.0.2rc11"
