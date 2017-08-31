### Installation

To install this package, run `pip install -r requirements.txt; pip install -e .`

If a remote redis-server is not configured (see below), it also must be installed locally.

### Configuration

Prism can be configured using a config file located at `~/.prism.yml`, click [here](https://github.com/lbryio/reflector-cluster/blob/master/prism.yml) for an example.

`prism-server` requires port 5566 (TCP, the default reflector port) to be open. If a remote redis server is used the 
server must be able to access it - only the prism server needs access to redis, the cluster hosts don't. 
Cluster hosts in `~/.prism.yml` may be local (if in the same subnet) or public ips, or a hostname. The cluster hosts
must have the `run_reflector_server` setting for `lbrynet-daemon` set to `True`, and must be able to listen on port 5566 from the prism server (they do not need to listen on 5566 publicly). Additionally, like
other lbrynet hosts, the cluster hosts must have ports 3333 (TCP) and 4444 (UDP) open publicly in order to seed the blobs pushed to them.

### How to use it

While running `redis-server` (locally by default, remote is configurable), start the cluster entry point 
server with `prism-server` and start a worker with `prism-worker` to begin forwarding received blobs into the cluster. To add more workers just run more `prism-worker` processes.

To monitor the server, worker, and job status, run `prism-supervisor` while `prism-server` is running. The log file can be monitored with 
`tail -f ~/prism-server.log`. The workers and jobs can be managed using `rq` commands.
