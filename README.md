# THIS REPO IS RETIRED. NEW REFLECTOR IS AT https://github.com/lbryio/reflector.go

## Requirements

Prism server needs access to a redis instance.


## Installation

```
# clone the repo
pip install -r requirements.txt
pip install .
```


## Configuration

### Prism Server

- Create config file at `~/.prism.yml` ([example](https://github.com/lbryio/reflector-cluster/blob/master/prism.yml)).
- Open port `5566/tcp` to the world.
- Ensure redis is accessible by server.

### Cluster Hosts

- Open ports `3333/tcp` and `4444/udp` to the world. Open port `5566/tcp` to the prism server.
- Set `run_reflector_server` to `True` in lbrynet daemon config.


## Use

While running `redis-server` (locally by default, remote is configurable), start the cluster entry point 
server with `prism-server` and start a worker with `prism-worker` to begin forwarding received blobs into the cluster. To add more workers just run more `prism-worker` processes.

To monitor the server, worker, and job status, run `prism-supervisor` while `prism-server` is running. The log file can be monitored with 
`tail -f ~/prism-server.log`. The workers and jobs can be managed using `rq` commands.
