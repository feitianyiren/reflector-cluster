### Installation

`pip install -r requirements.txt; pip install -e .`

### How to use it

While running `redis-server` (locally by default, remote is configurable), start the cluster entry point 
server with `prism-server` and start a worker with `prism-worker`. To add more workers run more `prism-worker` processes.

To monitor the server status, run `prism-supervisor` while `prism-server` is running.

### Configuration

The data directory (used as a blob cache while awaiting forwarding) defaults to `~/.prism`. 
Hosts can be added by providing their details in `~/.prism.yml`.