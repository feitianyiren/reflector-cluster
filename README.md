### Installation

`pip install -r requirements.txt; pip install -e .`

### How to use it

While running `redis-server` and `rq-worker`, start the cluster entry point 
server with `prism-server` and run the worker with `prism-worker`.

### Configuration

The data directory (used as a blob cache while awaiting forwarding) defaults to `~/.prism`. 
Hosts can be added by providing their details in `~/.prism.yml`.