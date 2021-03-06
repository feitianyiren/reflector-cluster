import sys
import os
import click
import psutil
from redis import Redis
from redis.exceptions import ConnectionError
from rq import Connection
from rq.cli.cli import main as cli_main, show_queues, show_workers, refresh, pass_cli_config

from prism.config import get_settings

settings = get_settings()
BLOB_DIR = settings['blob directory']
REDIS_ADDRESS = settings['redis server']
HOSTS = settings['hosts']
redis_conn = Redis(REDIS_ADDRESS)
server_proc = [proc for proc in psutil.process_iter() if proc.name() == "prism-server"][0]


def show_cluster_info():
    blob_count = redis_conn.hlen("blob_hashes")
    cluster_blob_count = redis_conn.scard("cluster_blobs")

    click.echo('')
    click.echo('%i blobs completed, %i blobs in cluster' % (blob_count, cluster_blob_count))
    for host in HOSTS:
        host_blobs = redis_conn.scard(host)
        click.echo('%s - %i blobs' % (host, host_blobs))


def show_prism_info(queues, raw, by_queue, queue_class, worker_class):
    local_blobs = len(os.listdir(os.path.expandvars(BLOB_DIR)))
    show_queues(queues, raw, by_queue, queue_class, worker_class)
    if not raw:
        click.echo('')
    show_workers(queues, raw, by_queue, queue_class, worker_class)
    show_cluster_info()
    click.echo('')
    click.echo("Redis clients: %i" % len(redis_conn.client_list()))
    click.echo("Local blobs: %i" % local_blobs)

    try:
        click.echo("Open files: %i" % len(server_proc.open_files()))
    except psutil.NoSuchProcess:
        sys.exit(0)
    if not raw:
        click.echo('')
        import datetime
        click.echo('Updated: %s' % datetime.datetime.now())


@cli_main.command()
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
@pass_cli_config
def main(cli_config, raw, by_queue, queues, **options):
    """RQ command-line monitor."""
    try:
        with Connection(redis_conn):
            refresh(0.1, show_prism_info, queues, raw, by_queue,
                    cli_config.queue_class, cli_config.worker_class)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)
