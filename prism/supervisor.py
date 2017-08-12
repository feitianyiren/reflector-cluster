import sys
import click
from redis import Redis, ConnectionError
from rq import Connection
from rq.cli.cli import main as cli_main, show_queues, show_workers, refresh
from rq.cli.cli import pass_cli_config

from prism.config import get_settings
settings = get_settings()
HOSTS = settings['hosts']

redis_conn = Redis()


def show_cluster_info():
    blob_count = redis_conn.hlen("blob_hashes")
    cluster_blob_count = redis_conn.scard("cluster_blobs")

    click.echo('')
    click.echo('%i blobs completed, %i blobs in cluster' % (blob_count, cluster_blob_count))
    for host in HOSTS:
        host_blobs = redis_conn.scard(host)
        click.echo('%s - %i blobs' % (host, host_blobs))


def show_prism_info(queues, raw, by_queue, queue_class, worker_class):
    show_queues(queues, raw, by_queue, queue_class, worker_class)
    if not raw:
        click.echo('')
    show_workers(queues, raw, by_queue, queue_class, worker_class)
    show_cluster_info()
    if not raw:
        click.echo('')
        import datetime
        click.echo('Updated: %s' % datetime.datetime.now())


@cli_main.command()
@click.option('--path', '-P', default='.', help='Specify the import path.')
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
@pass_cli_config
def main(cli_config, raw, by_queue, queues, **options):
    """RQ command-line monitor."""

    try:
        with Connection(cli_config.connection):
            refresh(0.1, show_prism_info, queues, raw, by_queue,
                    cli_config.queue_class, cli_config.worker_class)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)
