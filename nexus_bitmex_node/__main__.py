import sys
from os.path import abspath, join
import click

from nexus_bitmex_node import settings

sys.path.insert(0, abspath(join(__file__, "../", "../")))
sys.path.insert(0, abspath(join(__file__, "../")))


@click.group()
def cli():
    pass


@click.command()
@click.option("-h", "--host", "host", default=settings.HOST)
@click.option("-p", "--port", "port", default=settings.PORT)
def start(host, port):
    import uvicorn
    from nexus_bitmex_node import settings

    uvicorn.run(
        "nexus_bitmex_node.app:app",
        host=host,
        port=port,
        log_config=settings.LOGGING_CONFIG,
        reload=settings.SERVER_RELOAD,
    )


@click.command()
def run_tests(args=None):
    import pytest

    pytest.main(["."])


cli.add_command(start)
cli.add_command(run_tests)


if __name__ == "__main__":
    cli()
