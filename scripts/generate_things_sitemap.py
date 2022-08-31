import click

import isb_web
import isb_lib.core
from isb_lib.sitemaps import build_sitemap
from isb_lib.sitemaps.thing_sitemap import (
    ThingSitemapIndexIterator
)


@click.command()
@click.option(
    "-p",
    "--path",
    type=str,
    default=None,
    help="The disk path where the sitemap files are written",
)
@click.option(
    "-h",
    "--host",
    type=str,
    default=None,
    help="The hostname to include in the sitemap file",
)
@click.pass_context
def main(ctx, path: str, host: str):
    isb_lib.core.things_main(
        ctx, None, isb_web.config.Settings().solr_url, "INFO", False
    )
    build_sitemap(path, host, ThingSitemapIndexIterator())


if __name__ == "__main__":
    main()
