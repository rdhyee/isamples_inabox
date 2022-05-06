import sys
import click
import os


@click.command()
@click.option(
    "-s",
    "--source",
    help="The path to the sitemaps source directory where all the sitemaps by date live.",
)
@click.option(
    "-d",
    "--destination",
    help="The destination directory where the sitemaps symlink should live.",
)
def main(source, destination):
    if not os.path.exists(source):
        print("Sitemaps source directory doesn't exist.  Exiting.")
        sys.exit(-1)
    os.chdir(source)
    directories = filter(os.path.isdir, os.listdir(source))
    directories = [os.path.join(source, d) for d in directories]
    directories.sort(key=lambda x: os.path.getmtime(x))
    sitemaps_dir = os.path.join(directories[-1], "sitemaps")
    try:
        os.remove(destination)
    except OSError as e:
        # likely ok if it doesn't exist
        print(e)
    os.symlink(sitemaps_dir, destination)


if __name__ == "__main__":
    main()
