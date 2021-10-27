import click
import pandas
import os.path
import sys

# The Smithsonian files are currently exported in a Darwin Core Archive, and manually downloaded from
# https://collections.nmnh.si.edu/ipt/resource?r=nmnh_material_sample.  To import them, we should glom the relevant
# fields together then export that manually before importing using smithsonian_things.py.

OCCURRENCE_FILE_NAME = "occurrence.txt"
PREPARATION_FILE_NAME = "preparation.txt"
MATERIAL_FILE_NAME = "materialsample.txt"

OUTPUT_FILE_NAME = "unified.txt"


@click.command()
@click.option(
    "-d",
    "--directory",
    default=None,
    help="The path to the Smithsonian Darwin Core archive directory",
)
def main(directory):
    # Verify the directory exists
    if not os.path.exists(directory):
        sys.exit(f"Darwin Core Archive Directory doesn't exist at {directory}")
    occurence_file = os.path.join(directory, OCCURRENCE_FILE_NAME)
    if not os.path.exists(occurence_file):
        sys.exit(f"{occurence_file} doesn't exist")
    preparation_file = os.path.join(directory, PREPARATION_FILE_NAME)
    if not os.path.exists(preparation_file):
        sys.exit(f"{preparation_file} doesn't exist")
    material_file = os.path.join(directory, MATERIAL_FILE_NAME)
    if not os.path.exists(material_file):
        sys.exit(f"{material_file} doesn't exist")

    occurrences = pandas.read_table(occurence_file, low_memory=False, index_col="id", dtype=object)
    preparations = pandas.read_table(preparation_file, index_col="id", dtype=object)
    materials = pandas.read_table(material_file, index_col="id", dtype=object)

    occurrences = occurrences.merge(
        preparations, how="left", left_on="id", right_on="id", suffixes=(False, False)
    )
    occurrences = occurrences.merge(
        materials, how="left", left_on="id", right_on="id", suffixes=(False, False)
    )

    outpath = os.path.join(directory, OUTPUT_FILE_NAME)
    if os.path.exists(outpath):
        os.remove(outpath)
        print(f"removed previously existing output file at {outpath}")
    occurrences.to_csv(outpath, "\t")
    print(
        f"Wrote output file to {outpath}.  You can import this file using 'smithsonian_things.py load -f {outpath}'. Thank you, have a nice day."
    )


if __name__ == "__main__":
    main()
