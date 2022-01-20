import click
import pandas
import os.path

# Selects a hardcoded list of ids from a complete smithsonian dump and write them out as a partial file for unit testing
ids = [
    "http://n2t.net/ark:/65665/300008335-8d74-4c3f-873c-a9d8b4b3d6a8",
    "http://n2t.net/ark:/65665/3000094eb-c82e-4c82-88f2-25f2a9b40796",
    "http://n2t.net/ark:/65665/30000cb27-702b-4d34-ac24-3e46e14d5519",
    "http://n2t.net/ark:/65665/30000d403-f44f-498c-b7e3-ca1df52a2391",
    "http://n2t.net/ark:/65665/30002e5e4-91a3-4343-9519-2aab489dfbfd",
    "http://n2t.net/ark:/65665/300037033-1c48-460e-bc43-ce7fb3780a69",
    "http://n2t.net/ark:/65665/30003a155-444f-4add-9ec0-48bd2631237e",
    "http://n2t.net/ark:/65665/300042b39-2b9a-4df9-b27f-d47237261659",
    "http://n2t.net/ark:/65665/300047b21-f0c2-48f6-861d-a620466092e5",
    "http://n2t.net/ark:/65665/30004d383-9b25-4cfd-840d-a720361ec77e",
    "http://n2t.net/ark:/65665/30004d5b2-eeea-4b56-aaf7-750d6badd06e",
    "http://n2t.net/ark:/65665/300050eec-a1eb-45be-95d4-de0d73bd6f2b",
    "http://n2t.net/ark:/65665/300052e51-2b7d-4be7-9760-6316f2cef9c7",
    "http://n2t.net/ark:/65665/3000563bc-2e44-4139-8aeb-97857fedecde",
    "http://n2t.net/ark:/65665/30005b166-69c0-4e74-8ced-5bbb08af835a",
    "http://n2t.net/ark:/65665/300064c10-e25a-440c-8f54-e30447615244",
    "http://n2t.net/ark:/65665/30006cd83-36b3-4629-86db-f5a28307189f",
    "http://n2t.net/ark:/65665/3000745d7-3acf-4023-8de0-30a94e187672",
    "http://n2t.net/ark:/65665/300075a27-7020-4325-a598-ef40e7298f9f",
    "http://n2t.net/ark:/65665/3000768ed-fc68-446b-aa6d-f8bd437c796a",
]


@click.command()
@click.option(
    "-f",
    "--file",
    default=None,
    help="The path to the integrated Smithsonian unified.txt dump output",
)
@click.option(
    "-o",
    "--output",
    default=None,
    help="The path to the output file with the specified ids",
)
def main(file, output):
    unified_table = pandas.read_table(file, low_memory=False, dtype=object)
    selected_rows = unified_table[unified_table["id"].isin(ids)]
    print(f"selected rows are {selected_rows}")
    if os.path.exists(output):
        os.remove(output)
        print(f"removed previously existing output file at {output}")
    selected_rows.to_csv(output, sep="\t", index=False)
    print(f"wrote selected ids to {output}")


if __name__ == "__main__":
    main()
