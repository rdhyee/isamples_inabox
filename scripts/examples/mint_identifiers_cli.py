import requests
import click
import json


@click.command()
@click.option(
    "-t",
    "--identity_token",
    type=str,
    default=None,
    help="The identifier token",
)
@click.option(
    "-u",
    "--url",
    type=str,
    default="https://mars.cyverse.org/isamples_central/manage/mint_draft_identifiers",
    help="The url to the mint identifiers endpoint",
)
@click.option(
    "-f",
    "--file",
    type=str,
    help="The path to the metadata JSON file",
)
@click.option(
    "-n",
    "--num_identifiers",
    type=str,
    help="The number of identifiers to create",
)
def main(identity_token: str, url: str, file: str, num_identifiers: int):
    session = requests.session()
    with open(file) as json_file:
        data = json.load(json_file)
        post_data = {"num_drafts": num_identifiers, "datacite_metadata": data}
        post_bytes = json.dumps(post_data).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {identity_token}",
        }
        print(f"Attempting to post to {url} to mint an identifier…")
        response = session.post(url, post_bytes, headers=headers)
        if response.status_code == 200:
            print(f"Success, response is {response.json()}")
        else:
            print(f"Error response: {response}")


if __name__ == "__main__":
    main()
