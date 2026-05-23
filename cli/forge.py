import os
import sys
import json
import click
import requests


CONFIG_DIR = os.path.expanduser("~/.forge")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")


def save_config(data):
    """Save credentials to ~/.forge/config.json"""
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)
    os.chmod(CONFIG_FILE, 0o600)


def load_config():
    """Load credentials from ~/.forge/config.json"""
    if not os.path.exists(CONFIG_FILE):
        click.echo("Not logged in. Run: forge login <url>")
        sys.exit(1)
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


@click.group()
def cli():
    """Forge — CI/CD platform with integrated artifact registry."""
    pass


@cli.command()
@click.argument("url")
def login(url):
    """Authenticate with a Forge server."""
    url = url.rstrip("/")
    token = click.prompt("Enter your auth token", hide_input=True)

    # Verify the token works by hitting the server
    try:
        resp = requests.get(
            f"{url}/artifacts",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code == 401:
            click.echo("Error: invalid token.")
            sys.exit(1)
    except requests.ConnectionError:
        click.echo(f"Error: could not connect to {url}")
        sys.exit(1)

    save_config({"url": url, "token": token})
    click.echo(f"Logged in to {url}")


@cli.command()
@click.argument("pipeline", type=click.Path(exists=True))
def run(pipeline):
    """Submit a pipeline for execution."""
    # Person 5 will implement this
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("run_id")
@click.option("--follow", is_flag=True, help="Stream logs live")
def logs(run_id, follow):
    """Fetch logs for a pipeline run."""
    # Person 5 will implement this
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("path", type=click.Path(exists=True))
@click.option("--name", required=True, help="Artifact name")
@click.option("--version", required=True, help="Artifact version")
def publish(path, name, version):
    """Publish an artifact to the registry."""
    # Person 5 will implement this
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("pipeline", type=click.Path(exists=True))
def resolve(pipeline):
    """Resolve dependencies and print the lockfile."""
    # Person 5 will implement this
    click.echo("Not implemented yet.")


@cli.command()
@click.argument("package")
def ls(package):
    """List versions of a package."""
    # Person 5 will implement this
    click.echo("Not implemented yet.")


if __name__ == "__main__":
    cli()
