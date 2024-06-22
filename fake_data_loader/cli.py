import time
from pathlib import Path

import click

from auth_handler import AuthHandler
from event_generator import EventGenerator
from exceptions import EventFailedValidation
from logger import logger
from targets import PostgresTarget, S3Target


@click.group()
@click.pass_context
def cli(ctx: dict) -> None:
    """CLI tool to manage database tables."""
    ctx.ensure_object(dict)


@cli.command()
@click.option(
    "--config-path",
    "-c",
    required=True,
    help="Path of the config file containing the postgres credentials."
)
@click.option(
    "--recreate",
    "-r",
    is_flag=True,
    default=False,
    help="Flag to recreate the tables before starting the stream."
)
@click.option(
    "--event-lag",
    "-e",
    required=False,
    default="1",
    help="Time in seconds to wait between generating events."
)
@click.option(
    "--duration",
    "-d",
    required=False,
    default="60",
    help="Time in seconds to run the stream."
)
@click.pass_context
def start_stream(
    ctx: dict,
    config_path: str,
    recreate: bool,
    event_lag: str,
    duration: str
) -> None:
    """
    Start streaming events to a target.
    """
    time_start = time.time()

    target_credentials = AuthHandler().convert_to_dict(Path(config_path))
    postgres_target = PostgresTarget(target_credentials)
    s3_target = S3Target(target_credentials)
    event_generator = EventGenerator()

    postgres_target.create_tables(recreate=recreate)
    if recreate:
        s3_target.empty_bucket()

    while time.time() - time_start < int(duration):
        event = event_generator.get_event()
        logger.info(f"GENERATED EVENT: {event}")

        try:
            validation = postgres_target.validate_event(event)
        except EventFailedValidation as err:
            logger.error(err)
            continue

        payload = event_generator.generate_event_payload(event, validation)
        logger.info(f"PAYLOAD: {payload}")
        postgres_target.insert_event(payload)
        s3_target.write_event(payload)

        time.sleep(float(event_lag))

    postgres_target.close_connection()

    return


if __name__ == "__main__":
    cli(obj={})
