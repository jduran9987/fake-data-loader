import json
import re
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, BotoCoreError, ClientError  # noqa: E501
import psycopg2
from psycopg2 import OperationalError
from psycopg2.errors import UndefinedTable

from ddl import PG_TABLES
from exceptions import EventFailedValidation
from logger import logger


class Target(ABC):
    """
    Abstract class for targets.
    """
    @abstractmethod
    def __init__(self, credentials: dict) -> None:
        """
        Constructur makes a connection to the target.
        """
        pass


class PostgresTarget(Target):
    """
    Postgres target.
    """
    def __init__(self, credentials: dict) -> None:
        """
        Connect to Postgres.
        """
        try:
            self.connection = psycopg2.connect(
                user=credentials["PG_USERNAME"],
                password=credentials["PG_PASSWORD"],
                dbname=credentials["PG_DATABASE"],
                host=credentials["PG_HOST"],
                port=credentials["PG_PORT"]
            )
            self.connection.set_session(autocommit=True)
            self.cursor = self.connection.cursor()
            logger.info("Connection to postgres established.")
        except OperationalError as err:
            logger.error(err)

    def create_tables(self, recreate: bool) -> None:
        """
        Create tables.
        """
        logger.info("Creating uuid extensions...")
        self.cursor.execute("""
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        """)

        if recreate:
            logger.info("Dropping existing tables...")
            for table in PG_TABLES:
                try:
                    self.cursor.execute(f"""
                        DROP TABLE {table} CASCADE;
                    """)
                except UndefinedTable as err:
                    logger.warning(f"Table not dropped. Table {table} does not exist")  # noqa: E501
                    logger.error(err)
                    continue

        for table, ddl in PG_TABLES.items():
            logger.info(f"Creating {table} if not exists...")
            self.cursor.execute(ddl)

    def _validate_user_update_demographic(self) -> dict | None:
        depedencies = {}

        self.cursor.execute("""
            SELECT id
            FROM users
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        id = results[0]

        self.cursor.execute(f"""
            SELECT state
            FROM users
            WHERE id = '{id}';
        """)

        state = self.cursor.fetchone()[0]

        depedencies["id"] = id
        depedencies["state"] = state

        return depedencies

    def _validate_user_application_open(self) -> dict | None:
        dependencies = {}

        self.cursor.execute("""
            SELECT id
            FROM users
            WHERE id NOT IN (
                SELECT user_id
                FROM applications
            )
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        user_id = results[0]
        dependencies["user_id"] = user_id

        return dependencies

    def _validate_user_application_reject(self) -> dict | None:
        dependencies = {}

        self.cursor.execute("""
            SELECT user_id
            FROM applications
            WHERE status = 'pending'
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        user_id = results[0]
        dependencies["user_id"] = user_id

        return dependencies

    def _validate_user_application_approve(self) -> dict | None:
        dependencies = {}

        self.cursor.execute("""
            SELECT user_id
            FROM applications
            WHERE status = 'pending'
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        user_id = results[0]
        dependencies["user_id"] = user_id

        return dependencies

    def _validate_user_deposit(self) -> dict | None:
        dependencies = {}

        self.cursor.execute("""
            SELECT user_id
            FROM balances
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        user_id = results[0]
        dependencies["user_id"] = user_id

        return dependencies

    def _validate_user_withdraw(self) -> dict | None:
        dependencies = {}

        self.cursor.execute("""
            SELECT user_id, amount
            FROM balances
            WHERE amount > 0
            ORDER BY RANDOM()
            LIMIT 1;
        """)

        results = self.cursor.fetchone()

        if not results:
            return None

        user_id = results[0]
        amount = results[1]
        dependencies["user_id"] = user_id
        dependencies["amount"] = amount

        return dependencies

    def validate_event(self, event: str) -> dict | EventFailedValidation:
        """
        Validate an event
        """
        if event == "user sign up":
            validation = True
        elif event == "user update demographic":
            validation = self._validate_user_update_demographic()
        elif event == "user application open":
            validation = self._validate_user_application_open()
        elif event == "user application reject":
            validation = self._validate_user_application_reject()
        elif event == "user application approve":
            validation = self._validate_user_application_approve()
        elif event == "user deposit":
            validation = self._validate_user_deposit()
        elif event == "user withdraw":
            validation = self._validate_user_withdraw()

        if not validation:
            raise EventFailedValidation(f"{event} failed validation.")

        return validation

    def _insert_user_signup(self, payload: dict) -> None:
        """
        Insert user signup row.
        """
        query = f"""
            INSERT INTO users (first_name, last_name, email, dob, state, modified_at, created_at)
                VALUES
                    (
                        '{payload["first_name"]}',
                        '{payload["last_name"]}',
                        '{payload["email"]}',
                        '{payload["dob"]}',
                        '{payload["state"]}',
                        '{payload["event_ts"]}',
                        '{payload["event_ts"]}'
                    );
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _update_user_update_demographic(self, payload: dict) -> None:
        """
        Insert user update demographic row.
        """
        query = f"""
            UPDATE users
            SET state = '{payload["state"]}', modified_at = '{payload["event_ts"]}'
            WHERE id = '{payload["id"]}';
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _insert_user_application_open(self, payload: dict) -> None:
        """
        Insert open application.
        """
        query = f"""
            INSERT INTO applications (user_id, status, modified_at, created_at)
                VALUES (
                    '{payload["user_id"]}',
                    '{payload["status"]}',
                    '{payload["event_ts"]}',
                    '{payload["event_ts"]}'
                );
        """
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _update_user_application_reject(self, payload: dict) -> None:
        """
        Update reject application.
        """
        query = f"""
            UPDATE applications
            SET status = '{payload["status"]}', modified_at = '{payload["event_ts"]}'
            WHERE user_id = '{payload["user_id"]}';
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _update_user_application_approve(self, payload: dict) -> None:
        """
        Update approve application.
        """
        query = f"""
            UPDATE applications
            SET status = '{payload["status"]}', modified_at = '{payload["event_ts"]}'
            WHERE user_id = '{payload["user_id"]}';
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

        query = f"""
            INSERT INTO balances (user_id, amount, modified_at, created_at)
                VALUES (
                    '{payload["user_id"]}',
                    0.00,
                    '{payload["event_ts"]}',
                    '{payload["event_ts"]}'
                );
        """
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _insert_user_deposit(self, payload: dict) -> None:
        """
        Insert user deposit.
        """
        query = f"""
            INSERT INTO deposits (user_id, amount, created_at)
                VALUES (
                    '{payload["user_id"]}',
                    {payload["amount"]},
                    '{payload["event_ts"]}'
                );
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

        query = f"""
            UPDATE balances
            SET amount = amount + {payload["amount"]}, modified_at = '{payload["event_ts"]}'
            WHERE user_id = '{payload["user_id"]}';
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def _insert_user_withdraw(self, payload: dict) -> None:
        """
        Insert user withdraw.
        """
        query = f"""
            INSERT INTO withdrawals (user_id, amount, created_at)
                VALUES (
                    '{payload["user_id"]}',
                    {payload["amount"]},
                    '{payload["event_ts"]}'
                );
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

        query = f"""
            UPDATE balances
            SET amount = amount - {payload["amount"]}, modified_at = '{payload["event_ts"]}'
            WHERE user_id = '{payload["user_id"]}';
        """  # noqa: E501
        logger.info(f"QUERY: {query}")

        self.cursor.execute(query)

    def insert_event(self, payload: dict) -> None | EventFailedValidation:
        event = payload["event"]

        if event == "user sign up":
            self._insert_user_signup(payload)
        elif event == "user update demographic":
            self._update_user_update_demographic(payload)
        elif event == "user application open":
            self._insert_user_application_open(payload)
        elif event == "user application reject":
            self._update_user_application_reject(payload)
        elif event == "user application approve":
            self._update_user_application_approve(payload)
        elif event == "user deposit":
            self._insert_user_deposit(payload)
        elif event == "user withdraw":
            self._insert_user_withdraw(payload)

    def close_connection(self) -> None:
        """
        Close connection.
        """
        self.cursor.close()
        self.connection.close()


class S3Target(Target):
    """
    S3 target.
    """
    def __init__(self, credentials: dict) -> None:
        """
        Connect to S3.
        """
        self.bucket_name = credentials["BUCKET_NAME"]
        try:
            session = boto3.Session(
                aws_access_key_id=credentials["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=credentials["AWS_SECRET_ACCESS_KEY"],
                region_name=credentials["AWS_REGION"]
            )
            self.client = session.client("s3")
            self.resource = session.resource("s3").Bucket(self.bucket_name)
            logger.info("S3 client and bucket resource created.")
        except (NoCredentialsError, PartialCredentialsError) as err:
            logger.error(err)

    def _generate_key(self, payload: dict) -> str:
        """
        Generate S3 key partitioned by `event_ts`.
        """
        event_ts = payload["event_ts"]
        filename = re.sub(r"[-:.]", "_", event_ts) + ".json"
        date_partition = (
            datetime
            .strptime(
                event_ts,
                "%Y-%m-%dT%H:%M:%S.%f"
            )
            .strftime("%Y/%m/%d")
        )
        key_path = f"events/{date_partition}/{filename}"

        return key_path

    def write_event(self, payload: dict) -> None:
        """
        Write event to S3 bucket.
        """
        key_path = self._generate_key(payload)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = tempfile.mktemp(
                dir=temp_dir,
                suffix=".json"
            )

            with open(temp_file_path, "w") as file:
                json.dump(payload, file)

            try:
                self.client.upload_file(
                    temp_file_path,
                    self.bucket_name,
                    key_path
                )
                logger.info(f"Successfully loaded event record {key_path}")
            except (BotoCoreError, ClientError) as err:
                logger.error(err)

    def empty_bucket(self) -> None:
        """
        Empty S3 bucket
        """
        self.resource.objects.all().delete()
        self.resource.object_versions.all().delete()
        logger.info(f"Emptied bucket: {self.bucket_name}")


class FirehoseTarget(Target):
    """Firehose target."""
    def __init__(self, credentials: dict) -> None:
        """Connect to firehose."""
        self.stream_name = credentials["STREAM_NAME"]
        self.firehose_target_bucket_name = credentials["FIREHOSE_TARGET_BUCKET_NAME"]  # noqa: E501
        try:
            session = boto3.Session(
                aws_access_key_id=credentials["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=credentials["AWS_SECRET_ACCESS_KEY"],
                region_name=credentials["AWS_REGION"]
            )
            self.firehose_client = session.client("firehose")
            self.s3_client = session.client("s3")
            self.s3_resource = session.resource("s3").Bucket(self.firehose_target_bucket_name)  # noqa: E501
            logger.info("Firehose client created.")
        except (NoCredentialsError, PartialCredentialsError) as err:
            logger.error(err)

    def write_event(self, payload: dict) -> None:
        """Write a record to Firehose."""
        data = json.dumps(payload) + "\n"

        try:
            response = self.firehose_client.put_record(
                DeliveryStreamName=self.stream_name,
                Record={
                    'Data': data.encode("utf-8")
                }
            )
            print(f"Record sent to Firehose: {response}")
        except (NoCredentialsError, PartialCredentialsError):
            print("Error: AWS credentials not found.")
        except Exception as e:
            print(f"Error sending record to Firehose: {e}")

    def empty_bucket(self) -> None:
        """Empty S3 bucket."""
        self.s3_resource.objects.all().delete()
        self.s3_resource.object_versions.all().delete()
        logger.info(f"Emptied bucket: {self.firehose_target_bucket_name}")
