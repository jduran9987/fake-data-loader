import datetime
import random

from faker import Faker


class EventGenerator:
    """
    Event generator.
    """
    def __init__(self) -> None:
        self.events = [
            "user sign up",
            "user update demographic",
            "user application open",
            "user application reject",
            "user application approve",
            "user deposit",
            "user withdraw"
        ]

    def get_event(self) -> str:
        weights = [35, 2, 17, 5, 13, 20, 8]
        event = random.choices(
            self.events,
            weights=weights,
            k=1
        )

        return event[0]

    def generate_event_payload(self, event: str, validation: dict) -> dict:
        fake = Faker()
        event_ts = (
            datetime
            .datetime
            .now(datetime.UTC)
            .replace(tzinfo=None)
            .isoformat(timespec="milliseconds")
        )

        if event == "user sign up":
            payload = {
                "event": event,
                "event_ts": event_ts,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "dob": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),  # noqa: E501
                "state": fake.state_abbr()
            }
        elif event == "user update demographic":
            while True:
                state = fake.state_abbr()
                if state != validation["state"]:
                    break

            payload = {
                "event": event,
                "event_ts": event_ts,
                "id": validation["id"],
                "state": state
            }
        elif event == "user application open":
            payload = {
                "event": event,
                "event_ts": event_ts,
                "user_id": validation["user_id"],
                "status": "pending"
            }
        elif event == "user application reject":
            payload = {
                "event": event,
                "event_ts": event_ts,
                "user_id": validation["user_id"],
                "status": "rejected"
            }
        elif event == "user application approve":
            payload = {
                "event": event,
                "event_ts": event_ts,
                "user_id": validation["user_id"],
                "status": "approved"
            }
        elif event == "user deposit":
            amount = random.randint(1, 100000) / 100

            payload = {
                "event": event,
                "event_ts": event_ts,
                "user_id": validation["user_id"],
                "amount": amount
            }
        elif event == "user withdraw":
            amount = random.randint(1, validation["amount"]*100) / 100

            payload = {
                "event": event,
                "event_ts": event_ts,
                "user_id": validation["user_id"],
                "amount": amount
            }

        return payload
