from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
import json
import logging
import random
import time
import typing
import uuid


from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import (
    KafkaError,
    UnknownTopicOrPartitionError,
    TopicAlreadyExistsError,
)

logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.admin = self.create_admin()
        self.producer = self.create_producer()

    def create_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: json.dumps(v).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=100,
            request_timeout_ms=3000,  # 30 sec
        )

    def delete_topics(self, topic_names: typing.List[str]):
        for name in topic_names:
            try:
                self.admin_client.delete_topics([name], timeout_ms=1000)
            except UnknownTopicOrPartitionError:
                pass
            except Exception as err:
                raise RuntimeError(f"fails to delete topic - {name}") from err

    def create_topics(self, topics: typing.List[NewTopic], to_recreate: bool = True):
        if to_recreate:
            self.delete_topics([t.name for t in topics])
        for topic in topics:
            try:
                resp = self.admin_client.create_topics([topic])
                name, error_code, error_message = resp.topic_errors[0]
                logger.info(
                    f"topic created, name - {name}, error code - {error_code}, error message - {error_message}"
                )
            except TopicAlreadyExistsError:
                pass
            except KafkaError as err:
                raise RuntimeError(
                    f"fails to create topics - {', '.join(t.name for t in topics)}"
                ) from err
        logging.info(
            f"topics created successfully - {', '.join([t.name for t in topics])}"
        )

    def send_items(self, topic_name: str, item: dict[str, typing.Any]):
        try:
            self.producer_client.send(
                topic=topic_name,
                value=item,
            )
            logging.info(f"record sent, topic - {topic_name}, ref - {key['ref']}")
        except Exception as err:
            raise RuntimeError("fails to send a message") from err


class IoTTelemetry(ABC):
    def __init__(
        self,
        max_time: int,
    ):
        self.max_time: int = max_time

    @abstractmethod
    def generate(self):
        raise NotImplementedError


class IoTDevice(ABC):
    def __init__(self, max_time: int, datatype: IoTTelemetry, client: KafkaClient):
        self.max_time: int = max_time
        self.datatype = datatype
        self.client = client

    @abstractmethod
    def generate(self):
        raise NotImplementedError


class DroneTelemetry(IoTTelemetry):
    def __init__(self):
        self.device: str = uuid.uuid4()
        self.timestamp: int = datetime.datetime.now().timestamp()
        self.location: Location = Location()
        self.altitude: float = round(random.uniform(0, 1000), 1)
        self.speed: float = round(random.uniform(3, 60), 1)
        self.batteryVoltage: int = random.randint(15, 100)

    def generate(self) -> dict[str, typing.Any]:
        """
        Generates a DroneTelemetry instance with random values for each field.
        """
        telemetry_data = {}
        for key, value in self.__dict__.items():
            if not key.startswith("_") and key not in ["device", "location"]:
                telemetry_data[key] = self._generate_random_value(key, value)
        telemetry_data["device"] = self.device
        telemetry_data["location"] = self.location.generate()
        return telemetry_data

    def _generate_random_value(
        self, field_name: str, field_value: typing.Any
    ) -> typing.Any:
        if field_name == "location":
            return Location.generate()
        elif field_name == "timestamp":
            timestamp = datetime.datetime.now().timestamp()
            self.timestamp = timestamp
            return timestamp
        elif field_name == "altitude":
            altitude = round(self.altitude + random.uniform(0.0, 1.0), 2)
            self.altitude = altitude
            return altitude
        elif field_name == "speed":
            speed = self.speed + round(random.uniform(0.0, 1.0), 2)  # meters
            self.speed = speed
            return speed
        else:
            return field_value


class Drone(IoTDevice):
    def __init__(self, max_time: int, data_type: DroneTelemetry, client: KafkaClient):
        self.max_time: int = max_time
        self.data_type = data_type
        self.client = client

    def generate(self):
        start_time = time.time()
        while time.time() - start_time < self.max_time:
            data = self.data_type.generate()
            print(data)
            self.client.send_items(topic_name="drone", item=data)
            # Sleep for a random interval between 0.5 to 2 seconds
            sleep_time = random.uniform(0.2, 2.0)
            time.sleep(sleep_time)


class Location:
    def __init__(self):
        self.latitude: int = 34.1184
        self.longitude: int = -118.3004

    def offset_coordinates_by_meter(self, seconds=1):
        # Approximate values for 1 meter in degrees
        meter_in_degrees_lat = 0.000009
        meter_in_degrees_lon = 0.000009
        offset_lat = meter_in_degrees_lat * seconds
        offset_lon = meter_in_degrees_lon * seconds
        new_lat = self.latitude + offset_lat
        new_lon = self.longitude + offset_lon
        return new_lat, new_lon

    def generate(self) -> dict[str, float]:
        lat, lon = self.offset_coordinates_by_meter()
        self.latitude, self.longitude = lat, lon
        return {"latitude": lat, "longitude": lon}


@dataclass
class PathResults:
    paths: list[Location]
    totalDistance: int


@dataclass
class TelemetryResults:
    totalTime: int
    totalDistance: int


if __name__ == "__main__":
    from unittest.mock import MagicMock

    client = MagicMock()
    device = Drone(max_time=2, data_type=DroneTelemetry(), client=client)
    device.generate()
