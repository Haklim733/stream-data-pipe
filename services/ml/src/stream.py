from enum import Enum
import logging
import os
from pathlib import Path
import sys
from typing import TypeVar, Generic

from pydantic import BaseModel
import torch

from faststream import Context, ContextRepo, FastStream
from faststream.confluent import KafkaBroker

from transformers import AutoTokenizer, AutoModel
from transformers import RobertaTokenizer, RobertaForSequenceClassification
from faststream import FastStream

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:19092")
CONSUMER_TOPIC = os.getenv("KAFKA_CONSUMER_TOPIC", "none")
PRODUCER_TOPIC = os.getenv("KAFKA_PRODUCER_TOPIC", "none")
KAFKA_SESSION_TIMEOUT_MS = os.getenv("KAFKA_SESSION_TIMEOUT_MS", 10000)
KAFKA_HEARTBEAT_INTERVAL_MS = os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", 1000)
KAFKA_MAX_POLL_INTERVAL_MS = os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", 10000)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)

broker = KafkaBroker(BOOTSTRAP_SERVERS)
app = FastStream(broker, logger=logger)


ModelType = TypeVar("ModelType", bound=AutoModel)
TokenizerType = TypeVar("TokenizerType", bound=AutoTokenizer)


class MlModel(Generic[ModelType, TokenizerType]):
    model: ModelType
    tokenizer: TokenizerType
    device: torch.cuda

    def __init__(
        self, model: ModelType, tokenizer: TokenizerType, device: torch.device
    ):
        self.model = model
        self.tokenizer = tokenizer
        self.device = device
        self.model.to(device)


class Message(BaseModel):
    id: str
    created_at: int
    message: str


class Response(Message):
    emotion: str


class ModelName(Enum):
    BERT = "distilbert-base-uncased-finetuned-sst-2-english"
    ROBERTA = "j-hartmann/emotion-english-distilroberta-base"


@app.on_startup
async def load_model(
    context: ContextRepo,
):
    model_name = "j-hartmann/emotion-english-distilroberta-base"
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    base_dir = os.getenv("MODEL_DIR", "tmp")
    model_dir = Path(base_dir).joinpath(model_name)
    tokenizer = RobertaTokenizer.from_pretrained(model_dir)
    model = RobertaForSequenceClassification.from_pretrained(model_dir)
    model = MlModel(model=model, tokenizer=tokenizer, device=device)
    context.set_global("model", model)
    await broker.connect()


@app.on_shutdown
async def shutdown_model(model: dict = Context()):
    model.model.device.empty_cache()
    model.model.tokenizer.close()
    del model.model


def main(input_text: str, model: MlModel) -> str:
    encoded_input = model.tokenizer(
        input_text, return_tensors="pt", padding=True, truncation=True
    )
    encoded_input = {
        key: value.to(model.device) for key, value in encoded_input.items()
    }
    output = model.model(**encoded_input)
    logits = output.logits.to(model.model.device)
    label = torch.argmax(logits, dim=1).to(model.model.device).item()
    emotions = ["anger", "disgust", "fear", "joy", "neutral", "sadness", "surprise"]
    emotion = emotions[label]
    return emotion


@broker.subscriber(
    CONSUMER_TOPIC,
    auto_offset_reset="earliest",
    max_workers=2,
    isolation_level="read_committed",
)
@broker.publisher(
    PRODUCER_TOPIC,
    title="emotions",
    description="sentiment analysis on line of text",
    schema=Response,
    include_in_schema=False,
    batch=False,
)
def predict(
    id: str, created_at: int, message: str, model=Context()
) -> dict[str, int | str]:
    emotion = main(message, model)
    return {
        "id": id,
        "created_at": created_at,
        "message": message,
        "emotion": emotion,
    }


if __name__ == "__main__":
    from unittest.mock import MagicMock

    predict(
        Message(
            message="I love programming!",
            id="1",
            created_at=1,
            text="I love programming!",
        ),
        model=MagicMock(),
    )
