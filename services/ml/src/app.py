# app.py
from enum import Enum
import json
import logging
import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ValidationError
import torch
from transformers import RobertaTokenizer, RobertaForSequenceClassification

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)


class EmotionRequest(BaseModel):
    text: str


class EmotionResponse(BaseModel):
    emotion: str
    text: str


class ModelName(Enum):
    BERT = "distilbert-base-uncased-finetuned-sst-2-english"
    ROBERTA = "j-hartmann/emotion-english-distilroberta-base"


model_name = "j-hartmann/emotion-english-distilroberta-base"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

base_dir = os.getenv("MODEL_DIR", "tmp")
model_dir = Path(base_dir).joinpath(model_name)
tokenizer = RobertaTokenizer.from_pretrained(model_dir)
model = RobertaForSequenceClassification.from_pretrained(model_dir)
model.to(device)

app = FastAPI()


def main(input_text: str) -> str:
    encoded_input = tokenizer(
        input_text, return_tensors="pt", padding=True, truncation=True
    )
    encoded_input = {key: value.to(device) for key, value in encoded_input.items()}
    output = model(**encoded_input)
    logits = output.logits.to(device)
    label = torch.argmax(logits, dim=1).to(device).item()
    emotions = ["anger", "disgust", "fear", "joy", "neutral", "sadness", "surprise"]
    emotion = emotions[label]
    return emotion


@app.post("/emotions", response_model=EmotionResponse)
def predict(request: Request) -> EmotionRequest:
    try:
        print(request)
        data = json.loads(request.body())
        emotion_request = EmotionRequest(**data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        emotion = main(emotion_request.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    logger.info(emotion)
    return EmotionResponse(**{"emotion": emotion, "text": emotion_request.text}).dict()


if __name__ == "__main__":
    text = "I love programming!"
    result = main(text)
    print(result)
