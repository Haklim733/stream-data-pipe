import argparse
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
)
from pathlib import Path
import os
import sys


def main(model: str):
    """Download a Hugging Face model and tokenizer to the specified directory"""
    base_dir = os.getenv("MODEL_DIR", "tmp")
    model_dir = Path(base_dir).joinpath(model)
    model_dir.mkdir(parents=True, exist_ok=True)
    tokenizer = AutoTokenizer.from_pretrained(model)
    model = AutoModelForSequenceClassification.from_pretrained(model)

    # Save the model and tokenizer to the specified directory
    model.save_pretrained(model_dir.as_posix())
    tokenizer.save_pretrained(model_dir.as_posix())


if __name__ == "__main__":
    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.debug("This message will go to stdout")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model", dest="model", required=False, help="specify model to download"
    )
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    main(model=known_args.model)
