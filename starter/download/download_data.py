#!/usr/bin/env python
import argparse
import logging
import pathlib
import wandb
import requests
import tempfile
import os
import shutil
import tempfile
import io
import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    # Derive the base name of the file from the URL
    basename = pathlib.Path(args.file_url).name.split("?")[0].split("#")[0]

    # Download file, streaming so we can download files larger than
    # the available memory. We use a named temporary file that gets
    # destroyed at the end of the context, so we don't leave anything
    # behind and the file gets removed even in case of errors
    logger.info(f"Downloading {args.file_url} ...")
    buffer = io.BytesIO()

    # Download the file streaming and write to the buffer
    with requests.get(args.file_url, stream=True) as r:
        for chunk in r.iter_content(chunk_size=8192):
            buffer.write(chunk)

    # Reset the buffer position to the beginning
    buffer.seek(0)

    # Create a temporary file and write the buffer contents to it
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(buffer.getvalue())

    temp_file_path = temp_file.name

    logger.info("Creating run")
    with wandb.init(job_type="download_data") as run:
        logger.info("Creating artifact")
        artifact = wandb.Artifact(
            name=args.artifact_name,
            type=args.artifact_type,
            description=args.artifact_description,
            metadata={'original_url': args.file_url}
        )

        # Add the temporary file to the artifact
        artifact.add_file(temp_file_path, name=basename)

        logger.info("Logging artifact")
        run.log_artifact(artifact)

        artifact.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download a file and upload it as an artifact to W&B", fromfile_prefix_chars="@"
    )

    parser.add_argument(
        "--file_url", type=str, help="URL to the input file", required=True
    )

    parser.add_argument(
        "--artifact_name", type=str, help="Name for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_type", type=str, help="Type for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_description",
        type=str,
        help="Description for the artifact",
        required=True,
    )

    args = parser.parse_args()

    go(args)
