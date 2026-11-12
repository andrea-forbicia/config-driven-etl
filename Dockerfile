# Run one dataset end to end; credentials are mounted, never baked in:
#
#   docker build -t config-driven-etl .
#   docker run --rm \
#     -e GCP_PROJECT_ID=<project> -e BQ_DATASET=<dataset> \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/adc.json \
#     -v <path to credentials json>:/secrets/adc.json:ro \
#     -v <host data dir>:/data \
#     config-driven-etl run datasets/<name>.yaml
#
# Dataset files under datasets/ are in the image; mount others and pass
# their path. Exit codes are listed by `--help`.
#
# The container runs as uid 10001, which must be able to write the
# mounted data directory. Pass settings with -e rather than --env-file
# .env: a DATA_DIR there would move the data out of /data.

FROM python:3.12-slim AS build
WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /wheels .

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels \
    && useradd --create-home --uid 10001 app \
    && mkdir /data /app \
    && chown app:app /data /app
WORKDIR /app
COPY datasets ./datasets
USER app
ENTRYPOINT ["python", "-m", "config_driven_etl"]
CMD ["--help"]
