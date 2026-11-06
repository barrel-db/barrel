# Ivy ships a Python 3 wheel for Linux x86_64 only.
FROM --platform=linux/amd64 python:3.10-slim
RUN apt-get update && apt-get install -y --no-install-recommends tk libgmp10 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir ms-ivy==1.8.26
WORKDIR /proofs
ENTRYPOINT ["ivy_check"]
