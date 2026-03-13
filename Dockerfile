# ssh-parallel-test — lightweight image with SSH, rsync, and Docker CLI.
#
# Published to ghcr.io/ioqr/ssh-parallel-test:latest on every push to main.
#
# Usage:
#   docker run --rm \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v $(pwd):$(pwd) -v ~/.ssh:/root/.ssh:ro \
#     -v ~/.ssh-parallel-test:/root/.ssh-parallel-test \
#     --network host -w $(pwd) \
#     ghcr.io/ioqr/ssh-parallel-test:latest -c config.yml run

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl openssh-client rsync sshpass make \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyyaml

COPY spt.py /usr/local/bin/spt
RUN chmod +x /usr/local/bin/spt

ENTRYPOINT ["spt"]
