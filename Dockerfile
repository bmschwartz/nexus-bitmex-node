# syntax=docker/dockerfile:1.0.0-experimental

## For this to work you must run `export DOCKER_BUILDKIT=1`
## then build using the command
##  docker build --ssh github_ssh_key=/Users/<your_username>/.ssh/id_rsa .


## Stage 1
FROM python:3.8-alpine AS base

RUN apk update && apk add --no-cache  \
  git \
  ca-certificates \
  openssh-client \
  postgresql-dev \
  gcc \
  libffi \
  libffi-dev \
  musl-dev

RUN pip install wheel

# download public key for github.com
RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

WORKDIR /app

RUN mkdir /wheels

# Set git links to be ssh instead of https, we will pass the SSH key in a SSH forward agent style
RUN git config --global url.ssh://git@github.com/.insteadOf https://github.com/
# On the docker client side, you need to define that SSH forwarding is allowed for this build by
# using the --ssh flag.
# docker build --ssh github_ssh_key=path/to_github_ssh_key .
# RUN --mount=type=ssh,id=github_ssh_key git clone git@github.com/Lightricks/receipt-hacker.git /app
ADD . /app

RUN --mount=type=ssh,id=github_ssh_key pip wheel \
  --no-cache \
  --requirement requirements.txt \
  --wheel-dir=/app/wheels

# This small script is creating a local version of requirements.txt
# When we create wheels, we ususally install them with pip by instructing pip not to look for the requirements 
# on Pypi using the `--no-=index` flag, if we have a requirement that is not hosted on Pypi (for example github.com) 
# pip will still try to get it directly from the given link
# this script renames the packages with git or egg in them into the package name that will be found in the /wheel folder
RUN printf 'local_dependencies = [] \n\
with open("requirements.txt", "r") as dependencies_file: \n\
    for dependency in dependencies_file: \n\
        if dependency: \n\
            pkg_name = dependency \n\
            \n\
            if "egg=" in dependency:\n\
                # git://github.com/mozilla/elasticutils.git#egg=elasticutils\n\
                pkg_name = dependency.split("egg=")[-1]\n\
            \n\
            if "git+" in dependency:\n\
                # git+https://github.com/Repo/some-package.git@1.3.3\n\
                pkg_name = dependency.split("/")[-1].split(".")[0]\n\
            \n\
            local_dependencies.append(pkg_name)\n\
            \n\
with open("wheel-requirements.txt", "w") as requirements_file:\n\
    # filter is used to remove empty list members (None).\n\
    requirements_file.write("\\n".join(filter(None, local_dependencies)))'\
>> /app/localize_requirements.py

RUN python /app/localize_requirements.py

# Stage 2
FROM python:3.8-alpine

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN apk update && apk add --no-cache \
  ca-certificates \
  git \
  openssh \
  libpq

COPY --from=base /app /app

WORKDIR /app

RUN pip install -r wheel-requirements.txt --no-cache --no-index --find-links=/app/wheels

# Cleanup
RUN find / -type f \( -name "*.pyx" -o -name "*.pyd"  -o -name "*.whl" \) -delete && \
  find /usr/local/lib/python3.8  -type f \( -name "*.c" -o -name "*.pxd"  -o -name "*.pyd" -o -name "__pycache__" \) -delete && \
  rm wheel-requirements.txt && \
  rm -rf /app/wheels

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "-m", "nexus_bitmex_node", "start"]
