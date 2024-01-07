ARG ALT_REPO_PYPI=pypi.altono.app
FROM python:3.9-slim as base
ARG ALT_REPO_PYPI
WORKDIR /code
COPY requirements.txt requirements.txt
RUN apt-get update
RUN apt-get -y install gcc
RUN pip install -r requirements.txt --extra-index-url=https://${ALT_REPO_PYPI} --trusted-host=${ALT_REPO_PYPI} --use-deprecated=legacy-resolver
EXPOSE 8022
COPY . .
ENTRYPOINT [ "./scripts/start.sh" ]
CMD [ "api" ]
#CMD [ "./scripts/start.sh" ]

