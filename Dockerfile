
FROM tiangolo/meinheld-gunicorn-flask:python3.7

RUN pip install meinheld gunicorn

COPY requirements.txt /app
RUN pip install -r /app/requirements.txt

COPY ./app /app