FROM python:3.8-slim

WORKDIR /app

RUN pip install -U scikit-learn pandas

COPY train.py ./train.py

ENTRYPOINT [ "python", "pre_processment.py" ]