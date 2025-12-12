# Importar Imagem do Python
FROM python:3.10.19

WORKDIR /syncdata/syncdata/processamento/

COPY . .

RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "extractor.py"]