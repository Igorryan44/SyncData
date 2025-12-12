# Importar Imagem do Python
FROM python:3.10.19

WORKDIR /syncdata/

COPY . .

RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "syncdata/processamento/extractor.py"]