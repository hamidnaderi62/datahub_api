FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8001

ENV PYTHONUNBUFFERED=1


CMD ["python", "manage.py", "runserver", "0.0.0.0:8001"]