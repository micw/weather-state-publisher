FROM python:3.13-alpine

RUN addgroup -S publisher && adduser -S -G publisher publisher

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --requirement requirements.txt

COPY --chown=publisher:publisher publisher.py ./

USER publisher

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)"

CMD ["python", "-u", "/app/publisher.py"]
