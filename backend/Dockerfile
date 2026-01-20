FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set up plugins directory
ENV CHIMERA_LOCAL_PLUGINS=/app/plugins
ENV CHIMERA_PLUGINS_BUCKET=betfair-chimera-plugins
ENV CHIMERA_DEFAULT_PLUGIN=betfair

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
