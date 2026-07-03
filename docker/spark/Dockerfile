FROM apache/spark:4.0.1

WORKDIR /app

ENV PYTHONPATH=/app

COPY . .

ENV HOME=/app
ENV PATH="/opt/spark/bin:${PATH}"
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /tmp/.ivy2

COPY docker/spark/packages.txt /tmp/packages.txt

RUN PACKAGES=$(paste -sd, /tmp/packages.txt) && \
    spark-shell --packages "$PACKAGES" --version