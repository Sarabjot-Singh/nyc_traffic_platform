FROM apache/spark:4.0.1

WORKDIR /opt/project

ENV HOME=/tmp
ENV PYTHONPATH=/opt/project
ENV PATH="/opt/spark/bin:${PATH}"

COPY . .
COPY requirements.txt .
COPY docker/spark/packages.txt /tmp/packages.txt

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /tmp/.ivy2

RUN PACKAGES=$(paste -sd, /tmp/packages.txt) && \
    spark-shell --packages "$PACKAGES" --version