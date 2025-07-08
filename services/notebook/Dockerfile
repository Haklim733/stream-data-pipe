FROM python:3.12-slim

# Install only necessary system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HOME="/home/app"

WORKDIR ${HOME}

# Install Python dependencies
COPY requirements.txt ${HOME}/
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p ${HOME}/notebooks ${HOME}/data ${HOME}/warehouse ${HOME}/spark-events

# Copy notebooks
COPY notebooks/ ${HOME}/notebooks/

# Download sample data
RUN curl -o /home/app/data/brothers-karamazov.txt https://www.gutenberg.org/files/8576/8576-0.txt

# Copy configuration
COPY .pyiceberg.yaml /root/.pyiceberg.yaml
COPY ipython_kernel_config.py /etc/ipython/

# Create Jupyter configuration directory
RUN mkdir -p /home/app/.jupyter

# Create Jupyter configuration to fix authentication issues
RUN echo 'c.ServerApp.ip = "0.0.0.0"' > /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.port = 8888' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.allow_root = True' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.allow_origin = "*"' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.allow_remote_access = True' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.token = ""' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.password = ""' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.notebook_dir = "/home/app/notebooks"' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.trust_xheaders = True' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.open_browser = False' >> /home/app/.jupyter/jupyter_server_config.py && \
    echo 'c.ServerApp.allow_origin_pat = ".*"' >> /home/app/.jupyter/jupyter_server_config.py

# Trust all notebooks to avoid trust issues
RUN jupyter trust /home/app/notebooks/*.ipynb || true

# Create a simple entrypoint script
RUN echo '#!/bin/bash' > /entrypoint.sh \
    && echo 'set -e' >> /entrypoint.sh \
    && echo 'echo "Starting Jupyter Notebook..."' >> /entrypoint.sh \
    && echo 'echo "Trusting all notebooks..."' >> /entrypoint.sh \
    && echo 'jupyter trust /home/app/notebooks/*.ipynb || true' >> /entrypoint.sh \
    && echo 'exec "$@"' >> /entrypoint.sh \
    && chmod +x /entrypoint.sh

EXPOSE 8888

CMD ["jupyter", "notebook", "--notebook-dir=/home/app/notebooks", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=''"]