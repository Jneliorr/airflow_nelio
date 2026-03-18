# Usa a imagem oficial do Airflow 3.0 como base
FROM apache/airflow:3.1.7

USER root

# Instala dependências do sistema e o Google Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instala o Selenium e o webdriver-manager para facilitar a gestão do driver
RUN pip install --no-cache-dir selenium webdriver-manager