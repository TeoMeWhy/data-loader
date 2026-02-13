# Use the official Python image from the Docker Hub
FROM python:3.13

# Install dependencies
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium-archive-keyring.gpg] https://packages.adoptium.net/artifactory/deb $(grep VERSION_CODENAME /etc/os-release | cut -d= -f2) main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    apt-get clean

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Jupyter and Nekt SDK
RUN pip install jupyter python-dotenv pandas sqlalchemy git+https://github.com/nektcom/nekt-sdk-py.git#egg=nekt-sdk

# Expose port 8888 for Jupyter Notebook
EXPOSE 8888

# Set the working directory
WORKDIR /workspace

# Command to run Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--no-browser", "--allow-root"]