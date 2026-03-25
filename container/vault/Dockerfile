# Use a base image with necessary tools
FROM alpine:3.21

# Install required packages (curl and unzip)
RUN apk add --no-cache curl unzip

# Define the Vault version
ENV VAULT_VERSION=1.17.3

# Download and install Vault CLI
RUN curl -fsSL https://releases.hashicorp.com/vault/${VAULT_VERSION}/vault_${VAULT_VERSION}_linux_amd64.zip -o vault.zip \
    && unzip vault.zip \
    && mv vault /usr/local/bin/ \
    && chmod +x /usr/local/bin/vault \
    && rm vault.zip

# Verify installation
RUN vault --version

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the shell script into the container
COPY --chmod=644 script.sh /script.sh

# Make sure the script is executable
RUN chmod +x /script.sh

# Run the shell script when the container starts
CMD ["sh", "/script.sh"]