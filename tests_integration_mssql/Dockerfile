# Use the official Microsoft SQL Server image as a base
FROM mcr.microsoft.com/mssql/server:2022-latest

# Switch to root user to install dependencies
USER root

# Install MSSQL tools
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc > /dev/null && \
    curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list | tee /etc/apt/sources.list.d/mssql-release.list > /dev/null && \
    apt-get update && \
    apt-get install -y mssql-tools18 unixodbc-dev sqlcmd && \
    apt-get clean && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> /etc/profile.d/mssql.sh

# Set back to the default SQL Server user
USER mssql

# Start SQL Server
CMD ["/opt/mssql/bin/sqlservr"]
