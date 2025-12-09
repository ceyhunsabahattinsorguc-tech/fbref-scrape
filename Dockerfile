# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy csproj files and restore
COPY Fbref.Core/Fbref.Core.csproj Fbref.Core/
COPY Fbref.Infrastructure/Fbref.Infrastructure.csproj Fbref.Infrastructure/
COPY Fbref.Api/Fbref.Api.csproj Fbref.Api/
RUN dotnet restore Fbref.Api/Fbref.Api.csproj

# Copy everything and build
COPY . .
RUN dotnet publish Fbref.Api/Fbref.Api.csproj -c Release -o /app/publish

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

# Install Chrome/Chromium for PuppeteerSharp
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxkbcommon0 \
    xdg-utils \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set Puppeteer to use installed Chromium
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

COPY --from=build /app/publish .

EXPOSE 80
EXPOSE 443

ENTRYPOINT ["dotnet", "Fbref.Api.dll"]
