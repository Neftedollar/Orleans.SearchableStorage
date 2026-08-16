ARG DOTNET_SDK_IMAGE
ARG DOTNET_ASPNET_IMAGE

FROM ${DOTNET_SDK_IMAGE} AS build
# MCR ships no 10.0.303 SDK image while global.json requires exactly 10.0.303
# with roll-forward disabled. The pinned base image supplies the OS layer; the
# exact SDK comes from the official release tarball, checksum-pinned here and
# published with the same sha512 in the .NET 10.0 release metadata.
ADD --checksum=sha256:ec0833a374ccd6c4baf32600e3348d96b3a9499b6c7e518d4bf46fb385c6a4fd \
    https://builds.dotnet.microsoft.com/dotnet/Sdk/10.0.303/dotnet-sdk-10.0.303-linux-x64.tar.gz \
    /tmp/dotnet-sdk-10.0.303.tar.gz
RUN install -d -m 0755 /usr/share/dotnet-10.0.303 \
    && tar -xzf /tmp/dotnet-sdk-10.0.303.tar.gz -C /usr/share/dotnet-10.0.303 \
    && rm /tmp/dotnet-sdk-10.0.303.tar.gz
ENV DOTNET_ROOT=/usr/share/dotnet-10.0.303 \
    PATH=/usr/share/dotnet-10.0.303:${PATH}
WORKDIR /src

COPY global.json NuGet.Config Directory.Build.props Directory.Packages.props \
     Orleans.SearchableStorage.Qualification.slnx ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg \
     ./lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg
COPY deploy/hetzner-cx53/app-entrypoint.sh ./deploy/hetzner-cx53/app-entrypoint.sh

RUN test "$(dotnet --version)" = "10.0.303" \
    && dotnet --list-runtimes | grep -Eq '^Microsoft\.NETCore\.App 10\.0\.11 ' \
    && dotnet --list-runtimes | grep -Eq '^Microsoft\.AspNetCore\.App 10\.0\.11 ' \
    && printf '%s  %s\n' \
       d9c05681a0866f027d394843089d6534d06d151f18f611dce3f1e7b5f1e9331c \
       lock/package-source/Orleans.SearchableStorage.1.0.0-rc.2.nupkg \
       | sha256sum --check --strict \
    && dotnet restore Orleans.SearchableStorage.Qualification.slnx \
       --locked-mode --configfile NuGet.Config \
    && dotnet build Orleans.SearchableStorage.Qualification.slnx \
       -c Release --no-restore \
    && dotnet test Orleans.SearchableStorage.Qualification.slnx \
       -c Release --no-build --no-restore \
    && dotnet publish \
       src/Orleans.SearchableStorage.Qualification.SkyPulse.Web/Orleans.SearchableStorage.Qualification.SkyPulse.Web.csproj \
       -c Release --no-build --no-restore --no-self-contained \
       -p:UseAppHost=false -o /out \
    && dotnet publish \
       src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusBuilder.csproj \
       -c Release --no-build --no-restore --no-self-contained \
       -p:UseAppHost=false -o /tools/corpus-builder \
    && dotnet publish \
       src/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition/Orleans.SearchableStorage.Qualification.SkyPulse.CorpusAcquisition.csproj \
       -c Release --no-build --no-restore --no-self-contained \
       -p:UseAppHost=false -o /tools/corpus-acquisition

FROM ${DOTNET_ASPNET_IMAGE} AS runtime
ARG APP_UID=10001
ARG APP_GID=10001

USER root
RUN groupadd --gid "${APP_GID}" skypulse \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
       --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin skypulse \
    && install -d -m 0755 -o "${APP_UID}" -g "${APP_GID}" /app \
    && dotnet --list-runtimes | grep -Eq '^Microsoft\.NETCore\.App 10\.0\.11 ' \
    && dotnet --list-runtimes | grep -Eq '^Microsoft\.AspNetCore\.App 10\.0\.11 '

COPY --from=build --chown=${APP_UID}:${APP_GID} /out/ /app/
COPY --from=build --chown=${APP_UID}:${APP_GID} /tools/ /opt/skypulse-tools/
COPY deploy/hetzner-cx53/app-entrypoint.sh /usr/local/bin/skypulse-app-entrypoint
RUN chmod 0555 /usr/local/bin/skypulse-app-entrypoint

WORKDIR /app
USER ${APP_UID}:${APP_GID}
ENV DOTNET_EnableDiagnostics=0 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    ASPNETCORE_ENVIRONMENT=Production \
    ASPNETCORE_URLS=http://127.0.0.1:5080

ENTRYPOINT ["/usr/local/bin/skypulse-app-entrypoint"]
