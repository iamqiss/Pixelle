################################################################################
# This Dockerfile was generated from the template at distribution/src/docker/Dockerfile
#
# Beginning of multi stage Dockerfile
################################################################################
<% /*
  This file is passed through Groovy's SimpleTemplateEngine, so dollars and backslashes
  have to be escaped in order for them to appear in the final Dockerfile. You
  can also comment out blocks, like this one. See:

  https://docs.groovy-lang.org/latest/html/api/groovy/text/SimpleTemplateEngine.html

  We use control-flow tags in this file to conditionally render the content. The
  layout/presentation here has been adjusted so that it looks reasonable when rendered,
  at the slight expense of how it looks here.
*/ %>
################################################################################
# Build stage 0 `builder`:
# Extract Density artifact
################################################################################

FROM ${base_image} AS builder
# `tini` is a tiny but valid init for containers. This is used to cleanly
# control how Density and any child processes are shut down.
#
# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against the published checksum.
RUN set -eux ; \\
    tini_bin="" ; \\
    case "\$(arch)" in \\
        aarch64) tini_bin='tini-arm64' ;; \\
        x86_64)  tini_bin='tini-amd64' ;; \\
        *) echo >&2 ; echo >&2 "Unsupported architecture \$(arch)" ; echo >&2 ; exit 1 ;; \\
    esac ; \\
    curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin} ; \\
    curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/\${tini_bin}.sha256sum ; \\
    sha256sum -c \${tini_bin}.sha256sum ; \\
    rm \${tini_bin}.sha256sum ; \\
    mv \${tini_bin} /tini ; \\
    chmod +x /tini

RUN mkdir /usr/share/density
WORKDIR /usr/share/density

${source_density}

RUN tar zxf /opt/density.tar.gz --strip-components=1
RUN sed -i -e 's/DENSITY_DISTRIBUTION_TYPE=tar/DENSITY_DISTRIBUTION_TYPE=docker/' /usr/share/density/bin/density-env
RUN mkdir -p config config/jvm.options.d data logs
RUN chmod 0775 config config/jvm.options.d data logs
COPY config/density.yml config/log4j2.properties config/
RUN chmod 0660 config/density.yml config/log4j2.properties

################################################################################
# Build stage 1 (the actual Density image):
#
# Copy density from stage 0
# Add entrypoint
################################################################################

FROM ${base_image}

ENV DENSITY_CONTAINER true

RUN set -e \\
     && dnf -y update \\
     && dnf -y install --setopt=tsflags=nodocs \\
          nmap-ncat shadow-utils zip unzip \\
     && dnf clean all \\
     && rm -rf /var/cache/dnf

RUN groupadd -g 1000 density && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/density density && \\
    chmod 0775 /usr/share/density && \\
    chown -R 1000:0 /usr/share/density

WORKDIR /usr/share/density
COPY --from=builder --chown=1000:0 /usr/share/density /usr/share/density
COPY --from=builder --chown=0:0 /tini /tini

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/density/jdk/lib/security/cacerts

ENV PATH /usr/share/density/bin:\$PATH

COPY bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# The JDK's directories' permissions don't allow `java` to be executed under a different
# group to the default. Fix this.
RUN find /usr/share/density/jdk -type d -exec chmod 0755 '{}' \\; && \\
    chmod g=u /etc/passwd && \\
    chmod 0775 /usr/local/bin/docker-entrypoint.sh

# Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
RUN find / -xdev -perm -4000 -exec chmod ug-s {} +

EXPOSE 9200 9300

LABEL org.label-schema.build-date="${build_date}" \\
  org.label-schema.license="${license}" \\
  org.label-schema.name="Density" \\
  org.label-schema.schema-version="1.0" \\
  org.label-schema.url="https://www.density.org" \\
  org.label-schema.usage="https://www.density.org/guide/en/density/reference/index.html" \\
  org.label-schema.vcs-ref="${git_revision}" \\
  org.label-schema.vcs-url="https://github.com/density-project/Density" \\
  org.label-schema.vendor="Density" \\
  org.label-schema.version="${version}" \\
  org.opencontainers.image.created="${build_date}" \\
  org.opencontainers.image.documentation="https://www.density.org/guide/en/density/reference/index.html" \\
  org.opencontainers.image.licenses="${license}" \\
  org.opencontainers.image.revision="${git_revision}" \\
  org.opencontainers.image.source="https://github.com/density-project/Density" \\
  org.opencontainers.image.title="Density" \\
  org.opencontainers.image.url="https://www.density.org" \\
  org.opencontainers.image.vendor="Density" \\
  org.opencontainers.image.version="${version}"

ENTRYPOINT ["/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["densitywrapper"]

################################################################################
# End of multi-stage Dockerfile
################################################################################
