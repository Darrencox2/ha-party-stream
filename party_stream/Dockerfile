ARG BUILD_FROM
FROM $BUILD_FROM
ARG BUILD_ARCH
# A static Go binary built on the Mac (see README); nothing to compile on the Green.
COPY bin/party-stream-${BUILD_ARCH} /usr/bin/party-stream
RUN chmod +x /usr/bin/party-stream
CMD ["/usr/bin/party-stream"]
