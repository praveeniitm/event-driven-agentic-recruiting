FROM confluentinc/cp-kafka-connect:7.6.1
# The JDBC connector (with the PostgreSQL driver bundled) is not in the base image. Installed at build time.
# PROD: pin the connector version, mirror it into an internal artifact repo, build in CI and scan the image.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6
