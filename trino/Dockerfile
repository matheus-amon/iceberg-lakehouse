FROM trinodb/trino:439

# Trino's Iceberg connector accepts only REST, GLUE, JDBC and NESSIE catalogs
# (the `hadoop` and `hive` catalog types were removed), so a local demo needs a
# real catalog implementation. JDBC-on-SQLite is the only one of the four that
# needs no extra service -- no MinIO, no REST catalog container, no Nessie.
#
# The driver has to sit in the connector's own plugin directory; Trino loads
# catalog drivers from there, not from the general classpath.
COPY sqlite-jdbc-3.47.1.0.jar /usr/lib/trino/plugin/iceberg/

USER trino