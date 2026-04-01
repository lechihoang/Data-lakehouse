FROM apache/superset:3.1.3

COPY --chmod=755 entrypoint.sh /entrypoint.sh
COPY --chown=superset:superset conf/ /app/conf/

ENTRYPOINT ["/entrypoint.sh"]
