FROM memcached:1.6.18

COPY entrypoint.sh usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["memcached"]
