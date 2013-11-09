#!/bin/sh
rm -f log/sidekiq.log
redis-cli -c "FLUSHALL"
sidekiq -C config.yml
