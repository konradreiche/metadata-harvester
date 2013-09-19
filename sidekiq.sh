#!/bin/sh
rm -f log/sidekiq.log
sidekiq -C config.yml
