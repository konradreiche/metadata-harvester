#!/bin/sh
redis-cli -c "FLUSHALL"
ruby -Ilib bin/mdh $@
