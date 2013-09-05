#!/bin/sh
sidekiq -r ./lib/worker.rb -t 0
