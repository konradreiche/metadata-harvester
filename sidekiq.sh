#!/bin/sh
sidekiq -r ./lib/harvester.rb -t 0
