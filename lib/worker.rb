require 'action_view'
require 'curb'
require 'logger'
require 'open-uri'
require 'json'
require 'json_archiver'
require 'sidekiq'
require 'tire'

module MetadataHarvester
  include ActionView::Helpers::DateHelper

  class Worker
    include Sidekiq::Worker

    ##
    # Initializes basic attributes.
    #
    def initialize
      @timeout = 0
    end

    ##
    # Starts the metadata harvesting procedure
    #
    def perform(repository, options)
      repository = repository.with_indifferent_access
      id = repository[:name]
      @compress = options[:compress]
      @archiver = JsonArchiver.new(id)
      if repository.key?(:dump)
        download_dump(repository)
      else
        download_records(repository)
      end
    end

    ##
    #  Downloads and extracts the dump of a repository.
    #
    def download_dump(repository)
      url = repository[:dump]
      type = File.extname(url)[1..-1]
      @archiver.download(url)
      @archiver.extract(type)
    end

    ##
    # Downloads the metadata records through the exposed API.
    #
    def download_records(repository)
      url = repository[:url]
      rows = repository[:rows]
      steps = count(url).fdiv(rows).ceil

      before = Time.new
      steps.times do |i|
        records = query(url, rows, i)
        @archiver.store(records)
        now = Time.new
        elapsed = (now - before) * (steps - i + 1)
        eta = distance_of_time_in_words(before, before + elapsed)
        logger.info("#{i} of #{steps} - #{url} ~ #{eta}")
        before = now
      end
    end

    ##
    # Retrieve the number of total metadata records.
    #
    def count(url)
      curl = Curl::Easy.new
      curl.url = "#{url}/search/dataset"
      curl.ssl_verify_peer = false
      curl.perform
      JSON.parse(curl.body_str)['count']
    end

    ##
    # Queries a limited number (+rows+) of metadata records from a repository.
    #
    # Uses the CKAN Search API.
    #
    def query(url, rows, i)
      url = "#{url}/3/action/package_search"
      data = { rows: rows, start: i }
      curl = Curl.get(url, data)
      response = JSON.parse_recursively(curl.body_str)
      result = response['result']['results']
    rescue JSON::ParserError, Curl::Err::PartialFileError => e
      @timeout *= 2
      logger.warn("Parse Error. Retry in #{sleep}s")
      sleep(@timeout)
      retry
    end

  end

end
