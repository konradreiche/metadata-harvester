require 'active_support/all'
require 'action_view'
require 'curb'
require 'json'
require 'sidekiq'

require_relative 'core_ext'
require_relative 'json_archiver'

Sidekiq.configure_server do |config|
  namespace = 'metadata-harvester'
  url = 'redis://localhost:6379'
  config.redis =  { namespace: namespace, url: url  }
end

module MetadataHarvester

  class Worker
    include ActionView::Helpers::DateHelper
    include Sidekiq::Worker

    ##
    # Initializes basic attributes.
    #
    def initialize
      @timeout = 1
    end

    ##
    # Starts the metadata harvesting procedure
    #
    def perform(repository, options)
      repository = repository.with_indifferent_access
      id = repository[:name]
      logger.info("Harvest #{id}")

      @archiver = JsonArchiver.new(id)
      @options = options.with_indifferent_access

      if repository.key?(:dump)
        download_dump(repository)
      else
        download_records(repository)
      end
      @archiver.wrap_up()
      @archiver.compress if @options[:compress]
    end

    ##
    #  Downloads and extracts the dump of a repository.
    #
    def download_dump(repository)
      url = repository[:dump]
      type = File.extname(url)[1..-1]

      @archiver.download(url, type)
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
        logger.info("#{i + 1} of #{steps} - #{url} ~ #{eta}")
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
    rescue JSON::ParserError, Curl::Err::PartialFileError => e
      @timeout *= 2
      logger.warn("Parse Error. Retry in #{sleep}s")
      sleep(@timeout)
      retry
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
