require 'active_support/all'
require 'action_view'
require 'curb'
require 'json'
require 'sidekiq'

require_relative 'core_ext'
require_relative 'json_archiver'
require_relative 'prettier'

Sidekiq.configure_server do |config|
  namespace = 'metadata-harvester'
  url = 'redis://localhost:6379'
  config.redis =  { namespace: namespace, url: url  }
end

module MetadataHarvester

  class Worker
    include ActionView::Helpers::DateHelper
    include Sidekiq::Worker

    TIMEOUT_LOWER_CAP = 30.0
    TIMEOUT_UPPER_CAP = 600.0

    ##
    # Initializes basic attributes.
    #
    def initialize
      @timeout = TIMEOUT_LOWER_CAP
      logger.formatter = Prettier.new
    end

    ##
    # Starts the metadata harvesting procedure
    #
    def perform(repository, options)
      repository = repository.with_indifferent_access

      id = repository[:id]
      url = repository[:url]
      type = repository[:type]

      logger.formatter.add(id)
      logger.info("Start harvester")

      date = Date.today
      count = count(url)

      @archiver = JsonArchiver.new(id, type, date, count)
      @options = options.with_indifferent_access

      if repository.key?(:dump)
        download_dump(repository)
      else
        download_records(repository)
      end
    end

    ##
    # Downloads and extracts the dump of a repository.
    #
    def download_dump(repository)
      url = repository[:dump]
      type = File.extname(url)[1..-1]
      @archiver.download(url, type) do |target, type|
        @archiver.wrap(target, type)
      end
    end

    ##
    # Downloads the metadata records through the exposed API.
    #
    def download_records(repository)
      url = repository[:url]
      rows = repository[:rows]
      id = repository[:id]
      steps = count(url).fdiv(rows).ceil

      @archiver.store do |writer|
        before = Time.new
        steps.times do |i|
          records = query(url, rows, i, id)
          records = unify(records)

          writer.write(records)
          writer.flush

          before = eta(before, steps, i, url)
        end
      end
    end

    ##
    # Retrieve the number of total metadata records.
    #
    def count(url)
      curl = curl("#{url}/search/dataset")
      curl.perform
      content = curl.body_str

      return JSON.parse(content)['count']
    rescue JSON::ParserError, Curl::Err::ConnectionFailed, 
      Curl::Err::PartialFileError => e
      timeout()
      retry
    ensure
      curl.close() unless curl.nil?
    end

    ##
    # Queries a limited number (+rows+) of metadata records from a repository.
    #
    # Uses the CKAN Search API.
    #
    def query(url, rows, i, id)
      data = { rows: rows, start: i }
      curl = curl("#{url}/3/action/package_search", data)
      curl.perform

      content = curl.body_str
      response = JSON.parse_recursively(content)
      result = response['result']['results']
      @timeout /= 2 if @timeout > TIMEOUT_UPPER_CAP

      return result
    rescue JSON::ParserError, Curl::Err::PartialFileError,
     Curl::Err::ConnectionFailed => e
      timeout()
      retry
    ensure
      curl.close() unless curl.nil?
    end

    ##
    # Timeout method used to wait a timespan before the next request.
    #
    def timeout
      time = time_ago_in_words(@timeout.seconds.from_now)
      logger.warn("Response: Parse Error. Retry in #{time}")

      sleep(@timeout)
      @timeout *= 2 if @timeout < TIMEOUT_UPPER_CAP
    end

    def unify(records)
      records = records.each do |record|
        record['groups'] = record['groups'].map { |group| group['name'] }
        record['tags'] = record['tags'].map { |tag| tag['name'] }
      end
    end
    
    private
    ##
    # Logs the estimated time of arrival.
    #
    # @return [Time] the current time
    #
    def eta(before, steps, i, url)
      now = Time.new
      elapsed = (now - before) * (steps - i + 1)
      eta = distance_of_time_in_words(before, before + elapsed)

      logger.info("#{i + 1} of #{steps} - #{url} ~ #{eta}")
      return now
    end

    def curl(url, parameter={})
      curl = Curl::Easy.new
      curl.url = Curl::urlalize(url, parameter)
      curl.ssl_verify_peer = false
      curl.follow_location = true

      return curl
    end

  end

end
