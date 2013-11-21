require 'active_support/all'
require 'action_view'
require 'curb'
require 'oj'
require 'json'
require 'sidekiq'

require_relative 'ckan'
require_relative 'core_ext'
require_relative 'archiver'
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
      legacy = repository[:legacy]

      logger.formatter.add(id)
      logger.info("Start harvester")

      date = Date.today
      count = count(url, legacy)

      @options = options.with_indifferent_access

      if not @options[:dumps].nil?
        download_dumps(repository)
      elsif repository.key?(:dump)
        @archiver = Archiver.new(id, type, date, count)
        download_dump(repository, url)
      else
        @archiver = Archiver.new(id, type, date, count)
        download_records(repository, legacy)
      end
    end

    ##
    # Downloads and extracts the dump of a repository.
    #
    def download_dump(repository, url=nil)
      url = repository[:dump]
      file_type = File.extname(url)[1..-1]
      @archiver.download(url, file_type) do |target, type|
        @archiver.wrap(target, type)
      end
    end

    # Downloads a list of dumps based on the given options.
    #
    def download_dumps(repository)
      dumps = YAML.load(File.read(@options[:dumps]))
      id = repository[:id]
      type = repository[:type]
      dumps.delete('id')

      dumps.each do |date, url|
        @archiver = Archiver.new(id, type, date, '?')
        download_dump(repository, url)
      end
    end

    ##
    # Downloads the metadata records through the exposed API.
    #
    def download_records(repository, legacy=false)
      id = repository[:id]
      url = repository[:url]
      rows = repository.key?(:rows) ? repository[:rows] : 1000

      return download_records_legacy(id, url) if legacy

      total = count(url)
      steps = total.fdiv(rows).ceil

      @archiver.store do |writer|
        before = Time.new
        steps.times do |i|
          rows = total - (i * rows) if i == steps - 1
          records = query(url, rows, i, id)

          records = unify(records)
          Oj.to_stream(writer, records)
          writer.write("\n")

          before = eta(before, steps, i, url)
        end
      end
    end

    def query_legacy(url)
      response = Curl.get(url).body_str
      Oj.load(response)
    rescue JSON::ParserError, Curl::Err::ConnectionFailed, 
      Curl::Err::PartialFileError
      timeout()
      retry
    end

    def download_records_legacy(id, url)
      response = Curl.get("#{url}/rest/dataset").body_str
      records = Oj.parse(response)

      @archiver.store do |writer|
        before = Time.now
        metadata = []

        records.each_with_index do |record_name, i|
          dataset = query_legacy("#{url}/rest/dataset/#{record_name}")

          if not dataset.is_a?(Hash)
            logger.warn("Drop dataset #{record_name} (#{dataset}")
            next
          end

          metadata << dataset
          before = eta(before, records.length, i, id)
        end
        
        Oj.to_stream(writer, metadata)
        writer.write("\n")
      end
    end

    ##
    # Retrieve the number of total metadata records.
    #
    def count(url, legacy=false)
      model_api = '/rest/dataset'
      action_api = '/action/package_search'

      url = url + (legacy ? model_api : action_api)
      method = legacy ? :get : :post
      curl = curl(url, method)

      curl.perform
      response = Oj.load(curl.body_str)
      return legacy ? response.length : response['result']['count']
    rescue JSON::ParserError, Curl::Err::ConnectionFailedError, 
      Curl::Err::PartialFileError
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
      data = { rows: rows, start: rows * i }
      curl = curl("#{url}/3/action/package_search", :get, data)
      curl.perform

      content = curl.body_str
      response = Oj.load(content)
      result = response['result']['results']
      @timeout /= 2 if @timeout > TIMEOUT_UPPER_CAP

      return result
    rescue JSON::ParserError, Curl::Err::PartialFileError,
     Curl::Err::ConnectionFailedError
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

        record['extras'] = record['extras'].each_with_object({}) do |extra, result|
          value = extra['value']
          value = CKAN.normalize_extras(value) if value.is_a?(String)
          result[extra['key']] = value
        end
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
      elapsed = (now - before) * (steps - (i + 1))
      eta = distance_of_time_in_words(before, before + elapsed)

      logger.info("#{i + 1} of #{steps} - #{url} ~ #{eta}")
      return now
    end

    def curl(url, method=:get, parameter={})
      settings = Proc.new do |curl|
        curl.ssl_verify_peer = false
        curl.follow_location = true
      end

      case method
      when :get
        Curl.get(url, parameter, &settings)
      when :post
        Curl.post(url, JSON.dump(parameter), &settings)
      end
    end

  end

end
