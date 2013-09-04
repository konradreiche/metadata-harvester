require 'action_view'
require 'curb'
require 'logger'
require 'open-uri'
require 'json'
require 'sidekiq'
require 'tire'
require 'zipruby'
require 'zlib'

require_relative 'ckan'
require_relative 'json_archiver'

module MetadataHarvester

  class Worker
    include Sidekiq::Worker

    def initialize
      @log = Logger.new(STDOUT)
      @log.level = Logger::DEBUG
      @sleep = 3
    end

    def perform(url, source, limit, archive=false, import=nil)

      @archive = archive
      @archiver = JsonArchiver.new(source) if @archive
      if import.nil?
        steps = (count(url) / limit.to_f).ceil
        logger.info("Index records")

        before = Time.new
        elapsed = 0
        steps.times do |i|
          datasets = query(url, limit, i)
          index(datasets, source)
          now = Time.new
          e = (now - before) * (steps - i + 1)
          display = ActionView::Base.new.distance_of_time_in_words(before, before + e)
          logger.info("#{i} of #{steps} - #{url} ~ #{display}")
          before = now
        end
      else
        logger.info("Download #{import}")
        case import.split('.').last.to_sym
        when :zip
          zipbytes = Curl.get(import).body_str
          Zip::Archive.open_buffer(zipbytes) do |zf|
            # read the first file
            zf.fopen(zf.get_name(0)) do |f|
              unzipped = f.read
              logger.info("Index records")
              index(parse(unzipped), source)
            end
          end
        when :gz
          gz = Zlib::GzipReader.new(open(import))
          logger.info("Index records")
          index(parse(gz.read), source)
        end
      end
    end

    def count(url)
      curl = Curl::Easy.new
      curl.url = url + '/search/dataset'
      curl.ssl_verify_peer = false
      curl.perform
      JSON.parse(curl.body_str)['count']
    end

    def query(url, limit, i)
      url = url + '/3/action/package_search'
      data = { rows: limit, start: i }
      curl = Curl.get(url, data)
      result = JSON.parse(curl.body_str)['result']['results']
      result = result.each do |dataset|
        dataset = parse dataset
        dataset['extras'] = parse_extras dataset['extras']
        dataset['groups'] = dataset['groups'].map { |g| g['name'] }
        dataset['tags'] = dataset['tags'].map { |t| t['name'] }
      end.compact
    rescue JSON::ParserError, Curl::Err::PartialFileError => e
      @sleep *= 2
      logger.warn("Parse Error. Retry in #{sleep}s")
      sleep @sleep
      retry
    end

    def parse_extras(extras)
      result = {}
      extras.each do |item|
        value = item['value']
        if value.is_a? String
          value[0] = "" if value[0] == '"'
          value[value.length - 1] = "" if value[value.length - 1] == '"'
          value.gsub!("\\","")
        end
        result[item['key']] = parse value
      end
      result
    end

    def parse(json)
      if json.is_a? Hash
        json.each do |key, value|
          json[key] = parse value
        end
      elsif json.is_a? Array
        json.map { |item| parse item }
      else
        begin
          result = JSON.parse json
          parse result
        rescue
          json = nil if json.is_a? String and json.empty?
          json = nil if json.is_a? Array and json.empty?
          json = nil if json.is_a? Hash and json.empty?
          json
        end
      end
    end

    def index(datasets, repository)
      if @archive
        @archiver.store(datasets)
        return
      end
      date = Date.today
      datasets.each_with_index do |dataset, i|
        Tire.index 'metadata' do
          create
          store CKAN::Dataset.new(dataset, repository, date)
        end
      end
    end
  end
end
