require 'curb'
require 'logger'
require 'json'
require 'sidekiq'
require 'sidekiq/testing/inline'
require 'tire'

require_relative 'ckan'

Sidekiq.configure_server do |config|
  config.redis = { :namespace => 'metadata-harvester', :url => 'redis://localhost:6379' }
end

class Harvester
  include Sidekiq::Worker

  def initialize
    @log = Logger.new(STDOUT)
    @log.level = Logger::DEBUG
    @limit = 500
  end

  def perform(url, source)

    steps = (count(url) / @limit).ceil
    steps.times do |i|
      datasets = query(url, i + 1)
      index(datasets, source)
    end
  end

  def count(url)
    curl = Curl::Easy.new
    curl.url = url + '/search/dataset'
    curl.ssl_verify_peer = false
    curl.perform
    JSON.parse(curl.body_str)['count']
  end

  def query(url, i)
    url = url + '/3/action/current_package_list_with_resources'
    data = '{"limit": "%s", "page": "%s"}' % [@limit, i]
    curl = Curl.post(url, data)
    result = JSON.parse(curl.body_str)['result']
    result.each { |dataset|
      dataset = parse dataset
      dataset['extras'] = parse_extras dataset['extras']
    }.compact
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
        json
      end
    end
  end

  def index(datasets, source)
    for dataset in datasets
      Tire.index 'metadata' do
        create
        store CKAN::Dataset.new(dataset, source)
      end
    end
  end
end