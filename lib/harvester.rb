require 'curb'
require 'logger'
require 'sidekiq'
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
  end

  def perform(url, name, repository, i)
    @log.info "#{i}"
    dataset = query(url, name)
    index(dataset, repository) if dataset.is_a? Hash
  end

  def query(url, name)
    curl = Curl::Easy.new
    curl.url = url + '/rest/dataset/' + name
    curl.ssl_verify_peer = false
    curl.perform
    parse(curl.body_str)
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
        json == "" ? nil : json
      end
    end
  end

  def index(dataset, source)
    entry = CKAN::Dataset.new(dataset, source)
    Tire.index 'metadata' do
      create
      store entry
    end
  end
end
