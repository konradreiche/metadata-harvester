require 'curb'
require 'json'
require 'logger'
require 'tire'
require 'yaml'

require_relative 'ckan'

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

def symbolize(hash)
  hash.each_with_object({}) { |(k, v), h| h[k.to_sym] = v }
end

def load_repositories
  result = YAML.load_file('repositories.yml')
  symbolize result
end

def query_dataset(base_url, dataset)
  curl = Curl::Easy.new
  curl.url = base_url + '/rest/dataset/' + dataset
  curl.ssl_verify_peer = false
  curl.perform
  parse(curl.body_str)
end

def query_datasets(base_url) # rescue on wrong urls
  curl = Curl::Easy.new
  curl.url = base_url + '/rest/dataset'
  curl.ssl_verify_peer = false
  curl.perform
  JSON.parse(curl.body_str)
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

def index(dataset)
  entry = CKAN::Dataset.new dataset
  Tire.index 'ckan' do
    create
    store entry
  end
end

def harvest_ckan_repositories
  for url in load_repositories[:CKAN]
    datasets = query_datasets url
    datasets.each_with_index do |name, i|
      $log.info "#{i} of #{datasets.length}"
      dataset = query_dataset url, name
      index dataset
    end
  end
end

def load_datasets
  Tire.search('ckan') { query { all } }.results.map do |document|
    CKAN::Dataset.new JSON.parse document.to_json
  end
end 

harvest_ckan_repositories
