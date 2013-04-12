require 'curb'
require 'json'
require 'tire'
require 'yaml'

require_relative 'ckan'


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
  symbolize JSON.parse(curl.body_str)
end

def query_datasets(base_url) # rescue on wrong urls
  curl = Curl::Easy.new
  curl.url = base_url + '/rest/dataset'
  curl.ssl_verify_peer = false
  curl.perform
  JSON.parse(curl.body_str)
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
    for name in datasets
      index(query_dataset url, name)
    end
  end
end

def load_datasets
  Tire.search('ckan') { query { all } }.results.map do |document|
    CKAN::Dataset.new JSON.parse document.to_json
  end
end 

harvest_ckan_repositories