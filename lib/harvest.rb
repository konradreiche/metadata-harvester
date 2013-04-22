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
  result = symbolize result

  Tire.index 'repositories' do
    delete
    create :mappings => {
      :repository => {
        :properties => { :url => 'string' }
      } 
    }

    for repository in result[:CKAN]
      store :_id => repository['name'], :name => repository['name'],
        :url => repository['url'], :type => 'CKAN', :_type => 'repository'
    end
    refresh
  end
  result
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

def index(dataset, source)
  entry = CKAN::Dataset.new dataset, source
  Tire.index 'metadata' do
    create
    store entry
  end
end

def harvest_ckan_repositories
  for repository in load_repositories[:CKAN]
    datasets = query_datasets repository['url']
    datasets.each_with_index do |name, i|
      $log.info "#{i} of #{datasets.length}"
      dataset = query_dataset repository['url'], name
      next unless dataset.is_a? Hash
      index dataset, repository['name']
    end
  end
end

harvest_ckan_repositories
