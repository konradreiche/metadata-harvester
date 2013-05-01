require 'curb'
require 'geocoder'
require 'json'
require 'tire'
require 'yaml'

require_relative 'harvester'

Sidekiq.configure_client do |config|
    config.redis = { :namespace => 'metadata-harvester', :url => 'redis://localhost:6379' }
end

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
      location = Geocoder.search(repository['location']).first
      store :id => repository['name'], :name => repository['name'],
        :url => repository['url'], :type => 'CKAN', :_type => 'repository',
        :latitude => location.latitude, :longitude => location.longitude
    end
    refresh
  end
  result
end

def query(base_url)
  curl = Curl::Easy.new
  curl.url = base_url + '/rest/dataset'
  curl.ssl_verify_peer = false
  curl.perform
  JSON.parse(curl.body_str)
end

def harvest_ckan_repositories
  for repository in load_repositories[:CKAN]
    datasets = query repository['url']
    datasets.each_with_index do |name, i|
    Harvester.perform_async(repository['url'], name, repository['name'], i)
    end
  end
end

harvest_ckan_repositories
