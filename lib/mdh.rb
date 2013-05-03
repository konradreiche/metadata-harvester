require 'curb'
require 'geocoder'
require 'json'
require 'tire'
require 'yaml'

require_relative 'repository'
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

    for repository in result[:CKAN]

      datasets = JSON.parse(Curl.get(repository['url'] +
                                     '/search/dataset').body_str)['count']

      location = Geocoder.search(repository['location']).first
      store Repository::CKAN.new(:name => repository['name'],
                                 :url => repository['url'],
                                 :datasets => datasets,
                                 :latitude => location.latitude,
                                 :longitude => location.longitude)
    end
    refresh
  end
  result
end

def harvest_ckan_repositories
  for repository in load_repositories[:CKAN]
    Harvester.perform_async(repository['url'], repository['name'],
                            repository['limit'], repository['import'])
  end
end

harvest_ckan_repositories
