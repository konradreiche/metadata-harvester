require 'curb'
require 'geocoder'
require 'json'
require 'tire'
require 'trollop'
require 'yaml'

require_relative 'repository'
require_relative 'harvester'

if ENV['DEBUG']
  require 'sidekiq'
  require 'sidekiq/testing/inline'
end

Sidekiq.configure_client do |config|
    config.redis = { :namespace => 'metadata-harvester', :url => 'redis://localhost:6379' }
end

def symbolize(hash)
  hash.each_with_object({}) { |(k, v), h| h[k.to_sym] = v }
end

def load_repositories
  path = File.expand_path('..', File.dirname(__FILE__))
  result = YAML.load_file("#{path}/repositories.yml")
  result = symbolize result

  Tire.index 'repositories' do

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

  options = Trollop::options do
    opt :archive, 'Archive raw data in compressed JSON'
  end

  for repository in load_repositories[:CKAN]
    Harvester.perform_async(repository['url'],
                            repository['name'],
                            repository['limit'],
                            options[:archive],
                            repository['import'])
  end
end

harvest_ckan_repositories
