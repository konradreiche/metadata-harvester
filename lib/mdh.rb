require 'active_support/all'
require 'trollop'
require 'worker'
require 'yaml'

module MetadataHarvester

  def self.load_repositories
    path = File.expand_path('..', File.dirname(__FILE__))
    result = YAML.load_file("#{path}/repositories.yml")
    result.with_indifferent_access
  end

  def self.start
    options = Trollop::options do
      opt :compress, 'Compresses the data with Gzip afterwards'
    end

    for repository in load_repositories[:CKAN]
      catalog = load_repositories()
      catalog.each do |type, repositories|
        repositories.each do |repository|
          MetadataHarvester::Worker.perform_async(repository, options)
        end
      end
    end
  end

end
