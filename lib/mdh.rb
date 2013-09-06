require 'active_support/all'
require 'trollop'
require 'worker'
require 'yaml'

module MetadataHarvester

  def self.start
    options = Trollop::options do
      opt :compress, 'Compresses the data with Gzip afterwards'
      opt :repository, 'Select a repository for harvesting', :type => :string
    end

    catalog = load_repositories()
    catalog.each do |type, repositories|
      repositories.each do |repository|
        next if skip?(options, repository)
        MetadataHarvester::Worker.perform_async(repository, options)
      end
    end
  end

  private
  def self.load_repositories
    path = File.expand_path('..', File.dirname(__FILE__))
    result = YAML.load_file("#{path}/repositories.yml")
    result.with_indifferent_access
  end

  def self.skip?(options, repository)
    return false if options[:repository].nil?
    selection = options[:repository]
    selection.downcase != repository[:name].downcase
  end

end
