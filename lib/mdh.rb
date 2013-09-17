require 'active_support/all'
require 'trollop'
require 'worker'
require 'yaml'

module MetadataHarvester

  ##
  # Main method to start the harvesting procedure.
  #
  # This method is invoked from bin/mdh.
  #
  def self.start
    options = Trollop::options do
      opt :compress, 'Compresses the data with Gzip afterwards'
      opt :repository, 'Select a repository for harvesting', :type => :string
    end

    catalog = load_repositories()
    catalog[:repositories].each do |repository|
      next if skip?(options, repository)
      MetadataHarvester::Worker.perform_async(repository, options)
    end
  end

  private

  ##
  # Returns a hash with repositories containing the harvest URLs.
  #
  def self.load_repositories
    path = File.expand_path('..', File.dirname(__FILE__))
    result = YAML.load_file("#{path}/repositories.yml")
    result.with_indifferent_access
  end

  ##
  # Checks for a given repository whether it should be skipped in the process.
  #
  # If the repository option was selected this method encapsulates the logic
  # for comparing the repository names.
  #
  def self.skip?(options, repository)
    return false if options[:repository].nil?
    selection = options[:repository]
    selection.downcase != repository[:name].downcase
  end

end
