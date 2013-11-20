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
      opt :repositories, 'Only harvest certain repositories', :type => :strings
      opt :dumps, 'A file with a list of dumps and dates', :type => :string
    end

    repositories = load_repositories

    selection = options[:repositories]
    selection = repositories.map { |item| item[:id] } if selection.nil?

    if not options[:dumps].nil?
      dumps = YAML.load(File.read(options[:dumps])) unless options[:dumps].nil?
      selection = [dumps['id']]
    end

    catalog = filter(repositories, selection)
    catalog.each do |repository|
      MetadataHarvester::Worker.perform_async(repository, options)
    end
  end

  def self.filter(repositories, selection)
    repositories.select do |repository|
      selection.include?(repository[:id]) and not disabled?(repository)
    end
  end

  private

  ##
  # Returns a hash with repositories containing the harvest URLs.
  #
  def self.load_repositories
    path = File.expand_path('..', File.dirname(__FILE__))
    result = YAML.load_file("#{path}/repositories.yml")
    result = result.with_indifferent_access
    result[:repositories]
  end

  def self.disabled?(repository)
    not repository[:disabled].nil? and repository[:disabled]
  end

end
