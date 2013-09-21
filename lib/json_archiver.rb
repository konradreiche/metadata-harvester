require 'fileutils'
require 'yajl'
require 'zip'
require 'zlib'

require_relative 'yajl_ext'

module MetadataHarvester
  class JsonArchiver

    Zip.setup do |c|
      c.on_exists_proc = true
    end

    ##
    # Initializes the JSON archiver for a dedicated repository.
    #
    # Makes sure the archive path is available and sets the path variables.
    #
    def initialize(id, type, date, count)
      @header = { repository: id, type: type, date: date, count: count }
      directory = File.expand_path("../archives/#{id}", File.dirname(__FILE__))

      @path = "#{directory}/#{date}-#{id}"
      @gz_file = "#{@path}.raw.json.gz"

      File.delete(@gz_file) if File.exists?(@gz_file)
      FileUtils.mkdir_p(directory)
    end

    def store
      gz = File.new(@gz_file, 'w')
      writer = Yajl::Gzip::StreamWriter.new(gz)
      writer.write(@header)

      yield writer

      writer.close()
    end

    ##
    # Downloads a metadata record dump.
    #
    def download(url, type)
      Sidekiq.logger.info("#{@id} - Download #{url}")
      file = File.new("#{@path}.#{type}", 'w')

      curl = Curl::Easy.new(url)
      curl.ssl_verify_peer = false
      curl.on_body { |data| file.write(data) }

      curl.perform
      file.close()
    end

    ##
    # Extracts a downloaded metadata record if the format is supported.
    #
    def extract(type)
      Sidekiq.logger.info("#{@id} - Extract #{@path}.#{type}")
      case type.to_sym
      when :gz
        extract_gzip(type)
      when :zip
        extract_zip(type)
      else
        raise TypeError, "The filetype #{type} is unknown."
      end
      File.delete("#{@path}.#{type}")
    end
    
    ##
    # Compresses the JSON files afterwards.
    #
    def compress
      json_file = "#{@path}.json"
      gunzip_file = "#{@path}.gz"
      Zlib::GzipWriter.open(gunzip_file) do |gz|
        File.open(json_file, 'r') do |g|
          IO.copy_stream(g, gz)
        end
      end
      File.delete(json_file)
    end

    ##
    # Routine for extracting a GZip archive on the file system.
    #
    def extract_gzip(type)
      Zlib::GzipReader.open("#{@path}.#{type}") do |gz|
        File.open("#{@path}.raw.json", 'w') do |g|
          IO.copy_stream(gz, g)
        end
      end
    end

    ##
    # Routine for extracting a Zip archive on the file system.
    #
    def extract_zip(type)
      Zip::File.open("#{@path}.#{type}").first.extract("#{@path}.raw.json")
    end

  end
end
