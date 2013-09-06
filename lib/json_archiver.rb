require 'fileutils'
require 'zip'
require 'zlib'

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
    def initialize(id)
      @directory = File.expand_path("../archive/#{id}", File.dirname(__FILE__))
      FileUtils.mkdir_p(@directory)
      @path = "#{@directory}/#{Date.today}-#{id}"
    end

    ##
    # Writes a set of metadata records to the file system.
    #
    def store(records)
      json_file = "#{@path}.json"
      File.delete(json_file) if File.exists?(json_file)
      File.open(json_file, 'a') { |file| JSON.dump(records, file) }
    end

    ##
    # Downloads a metadata record dump.
    #
    def download(url)
      Sidekiq.logger.info("Download #{url}")
      curl = Curl::Easy.new(url)
      curl.ssl_verify_peer = false
      file = File.new(@path, 'w')
      curl.on_body { |data| file.write(data) }
      curl.perform
      file.close()
    end

    ##
    # Extracts a downloaded metadata record if the format is supported.
    #
    def extract(type)
      Sidekiq.logger.info("Extract #{@path}.#{type}")
      case type.to_sym
      when :gz
        extract_gzip()
      when :zip
        extract_zip()
      else
        raise TypeError, "The filetype #{type} is unknown."
      end
      File.delete(@path)
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
    def extract_gzip
      Zlib::GzipReader.open(@path) do |gz|
        File.open("#{@path}.json", 'w') do |g|
          IO.copy_stream(gz, g)
        end
      end
    end

    ##
    # Routine for extracting a Zip archive on the file system.
    #
    def extract_zip
      Zip::File.open(@path).first.extract("#{@path}.json")
    end

  end
end
