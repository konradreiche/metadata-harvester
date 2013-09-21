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
    # @return [String] the filename of the retrieved dump
    #
    def download(url, type)
      Sidekiq.logger.info("Download #{url}")

      filename = "#{@path}.dump.#{type}"
      file = File.new(filename, 'w')

      curl = Curl::Easy.new(url)
      curl.ssl_verify_peer = false
      curl.on_body { |data| file.write(data) }

      curl.perform
      file.close()
      yield filename, type
    end

    ##
    # Extracts a downloaded metadata record if the format is supported.
    #
    def wrap(filename, type)
      Sidekiq.logger.info("Extract #{filename}")

      case type.to_sym
      when :gz
        wrap_gzip(filename)
      when :zip
        wrap_zip(filename)
      else
        raise TypeError, "The filetype #{type} is unknown."
      end
      File.delete(filename)
    end

    ##
    # Routine for extracting a GZip archive on the file system.
    #
    def wrap_gzip(filename)
      Zlib::GzipWriter.open(@gz_file) do |writer|
        writer.write(Yajl::Encoder.encode(@header))
        Zlib::GzipReader.open(filename) do |reader|
          IO.copy_stream(reader, writer)
        end
      end
    end

    ##
    # Routine for extracting a Zip archive on the file system.
    #
    def wrap_zip(filename)
      Zlib::GzipWriter.open(@gz_file) do |writer|
        writer.write(Yajl::Encoder.encode(@header))
        Zip::InputStream.open(filename) do |reader|
          reader.get_next_entry
          IO.copy_stream(reader, writer)
        end
      end
    end

  end

end
