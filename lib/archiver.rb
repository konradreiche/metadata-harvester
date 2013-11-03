require 'fileutils'
require 'yajl'
require 'zip'
require 'zlib'

require_relative 'dump_handler'
require_relative 'yajl_ext'

##
# The JSON archiver encapsulates the functionality for storing metadata to the
# file system.
# 
# @author Konrad Reiche
#
module MetadataHarvester
  class Archiver

    Zip.setup do |c|
      c.on_exists_proc = true
    end

    ##
    # Initializes the JSON archiver for a dedicated repository.
    #
    # Makes sure the archive path is available and sets the path variables.
    #
    def initialize(path=nil, id, type, date, count)
      @header = { "repository" => id,
                  "type"       => type,
                  "date"       => date.to_s,
                  "count"      => count }

      if path.nil?
        directory = File.expand_path("../archives/#{id}", File.dirname(__FILE__))
      else
        directory = File.expand_path("#{path}/#{id}")
      end

      @path = "#{directory}/#{date}-#{id}"
      @gz_file = "#{@path}.jl.gz"

      File.delete(@gz_file) if File.exists?(@gz_file)
      FileUtils.mkdir_p(directory)
    end

    def store
      gz = File.new(@gz_file, 'w')
      writer = Zlib::GzipWriter.new(gz)
      writer.sync = true

      Oj.to_stream(writer, @header)
      writer.write("\n")

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
      curl.follow_location = true

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
        aise jypeError, "The filetype #{type} is unknown."
      end
      File.delete(filename)
    end

    ##
    # Routine for extracting a GZip archive on the file system.
    #
    def wrap_gzip(filename)
      init_wrap(filename) do |handler|
        Zlib::GzipReader.open(filename) do |reader|
          Oj.sc_parse(handler, reader)
        end
      end
    end

    ##
    # Routine for extracting a Zip archive on the file system.
    #
    def wrap_zip(filename)
      init_wrap(filename) do |handler|
        Zip::InputStream.open(filename) do |reader|
          input_stream = reader.get_next_entry.get_input_stream
          Oj.sc_parse(handler, input_stream)
        end
      end
    end

    private
    def init_wrap(fielname)
      store do |writer|
        callback = Proc.new do |records|
          writer.write(records)
          writer.write("\n")
        end

        handler = DumpHandler.new(callback)
        yield handler
      end
    end

  end

end
