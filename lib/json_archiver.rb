require 'fileutils'
require 'zip'
require 'zlib'

module MetadataHarvester
  class JsonArchiver

    def initialize(id)
      @directory = File.expand_path("../archive/#{id}", File.dirname(__FILE__))
      FileUtils.mkdir_p(@directory)
      @path = "#{@directory}/#{Date.today}-#{id}"
    end

    def store(records)
      json_file = "#{@path}.json"
      File.delete(json_file) if File.exists?(json_file)
      File.open(json_file, 'a') { |file| JSON.dump(records, file) }
    end

    def download(url)
      Sidekiq.logger.info("Download #{url}")
      curl = Curl::Easy.new(url)
      file = File.new(@path, 'w')
      curl.on_body { |data| file.write(data) }
      curl.perform
      file.close()
    end

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

    def extract_gzip
      Zlib::GzipReader.open(@path) do |gz|
        File.open("#{@path}.json", 'w') do |g|
          IO.copy_stream(gz, g)
        end
      end
    end

    def extract_zip
      Zip::File.open(@path).first.extract("#{@path}.json")
    end

  end
end
