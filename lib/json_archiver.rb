require 'file_utils'
require 'archive'
require 'zip'
require 'zlib'

class JsonArchiver

  def initialize(id)
    @directory = File.expand("../archive", File.dirname(__FILE__))
    FileUtils.mkdir_p(@directory)
    @path = "#{@directory}/#{Date.today}-#{id}"
  end

  def store(records)
    File.open(@destination, 'a') { |file| JSON.dump(records, file) }
  end

  def download(url)
    curl = Curl::Easy.new(curl)
    file = File.new(@path, 'a')
    curl.on_body { |data| file.write(data) }
    file.close()
  end

  def extract(type)
    case type
    when :gzip
      extract_gzip()
    when :zip
      extract_zip()
    else
      raise TypeError, "The filetype #{type} is unknown."
    end
  end

  def extract_gzip
    Zlib::GzipReader.open(@path) do |gz|
      File.new("#{@path}.json", 'w') do |g|
        IO.copy_stream(gz, g)
      end
    end
  end

  def extract_zip
    Zip::File.open(@path).first.extract("#{path}.json")
  end

end
