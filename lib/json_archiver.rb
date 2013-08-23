class JsonArchiver

  def initialize(id)
    directory = "archive/#{id}"
    FileUtils.mkdir_p(directory) unless Dir::exists?(directory)
    @path = "#{directory}/#{Date.today}-#{id}.json"
  end

  def store(records)
    File.open(@path, 'a') { |file| JSON.dump(records, file) }
  end

end
