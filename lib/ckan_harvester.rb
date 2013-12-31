module MetadataHarvester
  class CkanHarvester < Harvester

    def initialize(id, url, rows=1000)
      @rows = rows
      super(id, url)
    end

    def harvest
      total = number_datasets
    end

    def number_datasets
      query_url = "#{@url}/action/package_search"
      curl = curl(url, :post)
      curl.perform

      response = Oj.load(curl.body_str)
      response['result']['count']
    rescue JSON::ParserError, Curl::Err::ConnectionFailedError, 
      Curl::Err::PartialFileError
      timeout()
      retry
    ensure
      curl.close() unless curl.nil?
    end

  end
end
