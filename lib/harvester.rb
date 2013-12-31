module MetadataHarvester
  class Harvester

    TIMEOUT_LOWER_CAP = 30.0
    TIMEOUT_UPPER_CAP = 600.0

    def initialize(id, url)
      @id = id
      @url = url
      @timeout = TIMEOUT_LOWER_CAP
    end

    def curl(url, method, parameter={})
      settings = Proc.new do |curl|
        curl.ssl_verify_peer = false
        curl.follow_location = true
      end

      case method
      when :get
        Curl.get(url, parameter, &settings)
      when :post
        Curl.post(url, JSON.dump(parameter), &settings)
      end
    end

    ##
    # Timeout method used to wait a timespan before the next request.
    #
    def timeout
      time = time_ago_in_words(@timeout.seconds.from_now)
      logger.warn("Response: Parse Error. Retry in #{time}")

      sleep(@timeout)
      @timeout *= 2 if @timeout < TIMEOUT_UPPER_CAP
    end

  end
end
