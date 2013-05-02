module Repository

  class CKAN
    
    attr_reader :name, :url, :latitude, :longitude

    def initialize(attributes)
      @name = attributes[:name]
      @url = attributes[:url]
      @latitude = attributes[:latitude]
      @longitude = attributes[:longitude]
    end

    def id
      @name
    end

    def type
      'CKAN'
    end

    def _type
      'repository'
    end

    def to_indexed_json
      to_json
    end
  end

end
