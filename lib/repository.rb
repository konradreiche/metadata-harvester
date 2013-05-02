module Repository

  class CKAN
    
    attr_reader :name, :url, :type, :latitude, :longitude

    def initialize(attributes)
      @name = attributes[:name]
      @url = attributes[:url]
      @type = 'CKAN'
      @latitude = attributes[:latitude]
      @longitude = attributes[:longitude]
    end

    def id
      @name
    end

    def _type
      'repository'
    end

    def to_indexed_json
      to_json
    end
  end

end
