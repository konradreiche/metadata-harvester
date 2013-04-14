module Metadata

  class Dataset

    def initialize(attributes={})
      @attributes = attributes
      @attributes.each_pair do |name, value|
        instance_variable_set :"@#{name}", value unless meta_metadata? name
      end
    end

    def meta_metadata?(name)
      ['_score', '_type', '_index', '_version', 'sort', 'highlight',
       '_explanation'].include? name
    end
  end

end
