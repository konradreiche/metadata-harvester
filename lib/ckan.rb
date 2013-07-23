require_relative 'metadata'

module CKAN

  class Dataset < Metadata::Dataset

    def _type
      'ckan'
    end

    def to_indexed_json
      @attributes.to_json
    end

  end

end
