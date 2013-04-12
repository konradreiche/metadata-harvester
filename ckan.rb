require_relative 'metadata'

module CKAN

  class Dataset < Metadata::Dataset

    attr_reader :name, :title, :author, :author_email, :maintainer,
      :maintainer_email, :notes, :groups, :tags, :url, :type, :resources,
      :license_id, :extras

    def type
      'dataset'
    end

    def to_indexed_json
      @attributes.to_json
    end

  end

end
