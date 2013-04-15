require 'rails'

module Metadata


  class Dataset

    def initialize(attributes={})
      @attributes = Dataset.recursive_symbolize_keys attributes
    end

    def self.recursive_symbolize_keys(h)
      case h
      when Hash
        Hash[
          h.map do |k, v|
            [ k.respond_to?(:to_sym) ? k.to_sym : k, recursive_symbolize_keys(v) ]
          end
        ]
      when Enumerable
        h.map { |v| recursive_symbolize_keys(v) }
      else
        h
      end
    end

    def meta_metadata?(name)
      ['_score', '_type', '_index', '_version', 'sort', 'highlight',
       '_explanation'].include? name
    end
  end

end
