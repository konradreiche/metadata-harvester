module Metadata

  class Dataset

    def initialize(attributes={}, repository)
      @attributes = Hash.new
      @attributes[:document] = Dataset.recursive_symbolize_keys(attributes)
      @attributes[:repository] = repository
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

  end

end
