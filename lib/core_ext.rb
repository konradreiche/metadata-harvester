require 'json'

module CoreExt
  module ClassMethods

    ##
    # Applies JSON.parse on every element.
    # 
    # Returns null on an invalid JSON string.
    #
    def parse_recursively(source)
      if source.is_a?(Hash)
        source.each { |key, value| source[key] = parse_recursively(value) }
      elsif source.is_a?(Array)
        source.map { |item| parse_recursively(item) }
      elsif source.is_a?(String)
        return parse_recursively(JSON.parse(source)) if valid_json?(source)
        source
      else
        source
      end
    end

    def valid_json?(source)
      JSON.parse(source)
      true
    rescue JSON::ParserError
      false
    end

  end

  extend ClassMethods

  def self.included(other)
    other.extend(ClassMethods)
  end    

end

JSON.send(:include, CoreExt)
