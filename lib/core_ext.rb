module CoreExt
  module ClassMethods

    def parse_recursively(source)
      if source.is_a?(Hash)
        source.each { |key, value| source[key] = parse_recursively(value) }
      elsif source.is_a?(Array)
        source.map { |item| parse item }
      else
        result = JSON.parse(source)
        parse_recursively(result)
      rescue JSON::ParseError, TypeError
        source = nil if source.is_a?(String)and source.empty?
        source = nil if source.is_a?(Array) and source.empty?
        source = nil if source.is_a?(Hash) and source.empty?
        return source
      end
    end
   
    extend ClassMethods

    def self.included(other)
      other.extend(ClassMethods)
    end    

  end
end

JSON.send(:include, CoreExt)
