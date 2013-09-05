module CoreExt
  module ClassMethods

    def parse_recursively(source)
      if source.is_a?(Hash)
        source.each { |key, value| source[key] = parse_recursively(value) }
      elsif source.is_a?(Array)
        source.map { |item| parse item }
      else
        begin
          result = JSON.parse(source)
          parse_recursively(result)
        rescue JSON::ParserError, TypeError
          source
        end
      end
    end

  end

  extend ClassMethods

  def self.included(other)
    other.extend(ClassMethods)
  end    

end

JSON.send(:include, CoreExt)
