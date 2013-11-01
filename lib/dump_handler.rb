require 'oj'

module MetadataHarvester
  class DumpHandler < Oj::ScHandler

    attr_reader :record

    def initialize
      @stack = []
      @close = false
    end

    def hash_start
      @stack.push(Hash.new)
    end

    def hash_end
      @close = true
    end

    def hash_set(h, key, value)
      puts "#{key} => #{value}"
      if @close
        inner = @stack.pop
        @stack.last[key] = inner
      else
        @stack.last[key] = value
      end
    end

    def record
      @stack.first
    end

  end
end

