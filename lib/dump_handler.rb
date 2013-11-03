require 'oj'

module MetadataHarvester
  class DumpHandler < Oj::ScHandler

    attr_reader :records

    THRESHOLD = 1000

    def initialize(callback=nil)
      @callback = callback
      @depth = 0
    end

    def hash_start
      @depth += 1
      Hash.new
    end

    # Set the pointer if records is not initialized yet.
    #
    def array_start
      @working_set ? Array.new : @working_set = Array.new
    end

    def array_append(array, value)
      array << value
      @records = @working_set if @depth == 0

      if @callback and @working_set.maybe.size == THRESHOLD
        @callback.call(@working_set)
        @working_set = nil
      end
    end

    def hash_end
      @depth -= 1
    end

    def array_end
      #@callback.call(@working_set) if @callback and @depth == 0
    end

    def hash_set(hash, key, value)
      hash[key] = value
    end

  end
end

