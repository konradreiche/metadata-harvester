require 'oj'

module MetadataHarvester
  class DumpHandler < Oj::ScHandler

    attr_reader :records

    THRESHOLD = 1_000

    def initialize
      @records, @stack = [], []
      @hash_closed = false
    end

    def hash_start
      @stack << Hash.new
    end

    def hash_end
      @hash_closed = true
      @records << @stack.pop if @stack.size == 1
    end

    # Constructions the record in a document-depth fashion.
    #
    # (1) Base case: assign the value to the current hash on stack.
    #
    # (2) Recursive case: assign completed inner hash (last on stack) to
    #     previous hash (second last on stack).
    #
    def hash_set(hash, key, value)
      if @hash_closed
        closed_hash = @stack.pop
        @stack.last[key] = closed_hash
        @hash_closed = false
      else
        @stack.last[key] = value
      end
    end

  end
end

