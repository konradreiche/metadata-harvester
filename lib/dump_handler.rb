require 'oj'

module MetadataHarvester
  class DumpHandler < Oj::ScHandler

    def array_append(a, value)
    end

    def hash_set(h, key, value)
    end

  end
end

