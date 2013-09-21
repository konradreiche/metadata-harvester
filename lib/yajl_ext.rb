module Yajl
  module Gzip

    class StreamReader < ::Zlib::GzipReader

      def read(len=nil, buffer=nil)
        if val = super(len)
          unless buffer.nil?
            buffer.replace(val)
            return buffer
          end
          super(len)
        else
          nil
        end
      end

      def self.parse(input, options={}, buffer_size=nil, &block)
        if input.is_a?(String)
          input = StringIO.new(input)
        end
        Yajl::Parser.new(options).parse(new(input), buffer_size, &block)
      end

    end

    class StreamWriter < ::Zlib::GzipWriter

      def self.encode(obj, io)
        Yajl::Encoder.new.encode(obj, new(io))
      end

      def write(string)
        super(Yajl::Encoder.encode(string))
      end

    end
  end
end
