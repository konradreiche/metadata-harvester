module MetadataHarvester
  class CkanHarvester < Harvester

    def initialize(id, url, rows=1000)
      @archiver = Archiver.new(id, type, date, count)
      @rows = rows
      super(id, url)
    end

    def harvest
      total = number_datasets
      steps = total.fdiv(rows).ceil

      @archiver.store do |writer|
        before = Time.new
        steps.times do |i|
          rows = total - (i * rows) if i == steps - 1
          records = query(url, rows, i, id)

          records = unify(records)
          Oj.to_stream(writer, records)
          writer.write("\n")

          before = eta(before, steps, i, url)
        end
      end
    end

    def number_datasets
      query_url = "#{@url}/action/package_search"
      curl = curl(url, :post)
      curl.perform

      response = Oj.load(curl.body_str)
      response['result']['count']
    rescue JSON::ParserError, Curl::Err::ConnectionFailedError, 
      Curl::Err::PartialFileError
      timeout()
      retry
    ensure
      curl.close() unless curl.nil?
    end

  end
end
