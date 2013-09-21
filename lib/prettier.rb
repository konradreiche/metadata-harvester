module MetadataHarvester
  class Prettier < Sidekiq::Logging::Pretty

    def add(id)
      Thread.current[:__harvester_id__] = id
    end

    def call(severity, time, program_name, message)
      id = Thread.current[:__harvester_id__]
      id = " #{id}" unless id.nil?
      "#{time.strftime("%c")}#{id} [#{severity}] #{message}\n" 
    end

  end
end
