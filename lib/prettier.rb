module MetadataHarvester
  class Prettier < Sidekiq::Logging::Pretty

    def call(severity, time, program_name, message)
      "#{time.strftime("%c")}#{context} [#{severity}] #{message}\n" 
    end

  end
end
