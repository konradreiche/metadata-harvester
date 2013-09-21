module MetadataHarvester
  class Prettier < Sidekiq::Logging::Pretty

    def add(id)
      @ids = @ids || Hash.new
      @ids[Process.pid] = id
    end

    def call(severity, time, program_name, message)
      id = defined?(@ids) ? @ids[Process.pid] : nil
      "#{time.strftime("%c")} #{id} [#{severity}] #{message}\n" 
    end

  end
end
