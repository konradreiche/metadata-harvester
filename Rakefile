require 'geocoder'
require 'rake'
require 'yaml'

Geocoder.configure(timeout: 180)

namespace :generate do
  desc "Extend the list of repositories by their geographical coordinates"
  task :coordinates, :file do |t, args|

    file = args.key?(:file) ? args[:file] : 'repositories.yml'

    catalog = YAML.load(File.read(file))
    catalog['repositories'].each do |repository|

      latitude = repository.key?('latitude')
      longitude = repository.key?('longitude')

      if not latitude or not longitude
        city = repository['location']
        location = Geocoder.search(city).first

        repository['latitude'] = location.latitude
        repository['longitude'] = location.longitude
      end

    end

    # add an extra new line for a more readable result
    yaml = catalog.to_yaml.gsub(/longitude: [-]?\d+[.]\d+\n+/, "\\0\n")
    File.open('repositories.yml', 'w') { |f| f.write(yaml) }
  end
end



