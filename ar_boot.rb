# ar_boot.rb — connect the EventMachine worker to the Rails Postgres database
# without booting the full Rails stack.  Loaded once at worker startup.
require 'active_record'
require 'pg'
require 'fileutils'

RAILS_ENV = ENV.fetch('RAILS_ENV', 'development')

if ENV['DATABASE_URL'] && !ENV['DATABASE_URL'].empty?
  ActiveRecord::Base.establish_connection(ENV['DATABASE_URL'])
else
  ActiveRecord::Base.establish_connection(
    adapter:  'postgresql',
    host:     ENV.fetch('POSTGRES_HOST', 'postgres'),
    port:     ENV.fetch('POSTGRES_PORT', 5432).to_i,
    username: ENV.fetch('POSTGRES_USER', 'carbide'),
    password: ENV.fetch('POSTGRES_PASSWORD', 'carbide'),
    database: ENV.fetch('POSTGRES_DB',
      RAILS_ENV == 'production' ? 'carbide2_production' : 'carbide2_development'),
    pool:     ENV.fetch('RAILS_MAX_THREADS', 5).to_i
  )
end

# Minimal ApplicationRecord required for model inheritance.
class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end

# Load the FS models and services.
require_relative '../app/models/project'
require_relative '../app/models/project_membership'
require_relative '../app/models/project_setting'
require_relative '../app/models/fs_document'
require_relative '../app/models/directory_entry'
require_relative '../app/models/file_change'
require_relative '../app/models/agent'
require_relative '../app/services/fs_loader'

puts "[ar_boot] connected to Postgres at #{ENV.fetch('POSTGRES_HOST', '?')}:#{ENV.fetch('POSTGRES_PORT', '?')}"

