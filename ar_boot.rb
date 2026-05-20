# ar_boot.rb — connect the EventMachine worker to the Rails SQLite database
# without booting the full Rails stack.  Loaded once at worker startup.
require 'active_record'
require 'sqlite3'
require 'fileutils'

RAILS_ENV  = ENV.fetch('RAILS_ENV', 'development')
DB_PATH    = if ENV['DATABASE_URL']
               # Strip the "sqlite3:" scheme prefix if present
               ENV['DATABASE_URL'].sub(/\Asqlite3:/, '')
             else
               File.expand_path("../db/#{RAILS_ENV}.sqlite3", __dir__)
             end

ActiveRecord::Base.establish_connection(
  adapter:  'sqlite3',
  database: DB_PATH,
  timeout:  5000
)

# Enable WAL mode so the worker and the Rails server can both write concurrently.
ActiveRecord::Base.connection.execute('PRAGMA journal_mode=WAL')

# Minimal ApplicationRecord required for model inheritance.
class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end

# Load the FS models and services.
require_relative '../app/models/project'
require_relative '../app/models/project_setting'
require_relative '../app/models/fs_document'
require_relative '../app/models/directory_entry'
require_relative '../app/models/file_change'
require_relative '../app/services/fs_loader'

puts "[ar_boot] connected to #{DB_PATH}"
