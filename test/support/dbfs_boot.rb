# Shared boot for the DBFS worker tests: a throwaway Postgres database with the
# carbide2-server schema, the DBFS v2 library, models and ProjectFs — the same
# pieces worker/ar_boot.rb loads, without Rails. See the test headers for the
# environment variables.
require 'active_record'
require 'active_support/all'
require 'tmpdir'
require 'uri'

WORKER_DIR = File.expand_path('../..', __dir__)
SERVER_DIR = ENV['CARBIDE_SERVER_DIR'] || [File.expand_path('../carbide2-server', WORKER_DIR), File.expand_path('..', WORKER_DIR)]
  .find { |d| File.exist?(File.join(d, 'lib/dbfs_v2.rb')) } or abort 'set CARBIDE_SERVER_DIR'

url = URI(ENV.fetch('DBFS_IT_DATABASE_URL') { abort 'set DBFS_IT_DATABASE_URL (the database is dropped and recreated)' })
db  = url.path.delete_prefix('/')
abort "refusing to drop #{db.inspect}: the database name must end in _test" unless db.end_with?('_test')
admin = url.dup.tap { |u| u.path = '/postgres' }
ActiveRecord::Base.establish_connection(admin.to_s)
ActiveRecord::Base.connection.drop_database(db)
ActiveRecord::Base.connection.create_database(db)
ActiveRecord::Base.establish_connection(url.to_s)
ActiveRecord::Schema.verbose = false
load File.join(SERVER_DIR, 'db/schema.rb')

ENV['PROJECTS_ROOT'] = Dir.mktmpdir('carbide-dbfs-it-projects')
ActiveRecord::Base.belongs_to_required_by_default = true # as under Rails load_defaults
class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end
require File.join(SERVER_DIR, 'lib/dbfs_v2')
Dir[File.join(SERVER_DIR, 'app/models/*.rb')].sort.each { |f| require f }
require File.join(SERVER_DIR, 'app/services/project_fs')
require File.join(SERVER_DIR, 'app/services/fs_loader')

