# ar_boot.rb — connect the EventMachine worker to the Rails Postgres database
# without booting the full Rails stack.  Loaded once at worker startup.
require 'active_record'
require 'pg'
require 'fileutils'

RAILS_ENV = ENV.fetch('RAILS_ENV', 'development')

# EventMachine's defer thread pool is ~20 threads. Each one that touches AR
# leases a connection for the life of the thread (Rails 8 sticky checkout)
# unless we return it. A pool of 5 (Rails' default, and what DATABASE_URL
# alone gives you) is smaller than that pool, so the 6th concurrent ask /
# FsLoader / reconcile / tool write waits 5s and raises ConnectionTimeoutError.
# The worker is one process serving the whole workspace: size the pool for
# the defer threads, not for Puma.
WORKER_DB_POOL = Integer(ENV.fetch('WORKER_DB_POOL', '25'))
WORKER_DB_CHECKOUT_S = Float(ENV.fetch('WORKER_DB_CHECKOUT', '5'))

_worker_db = if ENV['DATABASE_URL'] && !ENV['DATABASE_URL'].empty?
               ActiveRecord::DatabaseConfigurations::ConnectionUrlResolver.new(ENV['DATABASE_URL']).to_hash
             else
               {
                 adapter:  'postgresql',
                 host:     ENV.fetch('POSTGRES_HOST', 'postgres'),
                 port:     ENV.fetch('POSTGRES_PORT', 5432).to_i,
                 username: ENV.fetch('POSTGRES_USER', 'carbide'),
                 password: ENV.fetch('POSTGRES_PASSWORD', 'carbide'),
                 database: ENV.fetch('POSTGRES_DB',
                   RAILS_ENV == 'production' ? 'carbide2_production' : 'carbide2_development')
               }
             end
_worker_db = _worker_db.transform_keys(&:to_sym)
_worker_db[:pool] = WORKER_DB_POOL
_worker_db[:checkout_timeout] = WORKER_DB_CHECKOUT_S
# DATABASE_URL often carries no pool; a leftover `?pool=5` must not win.
_worker_db.delete(:max_connections)
ActiveRecord::Base.establish_connection(_worker_db)

# Return this thread's leased connection(s) to the pool. Call from every
# EM.defer ensure, and before any long wait (model HTTP, shell) on a thread
# that has already used AR. The reactor thread keeps its lease: flush/watch
# hit the DB every 100ms and re-checkout would be noise.
def worker_release_db!
  return if defined?(EM) && EM.reactor_running? && EM.reactor_thread?
  ActiveRecord::Base.connection_handler.clear_active_connections!(:all)
end

# Minimal ApplicationRecord required for model inheritance.
class ApplicationRecord < ActiveRecord::Base
  self.abstract_class = true
end

# Load every model under app/models. Worker never instantiates most of these,
# but ActiveRecord needs the constants resolvable for belongs_to/has_many
# validators (e.g. AgentConversation belongs_to :user).
Dir[File.expand_path('../app/models/*.rb', __dir__)].sort.each do |f|
  require f
end
# DBFS v2: the library (outside any autoloader) and the carbide2 seam over it.
require_relative '../lib/dbfs_v2'
require_relative '../app/services/project_fs'
require_relative '../app/services/fs_loader'
require_relative 'db_pool'
WorkerDbPool.install!

puts "[ar_boot] connected to Postgres at #{ENV.fetch('POSTGRES_HOST', '?')}:#{ENV.fetch('POSTGRES_PORT', '?')} pool=#{WORKER_DB_POOL}"
