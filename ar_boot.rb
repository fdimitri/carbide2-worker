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

# Devise needs Devise.setup to have run before any `devise :...` macro fires
# (the User model uses it). We don't boot Rails in the worker, so do the
# bare minimum here: load the gem and configure secret_key so its modules
# can register cleanly.
require 'devise'
require 'devise/orm/active_record'
Devise.setup do |config|
  config.secret_key = ENV.fetch('DEVISE_SECRET_KEY',
                                ENV.fetch('SECRET_KEY_BASE', 'worker-devise-secret-placeholder'))
  config.mailer_sender = ENV.fetch('DEVISE_MAILER_SENDER', 'noreply@example.com')
end

# Load every model under app/models. Worker never instantiates most of these,
# but ActiveRecord needs the constants resolvable for belongs_to/has_many
# validators (e.g. AgentConversation belongs_to :user).
Dir[File.expand_path('../app/models/*.rb', __dir__)].sort.each do |f|
  require f
end
require_relative '../app/services/fs_loader'

puts "[ar_boot] connected to Postgres at #{ENV.fetch('POSTGRES_HOST', '?')}:#{ENV.fetch('POSTGRES_PORT', '?')}"

