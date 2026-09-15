# frozen_string_literal: true
#
# Worker unit tests.
#
# Mirrors scripts/test: minitest only, no gems beyond what ships with Ruby, no
# Rails, no EventMachine. The subjects here are the worker's PURE units — the
# ones that take values and return values. Anything that needs the reactor, a
# PTY, a WebSocket or kube is deliberately NOT tested here; that is what the
# substrate tests cover, and mocking those would test the mock.
#
#   ruby test/run_all.rb          # whole suite
#   ruby test/resolver_test.rb    # one file
require 'minitest/autorun'

WORKER_ROOT = File.expand_path('..', __dir__)
$LOAD_PATH.unshift(WORKER_ROOT) unless $LOAD_PATH.include?(WORKER_ROOT)

# A stand-in for a WebSocket. The pure units here key collections by the socket
# OBJECT, so identity is all that matters — never a method on it.
def fake_ws(name = nil)
  Object.new.tap { |o| o.define_singleton_method(:to_s) { name || super() } }
end
