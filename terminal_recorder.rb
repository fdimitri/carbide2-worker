# TerminalRecorder — append-only writer for asciinema v2 cast files.
# Copyright (C) 2025 Carbide2 contributors. GPLv3.
#
# File format (one JSON value per line, LF-separated):
#   header: {"version":2,"width":W,"height":H,"timestamp":<unix>,"title":"..."}
#   frames: [<elapsed_seconds_float>, "o", "<raw bytes as JSON string>"]
#
# Files render natively in `asciinema play <file>` and in the asciinema-player
# JS library. We only record output ("o") frames — user input is echoed back
# through the PTY as output anyway, so a single channel captures everything
# the user saw. (If we ever want strict audit-grade input/output separation,
# add a parallel "i" frame stream at write_input time.)
#
# Files live under TERMINAL_RECORDINGS_ROOT (env, default
# Rails.root/storage/terminal_recordings/). Layout:
#   <project_id>/<recording_id>.cast
#
# The DB row in `terminal_recordings` is the index; this class is purely
# about disk IO and is what TerminalInstance tees PTY chunks into.
require 'json'
require 'fileutils'

class TerminalRecorder
  attr_reader :recording_id, :file_path, :started_mono, :byte_count

  def initialize(recording_id:, abs_file_path:, cols:, rows:, title: nil)
    @recording_id  = recording_id
    @file_path     = abs_file_path
    @cols          = cols.to_i
    @rows          = rows.to_i
    @title         = title.to_s
    @byte_count    = 0
    @started_mono  = nil   # set on open!
    @started_wall  = nil
    @io            = nil
    @mutex         = Mutex.new
  end

  def open!
    FileUtils.mkdir_p(File.dirname(@file_path))
    @io = File.open(@file_path, 'wb')
    @io.sync       = true
    @started_mono  = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    @started_wall  = Time.now.to_i
    header = {
      version:   2,
      width:     @cols,
      height:    @rows,
      timestamp: @started_wall
    }
    header[:title] = @title unless @title.empty?
    @io.write(JSON.generate(header) + "\n")
    self
  end

  # Append a single output frame. `data` is the raw bytes the PTY produced.
  # JSON.generate handles all the escaping (NUL, backslash, controls,
  # high-bit bytes via \uXXXX) so the file stays single-line-per-frame.
  def write_output(data)
    return if @io.nil? || @io.closed?
    return if data.nil? || data.empty?
    @mutex.synchronize do
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - @started_mono
      # Force ASCII-8BIT → UTF-8 with replacement so JSON.generate doesn't
      # blow up on arbitrary PTY output (which is often invalid UTF-8 mid-
      # stream during e.g. compiler progress bars).
      safe = data.dup.force_encoding('UTF-8')
      safe = safe.scrub('') unless safe.valid_encoding?
      line = JSON.generate([elapsed.round(6), 'o', safe])
      @io.write(line + "\n")
      @byte_count += data.bytesize
    end
  rescue => e
    warn "[TerminalRecorder:#{@recording_id}] write failed: #{e.class}: #{e.message}"
  end

  def close!
    @mutex.synchronize do
      @io&.close rescue nil
      @io = nil
    end
  end

  def closed?
    @io.nil? || @io.closed?
  end
end
