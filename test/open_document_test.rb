require_relative 'test_helper'
require_relative '../open_document'

# OpenDocument tracks which sockets have a file open. It is what decides who
# receives `fs/change` fan-out, so its membership rules are load-bearing:
# everything here is about WHO, never about content.
#
# The key is the socket OBJECT (`@clients` is keyed by `s`, and `others(ws)`
# rejects by identity). That is what makes one entry per connection, however
# many panes that connection is rendering — the property the client-side
# one-tab-per-file rule exists to be compatible with.
class OpenDocumentTest < Minitest::Test
  def setup
    @doc = OpenDocument.new(1, '/README.md')
  end

  def test_a_new_document_has_no_clients
    assert @doc.empty?
    assert_equal [], @doc.viewers
    assert_equal [], @doc.others(fake_ws(:nobody))
  end

  def test_add_client_registers_a_viewer
    ws = fake_ws
    @doc.add_client(ws, user_id: 7, name: "frank")
    refute @doc.empty?
    assert @doc.member?(ws)
    assert_equal [{ user_id: 7, name: "frank", cursor: nil }], @doc.viewers
  end

  def test_adding_the_same_socket_twice_keeps_one_entry
    # One socket = one viewer, no matter how many panes it renders.
    ws = fake_ws
    @doc.add_client(ws, user_id: 7, name: "frank")
    @doc.add_client(ws, user_id: 7, name: "frank")
    assert_equal 1, @doc.viewers.length
  end

  def test_re_adding_resets_that_clients_cursor
    # Documented behaviour: add_client writes a fresh entry, so a re-open
    # clears the cursor until the next move. Pinned so it stays a decision.
    ws = fake_ws
    @doc.add_client(ws, user_id: 7, name: "frank")
    @doc.update_cursor(ws, line: 12, char: 4)
    assert_equal({ line: 12, char: 4 }, @doc.viewers[0][:cursor])

    @doc.add_client(ws, user_id: 7, name: "frank")
    assert_nil @doc.viewers[0][:cursor]
  end

  def test_distinct_sockets_are_distinct_viewers
    a, b = fake_ws, fake_ws
    @doc.add_client(a, user_id: 1, name: "a")
    @doc.add_client(b, user_id: 2, name: "b")
    assert_equal 2, @doc.viewers.length
  end

  def test_remove_client_drops_only_that_one
    a, b = fake_ws, fake_ws
    @doc.add_client(a, user_id: 1, name: "a")
    @doc.add_client(b, user_id: 2, name: "b")
    @doc.remove_client(a)

    refute @doc.member?(a)
    assert @doc.member?(b)
    assert_equal [{ user_id: 2, name: "b", cursor: nil }], @doc.viewers
  end

  def test_removing_a_non_member_is_a_no_op
    ws = fake_ws
    @doc.add_client(ws, user_id: 1, name: "a")
    @doc.remove_client(fake_ws)     # never added
    assert_equal 1, @doc.viewers.length
    refute @doc.empty?
  end

  def test_removing_the_last_client_empties_the_document
    ws = fake_ws
    @doc.add_client(ws, user_id: 1, name: "a")
    @doc.remove_client(ws)
    assert @doc.empty?
  end

  def test_update_cursor_records_a_position_for_a_member
    ws = fake_ws
    @doc.add_client(ws, user_id: 1, name: "a")
    @doc.update_cursor(ws, line: 3, char: 9)
    assert_equal({ line: 3, char: 9 }, @doc.viewers[0][:cursor])
  end

  def test_update_cursor_from_a_non_member_is_ignored
    # A cursor frame from a socket that never opened the file must not create
    # an entry — otherwise anyone could make themselves a fan-out target.
    ws = fake_ws
    @doc.update_cursor(ws, line: 3, char: 9)
    assert @doc.empty?
  end

  # --- others, the fan-out set ---------------------------------------------

  def test_others_excludes_the_given_socket
    a, b, c = fake_ws, fake_ws, fake_ws
    [a, b, c].each_with_index { |ws, i| @doc.add_client(ws, user_id: i, name: i.to_s) }
    assert_equal 2, @doc.others(a).length
    refute_includes @doc.others(a), a
    assert_includes @doc.others(a), b
    assert_includes @doc.others(a), c
  end

  def test_others_for_a_single_viewer_is_empty
    # The author's own edit is not echoed back to them: the client applies it
    # locally and there is no loopback.
    a = fake_ws
    @doc.add_client(a, user_id: 1, name: "a")
    assert_equal [], @doc.others(a)
  end

  def test_others_for_a_non_member_returns_everyone
    a = fake_ws
    @doc.add_client(a, user_id: 1, name: "a")
    assert_equal [a], @doc.others(fake_ws)
  end

  # --- viewer snapshots -----------------------------------------------------

  def test_viewers_reports_each_clients_cursor
    a, b = fake_ws, fake_ws
    @doc.add_client(a, user_id: 1, name: "a")
    @doc.add_client(b, user_id: 2, name: "b")
    @doc.update_cursor(a, line: 1, char: 2)

    by_user = @doc.viewers.each_with_object({}) { |v, h| h[v[:user_id]] = v[:cursor] }
    assert_equal({ line: 1, char: 2 }, by_user[1])
    assert_nil by_user[2]
  end
end
