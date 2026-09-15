require_relative 'test_helper'
require_relative '../agent_tools'

# AgentTools.compute_anchored_edits turns the agent's anchored edits into
# FileChange specs. It is pure (buffer in, buffer + specs out) and it guards
# every file write the agent makes, so a wrong offset here does not fail loudly
# — it corrupts a file.
#
# The minimal-span behaviour is the part worth pinning: push_replace_spec keeps
# the shared prefix and suffix in place so a pure insertion emits ONE insert
# rather than a delete+insert of identical text. The comment says that is one
# FileChange row and half the fs/change broadcast; the tests say what it does.
class AnchoredEditsTest < Minitest::Test
  def edits(*list) = list

  # The edit hash is STRING-keyed: these are JSON tool arguments on the wire,
  # and compute_anchored_edits reads ed['replace_all'] etc. Keyword options are
  # converted here so the tests can read naturally without pretending the API
  # takes symbols.
  def one(buffer, old, new, **opts)
    ed = { 'old_string' => old, 'new_string' => new }
    opts.each { |k, v| ed[k.to_s] = v }
    AgentTools.compute_anchored_edits(buffer, [ed])
  end

  # --- the helpers ----------------------------------------------------------

  def test_char_offset_to_line_char
    s = "abc\ndef"
    assert_equal [0, 0], AgentTools.char_offset_to_line_char(s, 0)
    assert_equal [0, 3], AgentTools.char_offset_to_line_char(s, 3)   # before the \n
    assert_equal [1, 0], AgentTools.char_offset_to_line_char(s, 4)   # after the \n
    assert_equal [1, 3], AgentTools.char_offset_to_line_char(s, 7)
  end

  def test_char_offset_clamps_a_negative_offset
    assert_equal [0, 0], AgentTools.char_offset_to_line_char("abc", -5)
  end

  def test_common_prefix_and_suffix
    assert_equal 2, AgentTools.common_prefix_len("abc", "abd")
    assert_equal 0, AgentTools.common_prefix_len("", "abc")
    assert_equal 3, AgentTools.common_prefix_len("abc", "abc")
    assert_equal 2, AgentTools.common_suffix_len("abc", "xbc")
    assert_equal 0, AgentTools.common_suffix_len("abc", "x")
    assert_equal 3, AgentTools.common_suffix_len("abc", "abc")
  end

  # --- the minimal-span rule ------------------------------------------------

  def test_a_pure_insertion_emits_only_an_insert
    # "b" -> "bX": nothing is deleted, so there must be no delete spec.
    r = one("abc", "b", "bX")
    assert_equal "abXc", r[:buffer]
    assert_equal 1, r[:specs].length
    assert_equal 'insertDataSingleLine', r[:specs][0][:change_type]
    assert_equal "X", r[:specs][0][:change_data][:data]
  end

  def test_a_pure_deletion_emits_only_a_delete
    r = one("abc", "b", "")
    assert_equal "ac", r[:buffer]
    assert_equal 1, r[:specs].length
    assert_equal 'deleteDataSingleLine', r[:specs][0][:change_type]
  end

  def test_a_shared_suffix_is_preserved_not_deleted_and_reinserted
    # aXb -> aYb: only the middle character differs, so the delete must cover
    # exactly that character and the insert must carry only its replacement.
    #
    # This is the case that exercises the shared-suffix term. Dropping it
    # (`del_end = endoff` instead of `endoff - sfx`) produces the SAME buffer —
    # it deletes "Xb" and re-inserts "Yb" — so asserting the buffer alone would
    # not notice. It is one FileChange row and one broadcast per file either
    # way, which is the cost the minimal span exists to avoid.
    r = one("aXb", "aXb", "aYb")
    assert_equal "aYb", r[:buffer]

    del = r[:specs].find { |s| s[:change_type].start_with?('delete') }
    ins = r[:specs].find { |s| s[:change_type].start_with?('insert') }
    assert_equal({ startLine: 0, startChar: 1, endLine: 0, endChar: 2 }, del[:change_data])
    assert_equal "Y", ins[:change_data][:data]
  end

  def test_a_full_replacement_emits_a_delete_and_an_insert
    r = one("abc", "b", "Z")
    assert_equal "aZc", r[:buffer]
    assert_equal %w[deleteDataSingleLine insertDataSingleLine],
                 r[:specs].map { |s| s[:change_type] }
  end

  def test_a_multiline_insertion_is_a_multi_line_change
    r = one("ab", "b", "b\nc")
    assert_equal "ab\nc", r[:buffer]
    assert_equal 'insertDataMultiLine', r[:specs][0][:change_type]
  end

  def test_offsets_are_computed_against_the_buffer_before_the_change
    # Replacing the "b" on line 1: the spec must point at line 1, char 0.
    r = one("a\nb\nc", "b", "B")
    ins = r[:specs].find { |s| s[:change_type].start_with?('insert') }
    assert_equal 1, ins[:change_line] || ins[:start_line]
    assert_equal 0, ins[:start_char]
  end

  # --- matching rules -------------------------------------------------------

  def test_missing_old_string_errors_with_the_edit_index
    r = one("abc", "zzz", "y")
    assert_match(/not found/, r[:error])
    assert_equal 0, r[:edit_index]
    assert_equal 0, r[:occurrences]
    refute r.key?(:buffer)
  end

  def test_an_empty_old_string_is_refused
    r = one("abc", "", "y")
    assert_match(/must not be empty/, r[:error])
  end

  def test_an_ambiguous_match_is_refused_rather_than_guessing
    r = one("aaa", "a", "b")
    assert_match(/matched 3 times/, r[:error])
    assert_equal 3, r[:occurrences]
    refute r.key?(:buffer)
  end

  def test_replace_first_takes_the_first_occurrence
    r = one("aaa", "a", "b", replace_first: true)
    assert_equal "baa", r[:buffer]
    assert_equal 1, r[:applied]
  end

  def test_replace_all_takes_every_occurrence
    r = one("aaa", "a", "b", replace_all: true)
    assert_equal "bbb", r[:buffer]
    assert_equal 3, r[:applied]
  end

  def test_replace_all_handles_a_length_changing_replacement
    # The shift bookkeeping: replacing every "a" with "aaaa" must not walk
    # into its own output.
    r = one("a-a-a", "a", "aaaa", replace_all: true)
    assert_equal "aaaa-aaaa-aaaa", r[:buffer]
    assert_equal 3, r[:applied]
  end

  def test_expected_count_matches
    r = one("aaa", "a", "b", expected_count: 3, replace_all: true)
    assert_equal "bbb", r[:buffer]
  end

  def test_expected_count_mismatch_is_an_error
    r = one("aaa", "a", "b", expected_count: 2)
    assert_match(/expected 2 occurrence/, r[:error])
    assert_equal 3, r[:occurrences]
    assert_equal 2, r[:expected_count]
  end

  def test_fail_on_multiple_refuses_a_repeated_match
    r = one("aaa", "a", "b", fail_on_multiple: true)
    assert_match(/fail_on_multiple/, r[:error])
  end

  def test_fail_on_multiple_allows_a_single_match
    r = one("abc", "b", "Z", fail_on_multiple: true)
    assert_equal "aZc", r[:buffer]
  end

  # --- sequencing -----------------------------------------------------------

  def test_edits_apply_in_order_against_the_evolving_buffer
    r = AgentTools.compute_anchored_edits("one two", edits(
      { 'old_string' => "one", 'new_string' => "1" },
      { 'old_string' => "two", 'new_string' => "2" },
    ))
    assert_equal "1 2", r[:buffer]
    assert_equal 2, r[:applied]
  end

  def test_a_later_edit_sees_the_earlier_edits_output
    # The second edit's anchor only exists after the first one lands, which is
    # the whole point of editing a buffer rather than the original text.
    r = AgentTools.compute_anchored_edits("a", edits(
      { 'old_string' => "a", 'new_string' => "ab" },
      { 'old_string' => "ab", 'new_string' => "abc" },
    ))
    assert_equal "abc", r[:buffer]
  end

  def test_the_input_buffer_is_not_mutated
    original = "abc"
    one(original, "b", "Z")
    assert_equal "abc", original
  end

  def test_a_failing_second_edit_reports_its_own_index
    r = AgentTools.compute_anchored_edits("abc", edits(
      { 'old_string' => "a", 'new_string' => "A" },
      { 'old_string' => "zzz", 'new_string' => "Z" },
    ))
    assert_equal 1, r[:edit_index]
    assert_match(/not found/, r[:error])
  end
end
