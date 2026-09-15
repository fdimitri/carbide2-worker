require_relative 'test_helper'
require_relative '../resolver'

# Resolver is the evict-vs-extend decision, and its comment states the model in
# closed form:
#
#   surcharge = 2 - 3f     break_even = 2/f - 3     (cached=1, uncached=3)
#   :evict at f >= 1/3     :extend below it         :mandatory near the ceiling
#
# These assert the arithmetic against those formulas rather than against
# whatever the code currently returns, so a changed constant or a flipped
# comparison fails instead of being blessed.
class ResolverTest < Minitest::Test
  def resolve(removed:, prompt:, limit: nil)
    Resolver.resolve(removed_tokens: removed, prompt_tokens: prompt, context_limit: limit)
  end

  # --- f, the fraction of the prompt being trimmed ---------------------------

  def test_f_is_removed_over_prompt
    assert_in_delta 0.25, resolve(removed: 250, prompt: 1000).f, 1e-9
  end

  def test_f_is_zero_when_nothing_is_removed
    assert_equal 0.0, resolve(removed: 0, prompt: 1000).f
  end

  def test_f_is_zero_for_an_empty_prompt_rather_than_dividing_by_zero
    # A conversation with no recorded usage yet. Must not raise, must not NaN.
    assert_equal 0.0, resolve(removed: 100, prompt: 0).f
  end

  # --- the economics ---------------------------------------------------------

  def test_surcharge_matches_the_documented_formula
    [0.0, 0.1, 1.0 / 3.0, 0.5, 0.9, 1.0].each do |f|
      r = resolve(removed: (f * 1000).round, prompt: 1000)
      assert_in_delta (2.0 - 3.0 * f), r.surcharge, 1e-3, "f=#{f}"
    end
  end

  def test_surcharge_is_positive_below_two_thirds_and_negative_above
    # The documented crossover: 2 - 3f is positive iff f < 2/3.
    assert_operator resolve(removed: 600, prompt: 1000).surcharge, :>, 0
    assert_operator resolve(removed: 700, prompt: 1000).surcharge, :<, 0
  end

  def test_break_even_matches_the_documented_formula
    [0.4, 0.5, 0.75, 1.0].each do |f|
      r = resolve(removed: (f * 1000).round, prompt: 1000)
      assert_in_delta ((2.0 / f) - 3.0), r.break_even_turns, 1e-3, "f=#{f}"
    end
  end

  def test_recovery_is_break_even_rounded_up
    r = resolve(removed: 400, prompt: 1000)   # break_even = 2.0
    assert_equal 2, r.recovery_turns
  end

  def test_recovery_is_nil_when_nothing_was_removed
    # break_even is infinite at f=0; nil is the honest report, not a giant int.
    r = resolve(removed: 0, prompt: 1000)
    refute r.break_even_turns.finite?
    assert_nil r.recovery_turns
  end

  # --- the verdict -----------------------------------------------------------

  def test_extend_below_the_cache_floor
    # f = 0.1 < 1/3: too small to pay for a full re-prefill.
    assert_equal :extend, resolve(removed: 100, prompt: 1000).verdict
  end

  def test_evict_exactly_at_the_cache_floor
    # f == 1/3 is the documented boundary and is inclusive (>=).
    r = Resolver.resolve(removed_tokens: 1000, prompt_tokens: 3000)
    assert_in_delta (1.0 / 3.0), r.f, 1e-9
    assert_equal :evict, r.verdict
  end

  def test_evict_just_below_the_floor_still_extends
    assert_equal :extend, resolve(removed: 999, prompt: 3000).verdict
  end

  def test_evict_above_the_floor
    assert_equal :evict, resolve(removed: 500, prompt: 1000).verdict
  end

  def test_mandatory_at_the_ceiling_even_when_f_would_extend
    # Nothing removed (f = 0 would be :extend), but the prompt is at the limit:
    # eviction happens regardless of f, because the alternative is death.
    r = resolve(removed: 1, prompt: 950, limit: 1000)
    assert_operator r.f, :<, Resolver::CACHE_FLOOR
    assert_equal :mandatory, r.verdict
  end

  def test_the_ceiling_is_inclusive
    r = resolve(removed: 0, prompt: 900, limit: 1000)   # exactly CEILING_FRACTION
    assert_equal :mandatory, r.verdict
  end

  def test_just_under_the_ceiling_is_not_mandatory
    assert_equal :extend, resolve(removed: 0, prompt: 899, limit: 1000).verdict
  end

  def test_no_context_limit_means_never_mandatory
    # context_limit is nil today (the ceiling is an open question), so the
    # caller must still get :evict/:extend rather than a spurious :mandatory.
    assert_equal :evict, resolve(removed: 500, prompt: 1000).verdict
    assert_equal :extend, resolve(removed: 100, prompt: 1000).verdict
  end

  def test_a_zero_context_limit_is_not_a_ceiling
    # A provider reporting 0 must not make every request mandatory.
    assert_equal :extend, resolve(removed: 0, prompt: 1000, limit: 0).verdict
  end
end
