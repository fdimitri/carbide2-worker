# resolver.rb — ADR-033 phase 2: the evict-vs-extend decision for tool history.
#
# Cache economics: cached tokens cost ~1/3 of uncached (DeepSeek's hit price).
# Eviction is front-trimming, so removing fraction f of the prompt re-prefills
# the surviving suffix UNcached on the trim turn. Normalize cached=1, uncached=3:
#
#   surcharge   = 3(1-f) - 1 = 2 - 3f      (positive iff f < 2/3)
#   break_even  = (2-3f)/f   = 2/f - 3      (turns to recover the surcharge)
#
# Verdicts:
#   :mandatory — prompt_tokens is within CEILING_FRACTION of the context limit;
#                eviction happens regardless of f (the alternative is death).
#   :evict     — f clears CACHE_FLOOR; the trim pays for itself.
#   :extend    — f is too small to justify a full re-prefill; hold and wait for
#                more to accumulate into a worthwhile batch.
#
# This is a pure function: no DB, no session. It reports "likely recovery in
# ~N turns" and lets the caller weigh that against remaining conversation life.
module Resolver
  # Evict without comment at/above this f (fraction of prompt removed).
  CACHE_FLOOR = 1.0 / 3.0
  # Mandatory eviction once the prompt reaches this fraction of the context window.
  CEILING_FRACTION = 0.9

  Result = Struct.new(:f, :surcharge, :break_even_turns, :recovery_turns, :verdict, keyword_init: true)

  module_function

  def resolve(removed_tokens:, prompt_tokens:, context_limit: nil)
    f = prompt_tokens.to_f.positive? ? removed_tokens.to_f / prompt_tokens.to_f : 0.0
    surcharge    = 2.0 - 3.0 * f
    break_even   = f.positive? ? (2.0 / f) - 3.0 : Float::INFINITY
    recovery     = break_even.finite? ? break_even.ceil : nil

    verdict =
      if context_limit && context_limit.to_f.positive? && prompt_tokens.to_f >= context_limit.to_f * CEILING_FRACTION
        :mandatory
      elsif f >= CACHE_FLOOR
        :evict
      else
        :extend
      end

    Result.new(f: f, surcharge: surcharge, break_even_turns: break_even,
               recovery_turns: recovery, verdict: verdict)
  end
end
